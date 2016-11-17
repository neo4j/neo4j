/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.index.bptree;

import org.neo4j.io.pagecache.PageCursor;

import static org.neo4j.index.bptree.GenSafePointer.MIN_GENERATION;
import static org.neo4j.index.bptree.GenSafePointer.checksumOf;
import static org.neo4j.index.bptree.GenSafePointer.readChecksum;
import static org.neo4j.index.bptree.GenSafePointer.readGeneration;
import static org.neo4j.index.bptree.GenSafePointer.readPointer;

public class GenSafePointerPair
{
    // Terminology:
    // - empty = all zeros
    // - broken = bad checksum
    // - invalid = broken or crash gen
    // - valid = correct checksum and not empty

    public static final long POINTER_UNDECIDED_SAME_GENERATION = -2;
    public static final long POINTER_UNDECIDED_NONE_CORRECT = -3;
    public static final long POINTER_UNDECIDED_BOTH_EMPTY = -4;

    public static final byte STABLE = 0;
    public static final byte UNSTABLE = 1;
    public static final byte CRASH = 2;
    public static final byte BROKEN = 3;
    public static final byte EMPTY = 4;

    // todo make nice
    // [    ,    ][    ,    ][    ,    ][    ,    ]  [    ,    ][    ,    ][    ,    ][    ,    ]
    // Success or Fail 1b, success=0, fail=1
    // Read or write   1b, read=0, write=1
    // Generation comparison 2b, equal=0x0, aBig=0x1, bBig=0x2
    // State 3b, stable=0x1, unstable=0x2, crash=0x3, broken=0x4, empty=0x5

    // READ IF SUCCESS
    // pointer
    // READ IF FAIL
    // state of A and B, generation comparison between A and B
    // WRITE IF SUCCESS
    // where to write
    // WRITE IF FAIL
    // state of A and B, generation comparison between A and B

    private static final long SUCCESS      = 0x00000000_00000000L;
    private static final long FAIL         = 0x80000000_00000000L;
    private static final long READ         = 0x00000000_00000000L;
    private static final long WRITE        = 0x40000000_00000000L;
    private static final long GEN_EQUAL    = 0x00000000_00000000L;
    private static final long GEN_A_BIG    = 0x10000000_00000000L;
    private static final long GEN_B_BIG    = 0x20000000_00000000L;
    private static final long WRITE_TO_A   = 0x00000000_00000000L;
    private static final long WRITE_TO_B   = 0x00200000_00000000L;
    private static final int STATE_SHIFT_A = 60;
    private static final int STATE_SHIFT_B = 57;

    private static final long SUCCESS_WRITE_TO_B = SUCCESS | WRITE | WRITE_TO_B; // Aggregation
    private static final long SUCCESS_WRITE_TO_A = SUCCESS | WRITE | WRITE_TO_A; // Aggregation

    private static final long SUCCESS_MASK  = SUCCESS | FAIL;
    private static final long WRITE_TO_MASK = WRITE_TO_A | WRITE_TO_B;

    /**
     * @param cursor {@link PageCursor} to read from.
     * @param stableGeneration stable index generation.
     * @param unstableGeneration unstable index generation.
     * @return most recent pointer from a GSPP, i.e. two GSP slots A/B. The one with highest generation
     * AND whose checksum checks out is returned.
     */
    public static long read( PageCursor cursor, int stableGeneration, int unstableGeneration )
    {
        // Try A
        long generationA = readGeneration( cursor );
        long pointerA = readPointer( cursor );
        short readChecksumA = readChecksum( cursor );
        short checksumA = checksumOf( generationA, pointerA );
        boolean correctChecksumA = readChecksumA == checksumA;

        // Try B
        long generationB = readGeneration( cursor );
        long pointerB = readPointer( cursor );
        short readChecksumB = readChecksum( cursor );
        short checksumB = checksumOf( generationB, pointerB );
        boolean correctChecksumB = readChecksumB == checksumB;

        byte pointerStateA = pointerState( stableGeneration, unstableGeneration, generationA, pointerA, correctChecksumA );
        byte pointerStateB = pointerState( stableGeneration, unstableGeneration, generationB, pointerB, correctChecksumB );

        if ( pointerStateA == UNSTABLE )
        {
            if ( pointerStateB == STABLE || pointerStateB == EMPTY )
            {
                return SUCCESS | READ | pointerA;
            }
        }
        else if ( pointerStateB == UNSTABLE )
        {
            if ( pointerStateA == STABLE || pointerStateA == EMPTY )
            {
                return SUCCESS | READ | pointerB;
            }
        }
        else if ( pointerStateA == STABLE && pointerStateB == STABLE )
        {
            // compare gen
            if ( generationA > generationB )
            {
                return SUCCESS | READ | pointerA;
            }
            else if ( generationB > generationA )
            {
                return SUCCESS | READ | pointerB;
            }
        }
        else if ( pointerStateA == STABLE )
        {
            return SUCCESS | READ | pointerA;
        }
        else if ( pointerStateB == STABLE )
        {
            return SUCCESS | READ | pointerB;
        }

        return FAIL | READ | generationState( generationA, generationB ) |
               ((long) pointerStateA) << STATE_SHIFT_A | ((long) pointerStateB) << STATE_SHIFT_B;
//
//        boolean aIsReadable = correctChecksumA && generationA >= MIN_GENERATION &&
//                (generationA <= stableGeneration || generationA == unstableGeneration);
//        boolean bIsReadable = correctChecksumB && generationB >= MIN_GENERATION &&
//                (generationB <= stableGeneration || generationB == unstableGeneration);
//        // Success cases
//        // - both valid and different generation
//        if ( aIsReadable && bIsReadable && generationA != generationB )
//        {
//            return generationA > generationB ? pointerA : pointerB;
//        }
//        // - one valid
//        else if ( aIsReadable ^ bIsReadable )
//        {
//            return aIsReadable ? pointerA : pointerB;
//        }
//        else
//        {
//            if ( isEmpty( generationA, pointerA ) && isEmpty( generationB, pointerB ) )
//            {
//                return POINTER_UNDECIDED_BOTH_EMPTY;
//            }
//            else if ( !aIsReadable && !bIsReadable && generationA != generationB )
//            {
//                return POINTER_UNDECIDED_NONE_CORRECT;
//            }
//            else if ( generationA == generationB )
//            {
//                return POINTER_UNDECIDED_SAME_GENERATION;
//            }
//            else
//            {
//                throw new UnsupportedOperationException( "Uncovered case" +
//                                                         " A: " + generationA + "," + pointerA + "," + checksumA +
//                                                         " B: " + generationB + "," + pointerB + "," + checksumB );
//            }
//        }
    }

    /**
     * Writes a GSP at one of the GSPP slots A/B. Both slots are observed and selection goes like this:
     *
     * <ol>
     * <li>if there's a slot with current generation it will be selected</li>
     * <li>otherwise, the one with lowest generation or an incorrect checksum will be selected</li>
     * </ol>
     *
     * @param cursor {@link PageCursor} to write to.
     * @param pointer pageId to write.
     * @param stableGeneration stable index generation.
     * @param unstableGeneration unstable index generation, which will be the generation to write in the slot.
     * @return {@code true} on success, otherwise {@code false} on failure.
     */
    public static long write( PageCursor cursor, long pointer, int stableGeneration, int unstableGeneration )
    {
        // Later there will be a selection which "slot" of GSP out of the two to write into.
        int offset = cursor.getOffset();
        // Try A
        long generationA = readGeneration( cursor );
        long pointerA = readPointer( cursor );
        short readChecksumA = readChecksum( cursor );
        short checksumA = checksumOf( generationA, pointerA );
        boolean correctChecksumA = readChecksumA == checksumA;

        // Try B
        long generationB = readGeneration( cursor );
        long pointerB = readPointer( cursor );
        short readChecksumB = readChecksum( cursor );
        short checksumB = checksumOf( generationB, pointerB );
        boolean correctChecksumB = readChecksumB == checksumB;

        byte pointerStateA = pointerState( stableGeneration, unstableGeneration, generationA, pointerA, correctChecksumA );
        byte pointerStateB = pointerState( stableGeneration, unstableGeneration, generationB, pointerB, correctChecksumB );

        long writeResult = writeResult( pointerStateA, pointerStateB, generationA, generationB );

        if ( isSuccess( writeResult ) )
        {
            boolean writeToA = ( writeResult & WRITE_TO_MASK ) == WRITE_TO_A;
            int writeOffset = writeToA ? offset : offset + GenSafePointer.SIZE;
            cursor.setOffset( writeOffset );
            GenSafePointer.write( cursor, unstableGeneration, pointer );
        }
        return writeResult;

        // todo keep below and have it return correct status from call
//        // Select correct slot
//        boolean writeToSlotA;
//
//        boolean aIsValid = correctChecksumA && (generationA <= stableGeneration || generationA == unstableGeneration);
//        boolean bIsValid = correctChecksumB && (generationB <= stableGeneration || generationB == unstableGeneration);
//
//        // Failure cases
//        // - both invalid
//        if ( !aIsValid && !bIsValid )
//        {
//            return false;
//        }
//        // - both valid and same generation, but not empty
//        if ( aIsValid && bIsValid && generationA == generationB && generationA >= MIN_GENERATION )
//        {
//            return false;
//        }
//
//        // Prioritized selection
//        // - one with unstable generation
//        if ( (aIsValid && generationA == unstableGeneration) || (bIsValid && generationB == unstableGeneration) )
//        {
//            writeToSlotA = aIsValid && generationA == unstableGeneration;
//        }
//        // - exactly one invalid
//        else if ( !aIsValid || !bIsValid )
//        {
//            writeToSlotA = !aIsValid;
//        }
//        // - empty
//        else if ( isEmpty( generationA, pointerA ) || isEmpty( generationB, pointerB ) )
//        {
//            writeToSlotA = isEmpty( generationA, pointerA );
//        }
//        // - lowest generation
//        else
//        {
//            writeToSlotA = generationA < generationB;
//        }
//
//        // And write
//        int writeOffset = writeToSlotA ? offset : offset + SIZE;
//        cursor.setOffset( writeOffset );
//        GenSafePointer.write( cursor, unstableGeneration, pointer );
//        return true;
    }

    // All different ordered combinations of pointer states and if they are considered to be an ok state to see
    // STABLE,STABLE      - OK if different generation
    // STABLE,UNSTABLE    - OK
    // STABLE,CRASH       - OK
    // STABLE,BROKEN      - OK
    // STABLE,EMPTY       - OK
    // UNSTABLE,UNSTABLE  - NOT OK
    // UNSTABLE,CRASH     - NOT OK
    // UNSTABLE,BROKEN    - NOT OK
    // UNSTABLE,EMPTY     - OK
    // CRASH,CRASH        - NOT OK
    // CRASH,BROKEN       - NOT OK
    // CRASH,EMPTY        - NOT OK
    // BROKEN,BROKEN      - NOT OK
    // BROKEN,EMPTY       - NOT OK
    // EMPTY,EMPTY        - OK if writing, not if reading
    private static long writeResult( byte pointerStateA, byte pointerStateB, long generationA, long generationB )
    {
        if ( pointerStateA == STABLE )
        {
            if ( pointerStateB == STABLE )
            {
                if ( generationA > generationB )
                {
                    // Write to slot B
                    return SUCCESS_WRITE_TO_B;
                }
                else if ( generationB > generationA )
                {
                    // Write to slot A
                    return SUCCESS_WRITE_TO_A;
                }
            }
            else
            {
                // Write to slot B
                return SUCCESS_WRITE_TO_B;
            }
        }
        else if ( pointerStateB == STABLE )
        {
            // write to slot A
            return SUCCESS_WRITE_TO_A;
        }
        else if ( pointerStateA == UNSTABLE )
        {
            if ( pointerStateB == EMPTY )
            {
                // write to slot A
                return SUCCESS_WRITE_TO_A;
            }
        }
        else if ( pointerStateB == UNSTABLE )
        {
            if ( pointerStateA == EMPTY )
            {
                // write to slot B
                return SUCCESS_WRITE_TO_B;
            }
        }
        else if ( pointerStateA == EMPTY && pointerStateB == EMPTY )
        {
            // write to slot A
            return SUCCESS_WRITE_TO_A;
        }

        // Encode error
        return FAIL | WRITE | generationState( generationA, generationB ) |
               ((long) pointerStateA) << STATE_SHIFT_A | ((long) pointerStateB) << STATE_SHIFT_B;
    }

    private static long generationState( long generationA, long generationB )
    {
        return generationA > generationB ? GEN_A_BIG : generationB > generationA ? GEN_B_BIG : GEN_EQUAL;
    }

    private static byte pointerState( int stableGeneration, int unstableGeneration,
            long generation, long pointer, boolean checksumIsCorrect )
    {
        if ( generation == 0L && pointer == 0L )
        {
            return EMPTY;
        }
        if ( !checksumIsCorrect )
        {
            return BROKEN;
        }
        if ( generation < MIN_GENERATION )
        {
            // todo Better error message?
            throw new UnsupportedOperationException( "Generation was less than MIN_GENERATION " + MIN_GENERATION +
                                                     " but checksum was correct. This should not happen." );
        }
        if ( generation <= stableGeneration )
        {
            return STABLE;
        }
        if ( generation == unstableGeneration )
        {
            return UNSTABLE;
        }
        return CRASH;
    }

    public static boolean isSuccess( long result )
    {
        return (result & SUCCESS_MASK) == 0;
    }
}
