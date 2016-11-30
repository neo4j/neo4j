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
package org.neo4j.index.gbptree;

import org.neo4j.io.pagecache.PageCursor;

import static java.lang.String.format;

import static org.neo4j.index.gbptree.GenSafePointer.MIN_GENERATION;
import static org.neo4j.index.gbptree.GenSafePointer.checksumOf;
import static org.neo4j.index.gbptree.GenSafePointer.readChecksum;
import static org.neo4j.index.gbptree.GenSafePointer.readGeneration;
import static org.neo4j.index.gbptree.GenSafePointer.readPointer;

/**
 * Two {@link GenSafePointer} forming the basis for a B+tree becoming generate-aware.
 * <p>
 * Generally a GSP fall into one out of these categories:
 * <ul>
 * <li>STABLE: generation made durable and safe by a checkpoint</li>
 * <li>UNSTABLE: generation which is currently under evolution and isn't safe until next checkoint</li>
 * <li>EMPTY: have never been written</li>
 * </ul>
 * There are variations of pointers written in UNSTABLE generation:
 * <ul>
 * <li>BROKEN: written during a concurrent page cache flush and wasn't flushed after that point before crash</li>
 * <li>CRASH: pointers written as UNSTABLE before a crash happened (non-clean shutdown) are seen as
 * CRASH during recovery</li>
 * </ul>
 * <p>
 * Combinations of above mentioned states of the two pointers dictates which, if any, to read from or write to.
 * From the perspective of callers there's only "read" and "write", the two pointers are hidden.
 * <p>
 * All methods are static and all interaction is made with primitives.
 * <p>
 * Flags in results from read/write method calls. Pointer is 6B so msb 2B can be used for flags,
 * although the most common case (successful read) has its flag zeros so a successful read doesn't need
 * any masking to extract pointer.
 * <pre>
 * [    ,    ][    ,    ][ ... 6B pointer data ... ]
 *  ▲▲▲▲ ▲▲▲▲  ▲▲▲
 *  ││││ ││││  ││└────────────────────────────────────── 0:{@link #WRITE_TO_A}/1:{@link #WRITE_TO_B}
 *  ││││ │││└──└└─────────────────────────────────────── POINTER STATE B
 *  ││││ └└└──────────────────────────────────────────── POINTER STATE A
 *  ││└└──────────────────────────────────────────────── GENERATION COMPARISON:
 *  ││                                                   {@link #GEN_B_BIG}, {@link #GEN_EQUAL}, {@link #GEN_A_BIG}
 *  │└────────────────────────────────────────────────── 0:{@link #READ}/1:{@link #WRITE}
 *  └─────────────────────────────────────────────────── 0:{@link #SUCCESS}/1:{@link #FAIL}
 * </pre>
 */
class GenSafePointerPair
{
    static final int SIZE = GenSafePointer.SIZE * 2;

    // Pointer states
    static final byte STABLE = 0;     // any previous generation made safe by a checkpoint
    static final byte UNSTABLE = 1;   // current generation, generation under evolution until next checkpoint
    static final byte CRASH = 2;      // pointer written as unstable and didn't make it to checkpoint before crashing
    static final byte BROKEN = 3;     // mismatching checksum
    static final byte EMPTY = 4;      // generation and pointer all zeros

    // Flags and failure information
    static final long SUCCESS      = 0x00000000_00000000L;
    static final long FAIL         = 0x80000000_00000000L;
    static final long READ         = 0x00000000_00000000L;
    static final long WRITE        = 0x40000000_00000000L;
    static final long GEN_EQUAL    = 0x00000000_00000000L;
    static final long GEN_A_BIG    = 0x10000000_00000000L;
    static final long GEN_B_BIG    = 0x20000000_00000000L;
    static final long WRITE_TO_A   = 0x00000000_00000000L;
    static final long WRITE_TO_B   = 0x00200000_00000000L;
    static final int STATE_SHIFT_A = 57;
    static final int STATE_SHIFT_B = 54;

    // Aggregations and masks
    static final long SUCCESS_WRITE_TO_B = SUCCESS | WRITE | WRITE_TO_B;
    static final long SUCCESS_WRITE_TO_A = SUCCESS | WRITE | WRITE_TO_A;
    static final long SUCCESS_MASK  = SUCCESS | FAIL;
    static final long READ_OR_WRITE_MASK = READ | WRITE;
    static final long WRITE_TO_MASK = WRITE_TO_A | WRITE_TO_B;
    static final long STATE_MASK = 0x7; // After shift
    static final long GEN_COMPARISON_MASK = GEN_EQUAL | GEN_A_BIG | GEN_B_BIG;

    static final String GEN_COMPARISON_NAME_B_BIG = "A < B";
    static final String GEN_COMPARISON_NAME_A_BIG = "A > B";
    static final String GEN_COMPARISON_NAME_EQUAL = "A == B";

    /**
     * Reads a GSPP, returning the read pointer or a failure. Check success/failure using {@link #isSuccess(long)}
     * and if failure extract more information using {@link #failureDescription(long)}.
     *
     * @param cursor {@link PageCursor} to read from, placed at the beginning of the GSPP.
     * @param stableGeneration stable index generation.
     * @param unstableGeneration unstable index generation.
     * @return most recent readable pointer, or failure. Check result using {@link #isSuccess(long)}.
     */
    public static long read( PageCursor cursor, long stableGeneration, long unstableGeneration )
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

//        TODO ANOTHER APPROACH, keep here for reference and pursuit later
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
     * Writes a GSP at one of the GSPP slots A/B, returning the result.
     * Check success/failure using {@link #isSuccess(long)} and if failure extract more information using
     * {@link #failureDescription(long)}.
     *
     * @param cursor {@link PageCursor} to write to, placed at the beginning of the GSPP.
     * @param pointer pageId to write.
     * @param stableGeneration stable index generation.
     * @param unstableGeneration unstable index generation, which will be the generation to write in the slot.
     * @return {@code true} on success, otherwise {@code false} on failure.
     */
    public static long write( PageCursor cursor, long pointer, long stableGeneration, long unstableGeneration )
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

//        TODO ANOTHER APPROACH, keep here for reference and pursuit later
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

    static byte pointerState( long stableGeneration, long unstableGeneration,
            long generation, long pointer, boolean checksumIsCorrect )
    {
        if ( GenSafePointer.isEmpty( generation, pointer ) )
        {
            return EMPTY;
        }
        if ( !checksumIsCorrect )
        {
            return BROKEN;
        }
        if ( generation < MIN_GENERATION )
        {
            throw new UnsupportedOperationException( "Generation was less than MIN_GENERATION " + MIN_GENERATION +
                    " but checksum was correct. Pointer was " + generation + "," + pointer );
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

    /**
     * Checks to see if a result from read/write was successful. If not more failure information can be extracted
     * using {@link #failureDescription(long)}.
     *
     * @param result result from {@link #read(PageCursor, long, long)} or {@link #write(PageCursor, long, long, long)}.
     * @return {@code true} if successful read/write, otherwise {@code false}.
     */
    static boolean isSuccess( long result )
    {
        return (result & SUCCESS_MASK) == SUCCESS;
    }

    /**
     * Calling {@link #read(PageCursor, long, long)} (potentially also {@link #write(PageCursor, long, long, long)})
     * can fail due to seeing an unexpected state of the two GSPs. Failing right there and then isn't an option
     * due to how the page cache works and that something read from a {@link PageCursor} must not be interpreted
     * until after passing a {@link PageCursor#shouldRetry()} returning {@code false}. This creates a need for
     * including failure information in result returned from these methods so that, if failed, can have
     * the caller which interprets the result fail in a proper place. That place can make use of this method
     * by getting a human-friendly description about the failure.
     *
     * @param result result from {@link #read(PageCursor, long, long)} or
     * {@link #write(PageCursor, long, long, long)}.
     * @return a human-friendly description of the failure.
     */
    static String failureDescription( long result )
    {
        StringBuilder builder =
                new StringBuilder( "GSPP " + (isRead( result ) ? "READ" : "WRITE") + " failure" );
        builder.append( format( "%n  Pointer state A: %s",
                pointerStateName( pointerStateFromResult( result, STATE_SHIFT_A ) ) ) );
        builder.append( format( "%n  Pointer state B: %s",
                pointerStateName( pointerStateFromResult( result, STATE_SHIFT_B ) ) ) );
        builder.append( format( "%n  Generations: " + generationComparisonFromResult( result ) ) );
        return builder.toString();
    }

    /**
     * Asserts that a result is {@link #isSuccess(long) successful}, otherwise throws {@link IllegalStateException}.
     *
     * @param result result returned from {@link #read(PageCursor, long, long)} or
     * {@link #write(PageCursor, long, long, long)}
     * @return {@code true} if {@link #isSuccess(long) successful}, for interoperability with {@code assert}.
     */
    static boolean assertSuccess( long result )
    {
        if ( !isSuccess( result ) )
        {
            throw new IllegalStateException( failureDescription( result ) );
        }
        return true;
    }

    private static String generationComparisonFromResult( long result )
    {
        long bits = result & GEN_COMPARISON_MASK;
        if ( bits == GEN_EQUAL )
        {
            return GEN_COMPARISON_NAME_EQUAL;
        }
        else if ( bits == GEN_A_BIG )
        {
            return GEN_COMPARISON_NAME_A_BIG;
        }
        else if ( bits == GEN_B_BIG )
        {
            return GEN_COMPARISON_NAME_B_BIG;
        }
        else
        {
            return "Unknown[" + bits + "]";
        }
    }

    static String pointerStateName( byte pointerState )
    {
        switch ( pointerState )
        {
        case STABLE: return "STABLE";
        case UNSTABLE: return "UNSTABLE";
        case CRASH: return "CRASH";
        case BROKEN: return "BROKEN";
        case EMPTY: return "EMPTY";
        default: return "Unknown[" + pointerState + "]";
        }
    }

    static byte pointerStateFromResult( long result, int shift )
    {
        return (byte) ((result >>> shift) & STATE_MASK);
    }

    static boolean isRead( long result )
    {
        return (result & READ_OR_WRITE_MASK) == READ;
    }
}
