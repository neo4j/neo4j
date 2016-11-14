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
import static org.neo4j.index.bptree.GenSafePointer.isEmpty;

public class GenSafePointerPair
{
    // Terminology:
    // - invalid = bad checksum or crash gen
    // - valid = correct checksum
    // - empty = all zeros

    public static final long POINTER_UNDECIDED_SAME_GENERATION = -2;
    public static final long POINTER_UNDECIDED_NONE_CORRECT = -3;
    public static final long POINTER_UNDECIDED_BOTH_EMPTY = -4;

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
        long generationA = GenSafePointer.readGeneration( cursor );
        long pointerA = GenSafePointer.readPointer( cursor );
        short readChecksumA = GenSafePointer.readChecksum( cursor );
        short checksumA = GenSafePointer.checksumOf( generationA, pointerA );
        boolean correctChecksumA = readChecksumA == checksumA;

        // Try B
        long generationB = GenSafePointer.readGeneration( cursor );
        long pointerB = GenSafePointer.readPointer( cursor );
        short readChecksumB = GenSafePointer.readChecksum( cursor );
        short checksumB = GenSafePointer.checksumOf( generationB, pointerB );
        boolean correctChecksumB = readChecksumB == checksumB;

        boolean aIsValid = correctChecksumA && generationA >= MIN_GENERATION &&
                (generationA <= stableGeneration || generationA == unstableGeneration);
        boolean bIsValid = correctChecksumB && generationB >= MIN_GENERATION &&
                (generationB <= stableGeneration || generationB == unstableGeneration);
        // Success cases
        // - both valid and different generation
        if ( aIsValid && bIsValid && generationA != generationB )
        {
            return generationA > generationB ? pointerA : pointerB;
        }
        // - one valid
        else if ( aIsValid ^ bIsValid )
        {
            return aIsValid ? pointerA : pointerB;
        }
        else
        {
            // TODO: all teh failurez
            return POINTER_UNDECIDED_NONE_CORRECT;
        }
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
    public static boolean write( PageCursor cursor, long pointer, int stableGeneration, int unstableGeneration )
    {
        // Later there will be a selection which "slot" of GSP out of the two to write into.
        int offset = cursor.getOffset();
        // Try A
        long generationA = GenSafePointer.readGeneration( cursor );
        long pointerA = GenSafePointer.readPointer( cursor );
        short readChecksumA = GenSafePointer.readChecksum( cursor );
        short checksumA = GenSafePointer.checksumOf( generationA, pointerA );
        boolean correctChecksumA = readChecksumA == checksumA;

        // Try B
        long generationB = GenSafePointer.readGeneration( cursor );
        long pointerB = GenSafePointer.readPointer( cursor );
        short readChecksumB = GenSafePointer.readChecksum( cursor );
        short checksumB = GenSafePointer.checksumOf( generationB, pointerB );
        boolean correctChecksumB = readChecksumB == checksumB;

        // Select correct slot
        boolean writeToSlotA;

        boolean aIsValid = correctChecksumA && (generationA <= stableGeneration || generationA == unstableGeneration);
        boolean bIsValid = correctChecksumB && (generationB <= stableGeneration || generationB == unstableGeneration);

        // Failure cases
        // - both invalid
        if ( !aIsValid && !bIsValid )
        {
            return false;
        }
        // - both valid and same generation
        if ( aIsValid && bIsValid && generationA == generationB )
        {
            return false;
        }

        // Prioritized selection
        // - one with unstable generation
        if ( (aIsValid && generationA == unstableGeneration) || (bIsValid && generationB == unstableGeneration) )
        {
            writeToSlotA = aIsValid && generationA == unstableGeneration;
        }
        // - exactly one invalid
        else if ( !aIsValid || !bIsValid )
        {
            writeToSlotA = !aIsValid;
        }
        // - empty
        else if ( isEmpty( generationA, pointerA ) || isEmpty( generationB, pointerB ) )
        {
            writeToSlotA = isEmpty( generationA, pointerA );
        }
        // - lowest generation
        else
        {
            writeToSlotA = generationA < generationB;
        }

        // And write
        int writeOffset = writeToSlotA ? offset : offset + GenSafePointer.SIZE;
        cursor.setOffset( writeOffset );
        GenSafePointer.write( cursor, unstableGeneration, pointer );
        return true;
    }

    public static boolean isFailed( long pointer )
    {
        return pointer <= POINTER_UNDECIDED_SAME_GENERATION;
    }
}
