/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.index.internal.gbptree;

/**
 * Logic for composing and decomposing stable/unstable generation number (unsigned int) to/from a single {@code long}.
 *
 * <pre>
 * long: [S,S,S,S,U,U,U,U]
 *       msb <-------> lsb
 * </pre>
 */
class Generation
{
    private static final long UNSTABLE_GENERATION_MASK = 0xFFFFFFFFL;
    private static final int STABLE_GENERATION_SHIFT = Integer.SIZE;

    private Generation()
    {
    }

    /**
     * Takes one stable and one unstable generation (both unsigned ints) and crams them into one {@code long}.
     *
     * @param stableGeneration stable generation.
     * @param unstableGeneration unstable generation.
     * @return the two generation numbers as one {@code long}.
     */
    public static long generation( long stableGeneration, long unstableGeneration )
    {
        GenerationSafePointer.assertGenerationOnWrite( stableGeneration );
        GenerationSafePointer.assertGenerationOnWrite( unstableGeneration );

        return (stableGeneration << STABLE_GENERATION_SHIFT) | unstableGeneration;
    }

    /**
     * Extracts and returns unstable generation from generation {@code long}.
     *
     * @param generation generation variable containing both stable and unstable generations.
     * @return unstable generation from generation.
     */
    public static long unstableGeneration( long generation )
    {
        return generation & UNSTABLE_GENERATION_MASK;
    }

    /**
     * Extracts and returns stable generation from generation {@code long}.
     *
     * @param generation generation variable containing both stable and unstable generations.
     * @return stable generation from generation.
     */
    public static long stableGeneration( long generation )
    {
        return generation >>> STABLE_GENERATION_SHIFT;
    }
}
