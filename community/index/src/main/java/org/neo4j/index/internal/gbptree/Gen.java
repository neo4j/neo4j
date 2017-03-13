/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.index.internal.gbptree;

/**
 * Logic for composing and decomposing stable/unstable generation number (unsigned int) to/from a single {@code long}.
 *
 * <pre>
 * long: [S,S,S,S,U,U,U,U]
 *       msb <-------> lsb
 * </pre>
 */
class Gen
{
    private static final long UNSTABLE_GEN_MASK = 0xFFFFFFFFL;
    private static final int STABLE_GEN_SHIFT = Integer.SIZE;

    /**
     * Takes one stable and one unstable generation (both unsigned ints) and crams them into one {@code long}.
     *
     * @param stableGen stable generation.
     * @param unstableGen unstable generation.
     * @return the two generation numbers as one {@code long}.
     */
    public static long gen( long stableGen, long unstableGen )
    {
        GenSafePointer.assertGenOnWrite( stableGen );
        GenSafePointer.assertGenOnWrite( unstableGen );

        return (stableGen << STABLE_GEN_SHIFT) | unstableGen;
    }

    /**
     * Extracts and returns unstable generation from generation {@code long}.
     *
     * @param gen generation variable containing both stable and unstable generations.
     * @return unstable generation from generation.
     */
    public static long unstableGen( long gen )
    {
        return gen & UNSTABLE_GEN_MASK;
    }

    /**
     * Extracts and returns stable generation from generation {@code long}.
     *
     * @param gen generation variable containing both stable and unstable generations.
     * @return stable generation from generation.
     */
    public static long stableGen( long gen )
    {
        return gen >>> STABLE_GEN_SHIFT;
    }
}
