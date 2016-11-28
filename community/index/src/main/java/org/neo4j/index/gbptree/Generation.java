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

/**
 * Logic for composing and decomposing stable/unstable to/from a single {@code long}.
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

    public static long generation( long stableGeneration, long unstableGeneration )
    {
        GenSafePointer.assertGeneration( stableGeneration );
        GenSafePointer.assertGeneration( unstableGeneration );

        return (stableGeneration << STABLE_GENERATION_SHIFT) | unstableGeneration;
    }

    public static long unstableGeneration( long rawGeneration )
    {
        return rawGeneration & UNSTABLE_GENERATION_MASK;
    }

    public static long stableGeneration( long rawGeneration )
    {
        return rawGeneration >>> STABLE_GENERATION_SHIFT;
    }
}
