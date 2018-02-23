/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class GenerationTest
{
    @Test
    public void shouldSetLowGenerations()
    {
        shouldComposeAndDecomposeGeneration( GenerationSafePointer.MIN_GENERATION, GenerationSafePointer.MIN_GENERATION + 1 );
    }

    @Test
    public void shouldSetHighGenerations()
    {
        shouldComposeAndDecomposeGeneration( GenerationSafePointer.MAX_GENERATION - 1, GenerationSafePointer.MAX_GENERATION );
    }

    private void shouldComposeAndDecomposeGeneration( long stable, long unstable )
    {
        // WHEN
        long generation = Generation.generation( stable, unstable );
        long readStable = Generation.stableGeneration( generation );
        long readUnstable = Generation.unstableGeneration( generation );

        // THEN
        assertEquals( stable, readStable );
        assertEquals( unstable, readUnstable );
    }
}
