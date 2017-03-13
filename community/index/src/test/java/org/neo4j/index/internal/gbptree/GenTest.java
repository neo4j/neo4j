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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class GenTest
{
    @Test
    public void shouldSetLowGen() throws Exception
    {
        shouldComposeAndDecomposeGen( GenSafePointer.MIN_GEN, GenSafePointer.MIN_GEN + 1 );
    }

    @Test
    public void shouldSetHighGen() throws Exception
    {
        shouldComposeAndDecomposeGen( GenSafePointer.MAX_GEN - 1, GenSafePointer.MAX_GEN );
    }

    private void shouldComposeAndDecomposeGen( long stable, long unstable )
    {
        // WHEN
        long gen = Gen.gen( stable, unstable );
        long readStable = Gen.stableGen( gen );
        long readUnstable = Gen.unstableGen( gen );

        // THEN
        assertEquals( stable, readStable );
        assertEquals( unstable, readUnstable );
    }
}
