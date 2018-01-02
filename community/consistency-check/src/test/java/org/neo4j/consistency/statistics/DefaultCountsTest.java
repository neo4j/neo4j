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
package org.neo4j.consistency.statistics;

import org.junit.Test;

import org.neo4j.consistency.statistics.Counts.Type;

import static org.junit.Assert.assertEquals;

public class DefaultCountsTest
{
    @Test
    public void shouldCountPerThread() throws Exception
    {
        // GIVEN
        Counts counts = new DefaultCounts( 3 );

        // WHEN
        counts.incAndGet( Type.activeCache, 0 );
        counts.incAndGet( Type.activeCache, 1 );
        counts.incAndGet( Type.backLinks, 2 );

        // THEN
        assertEquals( 2, counts.sum( Type.activeCache ) );
        assertEquals( 1, counts.sum( Type.backLinks ) );
        assertEquals( 0, counts.sum( Type.clearCache ) );
    }

    @Test
    public void shouldResetCounts() throws Exception
    {
        // GIVEN
        Counts counts = new DefaultCounts( 2 );
        counts.incAndGet( Type.activeCache, 0 );
        assertEquals( 1, counts.sum( Type.activeCache ) );

        // WHEN
        counts.reset();

        // THEN
        assertEquals( 0, counts.sum( Type.activeCache ) );
    }
}
