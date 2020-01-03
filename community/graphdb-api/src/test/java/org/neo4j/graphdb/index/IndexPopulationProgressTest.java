/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.graphdb.index;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class IndexPopulationProgressTest
{
    @Test
    void testNone()
    {
        assertEquals( 0, IndexPopulationProgress.NONE.getCompletedPercentage(), 0.01 );
    }

    @Test
    void testDone()
    {
        assertEquals( 100, IndexPopulationProgress.DONE.getCompletedPercentage(), 0.01 );
    }

    @Test
    void testNegativeCompleted()
    {
        assertThrows( IllegalArgumentException.class, () -> new IndexPopulationProgress( -1, 1 ) );
    }

    @Test
    void testNegativeTotal()
    {
        assertThrows( IllegalArgumentException.class, () -> new IndexPopulationProgress( 0, -1 ) );
    }

    @Test
    void testAllZero()
    {
        IndexPopulationProgress progress = new IndexPopulationProgress( 0, 0 );
        assertEquals( 0, progress.getCompletedCount() );
        assertEquals( 0, progress.getTotalCount() );
        assertEquals( 0, progress.getCompletedPercentage(), 0.01 );
    }

    @Test
    void testCompletedZero()
    {
        IndexPopulationProgress progress = new IndexPopulationProgress( 0, 1 );
        assertEquals( 1, progress.getTotalCount() );
        assertEquals( 0, progress.getCompletedCount() );
        assertEquals( 0, progress.getCompletedPercentage(), 0.01 );
    }

    @Test
    void testCompletedGreaterThanTotal()
    {
        assertThrows( IllegalArgumentException.class, () -> new IndexPopulationProgress( 2, 1 ) );
    }

    @Test
    void testGetCompletedPercentage()
    {
        IndexPopulationProgress progress = new IndexPopulationProgress( 1, 2 );
        assertEquals( 50.0f, progress.getCompletedPercentage(), 0.01f );
    }

    @Test
    void testGetCompleted()
    {
        IndexPopulationProgress progress = new IndexPopulationProgress( 1, 2 );
        assertEquals( 1L, progress.getCompletedCount() );
    }

    @Test
    void testGetTotal()
    {
        IndexPopulationProgress progress = new IndexPopulationProgress( 1, 2 );
        assertEquals( 2L, progress.getTotalCount() );
    }
}
