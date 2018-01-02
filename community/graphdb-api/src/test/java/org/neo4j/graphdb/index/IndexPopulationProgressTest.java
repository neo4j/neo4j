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
package org.neo4j.graphdb.index;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


public class IndexPopulationProgressTest
{
    @Test
    public void testNone() throws Exception
    {
        assertEquals( 0, IndexPopulationProgress.NONE.getCompletedPercentage(), 0 );
    }

    @Test
    public void testDone() throws Exception
    {
        assertEquals( 100, IndexPopulationProgress.DONE.getCompletedPercentage(), 0 );
    }

    @Test
    public void testNegativeCompleted() throws Exception
    {
        try
        {
            new IndexPopulationProgress( -1, 1 );
            fail( "Should have thrown IllegalArgumentException" );
        }
        catch ( IllegalArgumentException e )
        {
            // success
        }
    }

    @Test
    public void testNegativeTotal() throws Exception
    {
        try
        {
            new IndexPopulationProgress( 0, -1 );
            fail( "Should have thrown IllegalArgumentException" );
        }
        catch ( IllegalArgumentException e )
        {
            // success
        }
    }

    @Test
    public void testAllZero() throws Exception
    {
        IndexPopulationProgress progress = new IndexPopulationProgress( 0, 0 );
        assertEquals( 0, progress.getCompletedCount() );
        assertEquals( 0, progress.getTotalCount() );
        assertEquals( 0, progress.getCompletedPercentage(), 0 );
    }

    @Test
    public void testCompletedZero() throws Exception
    {
        IndexPopulationProgress progress = new IndexPopulationProgress( 0, 1 );
        assertEquals( 1, progress.getTotalCount() );
        assertEquals( 0, progress.getCompletedCount() );
        assertEquals( 0, progress.getCompletedPercentage(), 0 );
    }

    @Test
    public void testCompletedGreaterThanTotal() throws Exception
    {
        try
        {
            new IndexPopulationProgress( 2, 1 );
            fail( "Should have thrown IllegalArgumentException" );
        }
        catch ( IllegalArgumentException e )
        {
            // success
        }
    }

    @Test
    public void testGetCompletedPercentage() throws Exception
    {
        IndexPopulationProgress progress = new IndexPopulationProgress( 1, 2 );
        assertEquals( 50.0f, progress.getCompletedPercentage(), 0.0f );
    }

    @Test
    public void testGetCompleted() throws Exception
    {
        IndexPopulationProgress progress = new IndexPopulationProgress( 1, 2 );
        assertEquals( 1L, progress.getCompletedCount() );
    }

    @Test
    public void testGetTotal() throws Exception
    {
        IndexPopulationProgress progress = new IndexPopulationProgress( 1, 2 );
        assertEquals( 2L, progress.getTotalCount() );
    }
}
