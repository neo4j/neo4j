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
package org.neo4j.kernel.impl.core;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.ErrorState;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.impl.AbstractNeo4jTestCase.getStorePath;

public class TestShutdownSequence
{
    private GraphDatabaseService graphDb;
    
    @Before
    public void createGraphDb()
    {
        graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();
    }

    @Test
    public void canInvokeShutdownMultipleTimes()
    {
        graphDb.shutdown();
        graphDb.shutdown();
    }

    public @Test
    void eventHandlersAreOnlyInvokedOnceDuringShutdown()
    {
        final AtomicInteger counter = new AtomicInteger();
        graphDb.registerKernelEventHandler( new KernelEventHandler()
        {
            @Override
            public void beforeShutdown()
            {
                counter.incrementAndGet();
            }

            @Override
            public Object getResource()
            {
                return null;
            }

            @Override
            public void kernelPanic( ErrorState error )
            {
                // do nothing
            }

            @Override
            public ExecutionOrder orderComparedTo( KernelEventHandler other )
            {
                return ExecutionOrder.DOESNT_MATTER;
            }
        } );
        graphDb.shutdown();
        graphDb.shutdown();
        assertEquals( 1, counter.get() );
    }

    public @Test
    void canRemoveFilesAndReinvokeShutdown()
    {
        graphDb.shutdown();
        AbstractNeo4jTestCase.deleteFileOrDirectory( getStorePath( "shutdown" ) );
        graphDb.shutdown();
    }

    public @Test
    void canInvokeShutdownFromShutdownHandler()
    {
        graphDb.registerKernelEventHandler( new KernelEventHandler()
        {
            @Override
            public void beforeShutdown()
            {
                graphDb.shutdown();
            }

            @Override
            public Object getResource()
            {
                return null;
            }

            @Override
            public void kernelPanic( ErrorState error )
            {
                // do nothing
            }

            @Override
            public ExecutionOrder orderComparedTo( KernelEventHandler other )
            {
                return ExecutionOrder.DOESNT_MATTER;
            }
        } );
        graphDb.shutdown();
    }
}
