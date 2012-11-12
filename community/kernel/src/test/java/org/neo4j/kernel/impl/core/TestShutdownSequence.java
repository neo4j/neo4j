/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.impl.AbstractNeo4jTestCase.getStorePath;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.event.ErrorState;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;
import org.neo4j.kernel.impl.util.FileUtils;

public class TestShutdownSequence
{
    private EmbeddedGraphDatabase graphDb;
    
    @BeforeClass
    public static void deleteDb() throws Exception
    {
        FileUtils.deleteRecursively( new File( getStorePath( "shutdown" ) ) );
    }

    public @Before
    void createGraphDb()
    {
        graphDb = new EmbeddedGraphDatabase( getStorePath( "shutdown" ) );
    }

    public @Test
    void canInvokeShutdownMultipleTimes()
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
            public void beforeShutdown()
            {
                counter.incrementAndGet();
            }

            public Object getResource()
            {
                return null;
            }

            public void kernelPanic( ErrorState error )
            {
                // do nothing
            }

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
        AbstractNeo4jTestCase.deleteFileOrDirectory( new File(
                getStorePath( "shutdown" ) ) );
        graphDb.shutdown();
    }

    public @Test
    void canInvokeShutdownFromShutdownHandler()
    {
        graphDb.registerKernelEventHandler( new KernelEventHandler()
        {
            public void beforeShutdown()
            {
                graphDb.shutdown();
            }

            public Object getResource()
            {
                return null;
            }

            public void kernelPanic( ErrorState error )
            {
                // do nothing
            }

            public ExecutionOrder orderComparedTo( KernelEventHandler other )
            {
                return ExecutionOrder.DOESNT_MATTER;
            }
        } );
        graphDb.shutdown();
    }
}
