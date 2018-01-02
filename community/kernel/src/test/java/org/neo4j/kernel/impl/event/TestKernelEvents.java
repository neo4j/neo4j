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
package org.neo4j.kernel.impl.event;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.impl.AbstractNeo4jTestCase.deleteFileOrDirectory;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.ErrorState;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.test.TestGraphDatabaseFactory;

public class TestKernelEvents
{
    private static final String PATH = "target/var/neodb";
    
    private static final Object RESOURCE1 = new Object();
    private static final Object RESOURCE2 = new Object();
    
    @BeforeClass
    public static void doBefore()
    {
        deleteFileOrDirectory( PATH );
    }

    @Test
    public void testRegisterUnregisterHandlers()
    {
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();
        KernelEventHandler handler1 = new DummyKernelEventHandler( RESOURCE1 )
        {
            @Override
            public ExecutionOrder orderComparedTo( KernelEventHandler other )
            {
                return ExecutionOrder.DOESNT_MATTER;
            }
        };
        KernelEventHandler handler2 = new DummyKernelEventHandler( RESOURCE2 )
        {
            @Override
            public ExecutionOrder orderComparedTo( KernelEventHandler other )
            {
                return ExecutionOrder.DOESNT_MATTER;
            }
        };

        try
        {
            graphDb.unregisterKernelEventHandler( handler1 );
            fail( "Shouldn't be able to do unregister on a "
                  + "unregistered handler" );
        }
        catch ( IllegalStateException e )
        { /* Good */
        }

        assertTrue( handler1 == graphDb.registerKernelEventHandler(
                handler1 ) );
        assertTrue( handler1 == graphDb.registerKernelEventHandler(
                handler1 ) );
        assertTrue( handler1 == graphDb.unregisterKernelEventHandler(
                handler1 ) );

        try
        {
            graphDb.unregisterKernelEventHandler( handler1 );
            fail( "Shouldn't be able to do unregister on a "
                  + "unregistered handler" );
        }
        catch ( IllegalStateException e )
        { /* Good */
        }

        assertTrue( handler1 == graphDb.registerKernelEventHandler(
                handler1 ) );
        assertTrue( handler2 == graphDb.registerKernelEventHandler(
                handler2 ) );
        assertTrue( handler1 == graphDb.unregisterKernelEventHandler(
                handler1 ) );
        assertTrue( handler2 == graphDb.unregisterKernelEventHandler(
                handler2 ) );

        graphDb.shutdown();
    }

    @Test
    public void testShutdownEvents()
    {
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();
        DummyKernelEventHandler handler1 = new DummyKernelEventHandler( RESOURCE1 )
        {
            @Override
            public ExecutionOrder orderComparedTo( KernelEventHandler other )
            {
                if ( ((DummyKernelEventHandler) other).resource == RESOURCE2 )
                {
                    return ExecutionOrder.AFTER;
                }
                return ExecutionOrder.DOESNT_MATTER;
            }
        };
        DummyKernelEventHandler handler2 = new DummyKernelEventHandler( RESOURCE1 )
        {
            @Override
            public ExecutionOrder orderComparedTo( KernelEventHandler other )
            {
                if ( ((DummyKernelEventHandler) other).resource == RESOURCE1 )
                {
                    return ExecutionOrder.BEFORE;
                }
                return ExecutionOrder.DOESNT_MATTER;
            }
        };
        graphDb.registerKernelEventHandler( handler1 );
        graphDb.registerKernelEventHandler( handler2 );

        graphDb.shutdown();

        assertEquals( Integer.valueOf( 0 ), handler2.beforeShutdown );
        assertEquals( Integer.valueOf( 1 ), handler1.beforeShutdown );
    }

    private static abstract class DummyKernelEventHandler implements KernelEventHandler
    {
        private static int counter;
        private Integer beforeShutdown, kernelPanic;
        private final Object resource;

        DummyKernelEventHandler( Object resource )
        {
            this.resource = resource;
        }

        @Override
        public void beforeShutdown()
        {
            beforeShutdown = counter++;
        }

        @Override
        public Object getResource()
        {
            return this.resource;
        }

        @Override
        public void kernelPanic( ErrorState error )
        {
            kernelPanic = counter++;
        }
    }
}
