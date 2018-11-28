/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.event;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.DatabaseEventHandler;
import org.neo4j.graphdb.event.DatabaseEventHandlerAdapter;
import org.neo4j.graphdb.event.ErrorState;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith( TestDirectoryExtension.class )
class TestDatabaseEvents
{
    @Inject
    private TestDirectory testDirectory;
    private static final Object RESOURCE1 = new Object();
    private static final Object RESOURCE2 = new Object();

    @Test
    void testRegisterUnregisterHandlers()
    {
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();
        DatabaseEventHandler handler1 = new DummyDatabaseEventHandler( RESOURCE1 )
        {
            @Override
            public ExecutionOrder orderComparedTo( DatabaseEventHandler other )
            {
                return ExecutionOrder.DOESNT_MATTER;
            }
        };
        DatabaseEventHandler handler2 = new DummyDatabaseEventHandler( RESOURCE2 )
        {
            @Override
            public ExecutionOrder orderComparedTo( DatabaseEventHandler other )
            {
                return ExecutionOrder.DOESNT_MATTER;
            }
        };

        assertThrows( IllegalStateException.class, () -> graphDb.unregisterDatabaseEventHandler( handler1 ) );

        assertSame( handler1, graphDb.registerDatabaseEventHandler( handler1 ) );
        assertSame( handler1, graphDb.registerDatabaseEventHandler( handler1 ) );
        assertSame( handler1, graphDb.unregisterDatabaseEventHandler( handler1 ) );

        assertThrows( IllegalStateException.class, () -> graphDb.unregisterDatabaseEventHandler( handler1 ) );

        assertSame( handler1, graphDb.registerDatabaseEventHandler( handler1 ) );
        assertSame( handler2, graphDb.registerDatabaseEventHandler( handler2 ) );
        assertSame( handler1, graphDb.unregisterDatabaseEventHandler( handler1 ) );
        assertSame( handler2, graphDb.unregisterDatabaseEventHandler( handler2 ) );

        graphDb.shutdown();
    }

    @Test
    void testShutdownEvents()
    {
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();
        DummyDatabaseEventHandler handler1 = new DummyDatabaseEventHandler( RESOURCE1 )
        {
            @Override
            public ExecutionOrder orderComparedTo( DatabaseEventHandler other )
            {
                if ( ((DummyDatabaseEventHandler) other).resource == RESOURCE2 )
                {
                    return ExecutionOrder.AFTER;
                }
                return ExecutionOrder.DOESNT_MATTER;
            }
        };
        DummyDatabaseEventHandler handler2 = new DummyDatabaseEventHandler( RESOURCE1 )
        {
            @Override
            public ExecutionOrder orderComparedTo( DatabaseEventHandler other )
            {
                if ( ((DummyDatabaseEventHandler) other).resource == RESOURCE1 )
                {
                    return ExecutionOrder.BEFORE;
                }
                return ExecutionOrder.DOESNT_MATTER;
            }
        };
        graphDb.registerDatabaseEventHandler( handler1 );
        graphDb.registerDatabaseEventHandler( handler2 );

        graphDb.shutdown();

        assertEquals( 0, handler2.beforeShutdown );
        assertEquals( 1, handler1.beforeShutdown );
    }

    private abstract static class DummyDatabaseEventHandler extends DatabaseEventHandlerAdapter
    {
        private static int counter;
        private int beforeShutdown;
        private int kernelPanic;
        private final Object resource;

        DummyDatabaseEventHandler( Object resource )
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
        public void panic( ErrorState error )
        {
            kernelPanic = counter++;
        }
    }
}
