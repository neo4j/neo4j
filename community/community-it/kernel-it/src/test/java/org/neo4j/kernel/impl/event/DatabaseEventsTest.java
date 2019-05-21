/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.event.DatabaseEventContext;
import org.neo4j.graphdb.event.DatabaseEventListener;
import org.neo4j.graphdb.event.DatabaseEventListenerAdapter;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DatabaseEventsTest
{
    private DatabaseManagementService managementService;

    @BeforeEach
    void setUp()
    {
        managementService = new TestDatabaseManagementServiceBuilder().impermanent().build();
    }

    @AfterEach
    void tearDown()
    {
        if ( managementService != null )
        {
            managementService.shutdown();
        }
    }

    @Test
    void registerUnregisterHandlers()
    {
        DatabaseEventListener handler1 = new DummyDatabaseEventListener();
        DatabaseEventListener handler2 = new DummyDatabaseEventListener();

        assertThrows( IllegalStateException.class, () -> managementService.unregisterDatabaseEventListener( handler1 ) );

        managementService.registerDatabaseEventListener( handler1 );
        managementService.registerDatabaseEventListener( handler1 );
        managementService.unregisterDatabaseEventListener( handler1 );

        assertThrows( IllegalStateException.class, () -> managementService.unregisterDatabaseEventListener( handler1 ) );

        managementService.registerDatabaseEventListener( handler1 );
        managementService.registerDatabaseEventListener( handler2 );
        managementService.unregisterDatabaseEventListener( handler1 );
        managementService.unregisterDatabaseEventListener( handler2 );
    }

    @Test
    void shutdownEvents()
    {
        DummyDatabaseEventListener handler1 = new DummyDatabaseEventListener();
        DummyDatabaseEventListener handler2 = new DummyDatabaseEventListener();
        managementService.registerDatabaseEventListener( handler1 );
        managementService.registerDatabaseEventListener( handler2 );

        managementService.shutdown();

        assertEquals( 2, handler2.getShutdownCounter() );
        assertEquals( 2, handler1.getShutdownCounter() );
    }

    private static class DummyDatabaseEventListener extends DatabaseEventListenerAdapter
    {
        private int shutdownCounter;

        @Override
        public void databaseShutdown( DatabaseEventContext eventContext )
        {
            shutdownCounter++;
        }

        int getShutdownCounter()
        {
            return shutdownCounter;
        }
    }
}
