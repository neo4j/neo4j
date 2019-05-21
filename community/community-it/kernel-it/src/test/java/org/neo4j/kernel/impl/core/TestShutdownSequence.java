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
package org.neo4j.kernel.impl.core;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.event.DatabaseEventContext;
import org.neo4j.graphdb.event.DatabaseEventListenerAdapter;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

class TestShutdownSequence
{
    private DatabaseManagementService managementService;

    @BeforeEach
    void createGraphDb()
    {
        managementService = new TestDatabaseManagementServiceBuilder().impermanent().build();
    }

    @Test
    void canInvokeShutdownMultipleTimes()
    {
        managementService.shutdown();
        managementService.shutdown();
    }

    @Test
    void notifyEventListenersOnShutdown()
    {
        MutableInt counter = new MutableInt();
        managementService.registerDatabaseEventListener( new DatabaseEventListenerAdapter()
        {
            @Override
            public void databaseShutdown( DatabaseEventContext eventContext )
            {
                counter.incrementAndGet();
            }
        } );
        managementService.shutdown();
        managementService.shutdown();
        assertEquals( 2, counter.intValue() );
    }

    @Test
    void canRemoveFilesAndReinvokeShutdown() throws IOException
    {
        GraphDatabaseAPI databaseAPI = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        FileSystemAbstraction fileSystemAbstraction = getDatabaseFileSystem( databaseAPI );
        managementService.shutdown();
        fileSystemAbstraction.deleteRecursively( databaseAPI.databaseLayout().databaseDirectory() );
        managementService.shutdown();
    }

    @Test
    void canInvokeShutdownFromShutdownListener()
    {
        managementService.registerDatabaseEventListener( new DatabaseEventListenerAdapter()
        {
            @Override
            public void databaseShutdown( DatabaseEventContext eventContext )
            {
                managementService.shutdown();
            }
        } );
        managementService.shutdown();
    }

    private static FileSystemAbstraction getDatabaseFileSystem( GraphDatabaseAPI databaseAPI )
    {
        return databaseAPI.getDependencyResolver().resolveDependency( FileSystemAbstraction.class );
    }
}
