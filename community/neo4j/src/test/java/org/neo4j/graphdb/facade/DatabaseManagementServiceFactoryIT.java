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
package org.neo4j.graphdb.facade;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.DatabaseEventContext;
import org.neo4j.graphdb.event.DatabaseEventListenerAdapter;
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;

@TestDirectoryExtension
class DatabaseManagementServiceFactoryIT
{
    @Inject
    private TestDirectory testDirectory;

    private DatabaseManagementService managementService;

    @BeforeEach
    void setUp()
    {
        managementService = getDatabaseManagementService();
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
    void reportCorrectDatabaseNames()
    {
        GraphDatabaseService system = managementService.database( SYSTEM_DATABASE_NAME );
        GraphDatabaseService neo4j = managementService.database( DEFAULT_DATABASE_NAME );
        assertEquals( SYSTEM_DATABASE_NAME, system.databaseName() );
        assertEquals( DEFAULT_DATABASE_NAME, neo4j.databaseName() );
    }

    @Test
    void haveTwoDatabasesByDefault()
    {
        assertThat( managementService.listDatabases(), hasSize( 2 ) );
    }

    @Test
    void failToCreateNonDefaultDatabase()
    {
        assertThrows( DatabaseManagementException.class, () -> managementService.createDatabase( "newDb" ) );
    }

    @Test
    void failToDropDatabase()
    {
        for ( String databaseName : managementService.listDatabases() )
        {
            assertThrows( DatabaseManagementException.class, () -> managementService.dropDatabase( databaseName ) );
        }
    }

    @Test
    void failToStartStopSystemDatabase()
    {
        assertThrows( DatabaseManagementException.class, () -> managementService.shutdownDatabase( SYSTEM_DATABASE_NAME ) );
        assertThrows( DatabaseManagementException.class, () -> managementService.startDatabase( SYSTEM_DATABASE_NAME ) );
    }

    @Test
    void shutdownShouldShutdownAllDatabases()
    {
        ShutdownListenerDatabaseEventListener shutdownListenerDatabaseEventListener = new ShutdownListenerDatabaseEventListener();
        managementService.registerDatabaseEventListener( shutdownListenerDatabaseEventListener );
        managementService.shutdown();
        managementService = null;

        assertEquals( 2, shutdownListenerDatabaseEventListener.getShutdownInvocations() );
    }

    private DatabaseManagementService getDatabaseManagementService()
    {
        DatabaseManagementServiceFactory databaseManagementServiceFactory =
                new DatabaseManagementServiceFactory( DatabaseInfo.COMMUNITY, CommunityEditionModule::new );
        Config cfg = Config.defaults( neo4j_home, testDirectory.absolutePath().toPath() );
        return databaseManagementServiceFactory.build( testDirectory.storeDir(), cfg, GraphDatabaseDependencies.newDependencies() );
    }

    private static class ShutdownListenerDatabaseEventListener extends DatabaseEventListenerAdapter
    {
        private final AtomicLong shutdownInvocations = new AtomicLong();

        @Override
        public void databaseShutdown( DatabaseEventContext eventContext )
        {
            shutdownInvocations.incrementAndGet();
        }

        long getShutdownInvocations()
        {
            return shutdownInvocations.get();
        }
    }
}
