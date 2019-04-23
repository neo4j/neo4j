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
package org.neo4j.dmbs.database;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;

import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.UnableToStartDatabaseException;
import org.neo4j.graphdb.factory.DatabaseManagementServiceBuilder;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.matchers.NestedThrowableMatcher;
import org.neo4j.test.rule.TestDirectory;

import static org.apache.commons.io.FileUtils.deleteDirectory;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

@ExtendWith( TestDirectoryExtension.class )
class DatabaseFailureIT
{
    private static final DatabaseId DEFAULT_DATABASE_ID = new DatabaseId( DEFAULT_DATABASE_NAME );
    private static final DatabaseId SYSTEM_DATABASE_ID = new DatabaseId( SYSTEM_DATABASE_NAME );
    @Inject
    private TestDirectory testDirectory;
    private GraphDatabaseAPI database;
    private DatabaseManagementService managementService;

    @BeforeEach
    void setUp()
    {
        database = startDatabase();
    }

    @AfterEach
    void tearDown()
    {
        managementService.shutdown();
    }

    @Test
    void startWhenDefaultDatabaseFailedToStart() throws IOException
    {
        managementService.shutdown();
        deleteDirectory( testDirectory.databaseLayout().getTransactionLogsDirectory() );

        database = startDatabase();
        var databaseManager = getDatabaseManager();
        assertTrue( databaseManager.getDatabaseContext( DEFAULT_DATABASE_ID ).get().isFailed() );
        assertFalse( databaseManager.getDatabaseContext( SYSTEM_DATABASE_ID ).get().isFailed() );
    }

    @Test
    void failToStartWhenSystemDatabaseFailedToStart() throws IOException
    {
        managementService.shutdown();
        deleteDirectory( testDirectory.databaseLayout( SYSTEM_DATABASE_NAME ).getTransactionLogsDirectory() );

        Exception startException = assertThrows( Exception.class, this::startDatabase );
        assertThat( startException, new NestedThrowableMatcher( UnableToStartDatabaseException.class ) );
    }

    private DatabaseManager<?> getDatabaseManager()
    {
        return database.getDependencyResolver().resolveDependency( DatabaseManager.class );
    }

    private GraphDatabaseAPI startDatabase()
    {
        startDatabaseServer();
        return (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
    }

    private void startDatabaseServer()
    {
        managementService = new DatabaseManagementServiceBuilder( testDirectory.storeDir() ).build();
    }
}
