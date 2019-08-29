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
package org.neo4j.dbms.database;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.kernel.database.DatabaseIdRepository.SYSTEM_DATABASE_ID;

@TestDirectoryExtension
class DefaultDatabaseManagerIT
{
    private DatabaseId defaultDatabaseId;

    @Inject
    private TestDirectory testDirectory;
    private GraphDatabaseService database;
    private DatabaseManagementService managementService;
    private DatabaseManager<?> databaseManager;

    @BeforeEach
    void setUp()
    {
        managementService = new DatabaseManagementServiceBuilder( testDirectory.storeDir() ).build();
        database = managementService.database( DEFAULT_DATABASE_NAME );
        databaseManager = ((GraphDatabaseAPI)database).getDependencyResolver().resolveDependency( DatabaseManager.class );
        defaultDatabaseId = databaseManager.databaseIdRepository().get( DEFAULT_DATABASE_NAME ).get();
    }

    @AfterEach
    void tearDown()
    {
        managementService.shutdown();
    }

    @Test
    void createDatabase()
    {
        assertThrows( DatabaseManagementException.class, () -> databaseManager.createDatabase( defaultDatabaseId ) );
    }

    @Test
    void lookupExistingDatabase()
    {
        var defaultDatabaseContext = databaseManager.getDatabaseContext( defaultDatabaseId );
        var systemDatabaseContext = databaseManager.getDatabaseContext( SYSTEM_DATABASE_ID );

        assertTrue( defaultDatabaseContext.isPresent() );
        assertTrue( systemDatabaseContext.isPresent() );
    }

    @Test
    void listDatabases()
    {
        var databases = databaseManager.registeredDatabases();
        assertEquals( 2, databases.size()  );
        ArrayList<DatabaseId> databaseNames = new ArrayList<>( databases.keySet() );
        assertEquals( SYSTEM_DATABASE_ID, databaseNames.get( 0 ) );
        assertEquals( defaultDatabaseId, databaseNames.get( 1 ) );
    }

    @Test
    void shutdownDatabaseOnStop() throws Throwable
    {
        databaseManager.stop();
        assertFalse( database.isAvailable( 0 ) );
    }
}
