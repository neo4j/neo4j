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

import java.util.ArrayList;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

@ExtendWith( TestDirectoryExtension.class )
class DefaultDatabaseManagerIT
{
    public static final DatabaseId DEFAULT_DATABASE_ID = new DatabaseId( DEFAULT_DATABASE_NAME );
    public static final DatabaseId SYSTEM_DATABASE_ID = new DatabaseId( SYSTEM_DATABASE_NAME );
    @Inject
    private TestDirectory testDirectory;
    private GraphDatabaseService database;

    @BeforeEach
    void setUp()
    {
        database = new GraphDatabaseFactory().newEmbeddedDatabase( testDirectory.databaseDir() );
    }

    @AfterEach
    void tearDown()
    {
        database.shutdown();
    }

    @Test
    void createDatabase()
    {
        DatabaseManager<?> databaseManager = getDatabaseManager();
        assertThrows( IllegalStateException.class, () -> databaseManager.createDatabase( DEFAULT_DATABASE_ID ) );
    }

    @Test
    void lookupExistingDatabase()
    {
        DatabaseManager<?> databaseManager = getDatabaseManager();
        var defaultDatabaseContext = databaseManager.getDatabaseContext( DEFAULT_DATABASE_ID );
        var systemDatabaseContext = databaseManager.getDatabaseContext( SYSTEM_DATABASE_ID );

        assertTrue( defaultDatabaseContext.isPresent() );
        assertTrue( systemDatabaseContext.isPresent() );
    }

    @Test
    void listDatabases()
    {
        DatabaseManager<?> databaseManager = getDatabaseManager();
        var databases = databaseManager.registeredDatabases();
        assertEquals( 2, databases.size()  );
        ArrayList<DatabaseId> databaseNames = new ArrayList<>( databases.keySet() );
        assertEquals( SYSTEM_DATABASE_ID, databaseNames.get( 0 ) );
        assertEquals( DEFAULT_DATABASE_ID, databaseNames.get( 1 ) );
    }

    @Test
    void shutdownDatabaseOnStop() throws Throwable
    {
        DatabaseManager<?> databaseManager = getDatabaseManager();
        databaseManager.stop();
        assertFalse( database.isAvailable( 0 ) );
    }

    private DatabaseManager<?> getDatabaseManager()
    {
        return ((GraphDatabaseAPI)database).getDependencyResolver().resolveDependency( DatabaseManager.class );
    }

}
