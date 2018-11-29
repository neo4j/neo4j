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
package org.neo4j.kernel.diagnostics.providers;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.storageengine.api.StorageEngine;

import static java.util.Collections.singletonList;
import static java.util.Optional.of;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DbmsDiagnosticsManagerTest
{
    private static final String DEFAULT_DATABASE_NAME = "testDatabase";

    private DbmsDiagnosticsManager diagnosticsManager;
    private AssertableLogProvider logProvider;
    private DatabaseManager databaseManager;
    private StorageEngine storageEngine;
    private Database defaultDatabase;

    @BeforeEach
    void setUp()
    {
        logProvider = new AssertableLogProvider();
        databaseManager = mock( DatabaseManager.class );

        storageEngine = mock( StorageEngine.class );
        defaultDatabase = prepareDatabase();

        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependency( Config.defaults() );
        dependencies.satisfyDependency( databaseManager );

        when( databaseManager.listDatabases() ).thenReturn( singletonList( DEFAULT_DATABASE_NAME ) );
        DatabaseContext context = mock( DatabaseContext.class );
        when( context.getDatabase() ).thenReturn( defaultDatabase );
        when( databaseManager.getDatabaseContext( DEFAULT_DATABASE_NAME ) ).thenReturn( of( context ) );

        diagnosticsManager = new DbmsDiagnosticsManager( dependencies, new SimpleLogService( logProvider ) );
    }

    @Test
    void dumpSystemDiagnostics()
    {
        logProvider.assertNoLoggingOccurred();

        diagnosticsManager.dumpSystemDiagnostics();

        assertContainsSystemDiagnostics();
    }

    @Test
    void dumpDatabaseDiagnostics()
    {
        logProvider.assertNoLoggingOccurred();

        diagnosticsManager.dumpDatabaseDiagnostics( defaultDatabase );

        assertContainsDatabaseDiagnostics();
    }

    @Test
    void dumpAllDiagnostics()
    {
        logProvider.assertNoLoggingOccurred();

        diagnosticsManager.dumpAll();

        assertContainsSystemDiagnostics();
        assertContainsDatabaseDiagnostics();
    }

    private void assertContainsSystemDiagnostics()
    {
        logProvider.assertContainsMessageContaining( "System diagnostics" );
        logProvider.assertContainsMessageContaining( "System memory information" );
        logProvider.assertContainsMessageContaining( "JVM memory information" );
        logProvider.assertContainsMessageContaining( "(IANA) TimeZone database version" );
        logProvider.assertContainsMessageContaining( "Operating system information" );
        logProvider.assertContainsMessageContaining( "System properties" );
        logProvider.assertContainsMessageContaining( "JVM information" );
        logProvider.assertContainsMessageContaining( "Java classpath" );
        logProvider.assertContainsMessageContaining( "Library path" );
        logProvider.assertContainsMessageContaining( "Network information" );
        logProvider.assertContainsMessageContaining( "DBMS config" );
    }

    private void assertContainsDatabaseDiagnostics()
    {
        logProvider.assertContainsMessageContaining( "Database: " + DEFAULT_DATABASE_NAME );
        logProvider.assertContainsMessageContaining( "Version" );
        logProvider.assertContainsMessageContaining( "Store files" );
        logProvider.assertContainsMessageContaining( "Transaction log" );
    }

    private Database prepareDatabase()
    {
        Database database = mock( Database.class );
        Dependencies databaseDependencies = new Dependencies();
        databaseDependencies.satisfyDependency( DatabaseInfo.COMMUNITY );
        databaseDependencies.satisfyDependency( storageEngine );
        when( database.getDependencyResolver() ).thenReturn( databaseDependencies );
        when( database.getDatabaseName() ).thenReturn( DbmsDiagnosticsManagerTest.DEFAULT_DATABASE_NAME );
        return database;
    }
}
