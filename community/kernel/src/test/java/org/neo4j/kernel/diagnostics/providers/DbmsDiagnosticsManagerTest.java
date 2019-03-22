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
package org.neo4j.kernel.diagnostics.providers;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.TreeMap;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.StandaloneDatabaseContext;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageEngineFactory;

import static java.util.Collections.singletonMap;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DbmsDiagnosticsManagerTest
{
    private static final String DEFAULT_DATABASE_NAME = "testDatabase";
    private static final DatabaseId DEFAULT_DATABASE_ID = new DatabaseId( DEFAULT_DATABASE_NAME );

    private DbmsDiagnosticsManager diagnosticsManager;
    private AssertableLogProvider logProvider;
    private DatabaseManager<StandaloneDatabaseContext> databaseManager;
    private StorageEngine storageEngine;
    private StorageEngineFactory storageEngineFactory;
    private Database defaultDatabase;

    @BeforeEach
    @SuppressWarnings( "unchecked" )
    void setUp() throws IOException
    {
        logProvider = new AssertableLogProvider();
        databaseManager = mock( DatabaseManager.class );

        storageEngine = mock( StorageEngine.class );
        storageEngineFactory = mock( StorageEngineFactory.class );
        defaultDatabase = prepareDatabase();
        when( storageEngineFactory.listStorageFiles( any(), any() ) ).thenReturn( Collections.emptyList() );

        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependency( Config.defaults() );
        dependencies.satisfyDependency( databaseManager );

        StandaloneDatabaseContext context = mock( StandaloneDatabaseContext.class );
        when( context.database() ).thenReturn( defaultDatabase );
        when( databaseManager.getDatabaseContext( DEFAULT_DATABASE_ID ) ).thenReturn( Optional.of( context ) );
        when( databaseManager.registeredDatabases() ).thenReturn( new TreeMap<>( singletonMap( DEFAULT_DATABASE_ID, context ) ) );

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
        databaseDependencies.satisfyDependency( storageEngineFactory );
        databaseDependencies.satisfyDependency( new DefaultFileSystemAbstraction() );
        when( database.getDependencyResolver() ).thenReturn( databaseDependencies );
        when( database.getDatabaseId() ).thenReturn( DEFAULT_DATABASE_ID );
        return database;
    }
}
