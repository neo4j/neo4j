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
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.StandaloneDatabaseContext;
import org.neo4j.internal.diagnostics.DiagnosticsProvider;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Logger;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageEngineFactory;

import static java.util.Collections.singletonMap;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DbmsDiagnosticsManagerTest
{
    private static final NamedDatabaseId DEFAULT_DATABASE_ID = TestDatabaseIdRepository.randomNamedDatabaseId();
    private static final String DEFAULT_DATABASE_NAME = DEFAULT_DATABASE_ID.name();

    private DbmsDiagnosticsManager diagnosticsManager;
    private AssertableLogProvider logProvider;
    private DatabaseManager<StandaloneDatabaseContext> databaseManager;
    private StorageEngine storageEngine;
    private StorageEngineFactory storageEngineFactory;
    private Database defaultDatabase;
    private StandaloneDatabaseContext defaultContext;
    private Dependencies dependencies;

    @BeforeEach
    @SuppressWarnings( "unchecked" )
    void setUp() throws IOException
    {
        logProvider = new AssertableLogProvider();
        databaseManager = mock( DatabaseManager.class );

        storageEngine = mock( StorageEngine.class );
        storageEngineFactory = mock( StorageEngineFactory.class );
        defaultContext = mock( StandaloneDatabaseContext.class );
        defaultDatabase = prepareDatabase();
        when( storageEngineFactory.listStorageFiles( any(), any() ) ).thenReturn( Collections.emptyList() );

        dependencies = new Dependencies();
        dependencies.satisfyDependency( Config.defaults() );
        dependencies.satisfyDependency( databaseManager );

        when( defaultContext.database() ).thenReturn( defaultDatabase );
        when( databaseManager.getDatabaseContext( DEFAULT_DATABASE_ID ) ).thenReturn( Optional.of( defaultContext ) );
        when( databaseManager.registeredDatabases() ).thenReturn( new TreeMap<>( singletonMap( DEFAULT_DATABASE_ID, defaultContext ) ) );

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
    void dumpDiagnosticOfStoppedDatabase()
    {
        when( defaultDatabase.isStarted() ).thenReturn( false );
        logProvider.assertNoLoggingOccurred();

        diagnosticsManager.dumpAll();

        logProvider.formattedMessageMatcher().assertContains( "Database: " + DEFAULT_DATABASE_NAME.toLowerCase() );
        logProvider.formattedMessageMatcher().assertContains( "Database is stopped." );
    }

    @Test
    void dumpDiagnosticsInConciseForm()
    {
        Map<NamedDatabaseId,StandaloneDatabaseContext> databaseMap = new HashMap<>();
        int numberOfDatabases = 1000;
        for ( int i = 0; i < numberOfDatabases; i++ )
        {
            Database database = mock( Database.class );
            NamedDatabaseId namedDatabaseId = TestDatabaseIdRepository.randomNamedDatabaseId();
            when( database.getNamedDatabaseId() ).thenReturn( namedDatabaseId );
            databaseMap.put( namedDatabaseId, new StandaloneDatabaseContext( database ) );
        }
        when( databaseManager.registeredDatabases() ).thenReturn( new TreeMap<>( databaseMap ) );

        diagnosticsManager.dumpAll();
        for ( var dbId : databaseMap.keySet() )
        {
            logProvider.formattedMessageMatcher().assertContains( "Database: " + dbId.name() );
        }
    }

    @Test
    @EnabledOnOs( OS.LINUX )
    void dumpNativeAccessProviderOnLinux()
    {
        diagnosticsManager.dumpAll();
        logProvider.rawMessageMatcher().assertContains( "Linux native access is available." );
    }

    @Test
    @DisabledOnOs( OS.LINUX )
    void dumpNativeAccessProviderOnNonLinux()
    {
        diagnosticsManager.dumpAll();
        logProvider.rawMessageMatcher().assertContains( "Native access is not available for current platform." );
    }

    @Test
    void dumpAllDiagnostics()
    {
        logProvider.assertNoLoggingOccurred();

        diagnosticsManager.dumpAll();

        assertContainsSystemDiagnostics();
        assertContainsDatabaseDiagnostics();
    }

    @Test
    void dumpAdditionalDiagnosticsIfPresent()
    {
        diagnosticsManager.dumpAll();

        assertNoAdditionalDiagnostics();

        DiagnosticsProvider diagnosticsProvider = new DiagnosticsProvider()
        {
            @Override
            public String getDiagnosticsName()
            {
                return "foo";
            }

            @Override
            public void dump( Logger logger )
            {
            }
        };
        dependencies.satisfyDependency( diagnosticsProvider );

        logProvider.clear();
        diagnosticsManager.dumpAll();
        assertContainingAdditionalDiagnostics( diagnosticsProvider );
    }

    private void assertContainsSystemDiagnostics()
    {
        logProvider.rawMessageMatcher().assertContains( "System diagnostics" );
        logProvider.rawMessageMatcher().assertContains( "System memory information" );
        logProvider.rawMessageMatcher().assertContains( "JVM memory information" );
        logProvider.rawMessageMatcher().assertContains( "(IANA) TimeZone database version" );
        logProvider.rawMessageMatcher().assertContains( "Operating system information" );
        logProvider.rawMessageMatcher().assertContains( "System properties" );
        logProvider.rawMessageMatcher().assertContains( "JVM information" );
        logProvider.rawMessageMatcher().assertContains( "Java classpath" );
        logProvider.rawMessageMatcher().assertContains( "Library path" );
        logProvider.rawMessageMatcher().assertContains( "Network information" );
        logProvider.rawMessageMatcher().assertContains( "DBMS config" );
    }

    private void assertContainingAdditionalDiagnostics( DiagnosticsProvider diagnosticsProvider )
    {
        logProvider.rawMessageMatcher().assertContains( diagnosticsProvider.getDiagnosticsName() );
    }

    private void assertNoAdditionalDiagnostics()
    {
        logProvider.rawMessageMatcher().assertNotContains( "Additional diagnostics" );
    }

    private void assertContainsDatabaseDiagnostics()
    {
        logProvider.rawMessageMatcher().assertContains( "Database: " + DEFAULT_DATABASE_NAME.toLowerCase() );
        logProvider.rawMessageMatcher().assertContains( "Version" );
        logProvider.rawMessageMatcher().assertContains( "Store files" );
        logProvider.rawMessageMatcher().assertContains( "Transaction log" );
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
        when( database.getNamedDatabaseId() ).thenReturn( DEFAULT_DATABASE_ID );
        when( database.isStarted() ).thenReturn( true );
        return database;
    }
}
