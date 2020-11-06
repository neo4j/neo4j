/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.util.Collection;
import java.util.List;
import java.util.StringJoiner;
import java.util.function.Consumer;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.internal.diagnostics.DiagnosticsLogger;
import org.neo4j.internal.diagnostics.DiagnosticsManager;
import org.neo4j.internal.diagnostics.DiagnosticsProvider;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.internal.LogService;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageEngineFactory;

import static java.lang.String.format;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.neo4j.util.FeatureToggles.getInteger;

public class DbmsDiagnosticsManager
{
    private static final int CONCISE_DATABASE_DUMP_THRESHOLD = getInteger( DbmsDiagnosticsManager.class, "conciseDumpThreshold", 10 );
    private static final int CONCISE_DATABASE_NAMES_PER_ROW = 5;
    private final Dependencies dependencies;
    private final boolean enabled;
    private final Log log;

    public DbmsDiagnosticsManager( Dependencies dependencies, LogService logService )
    {
        this.log = logService.getInternalLog( DiagnosticsManager.class );
        this.dependencies = dependencies;
        this.enabled = dependencies.resolveDependency( Config.class ).get( GraphDatabaseInternalSettings.dump_diagnostics );
    }

    public void dumpSystemDiagnostics()
    {
        if ( enabled )
        {
            dumpSystemDiagnostics( log );
        }
    }

    public void dumpDatabaseDiagnostics( Database database )
    {
        if ( enabled )
        {
            dumpDatabaseDiagnostics( database, log, false );
        }
    }

    public void dumpAll()
    {
        dumpAll( log );
    }

    public void dumpAll( Log log )
    {
        if ( enabled )
        {
            dumpSystemDiagnostics( log );
            dumpAllDatabases( log );
        }
    }

    private void dumpAllDatabases( Log log )
    {
        Collection<? extends DatabaseContext> values = getDatabaseManager().registeredDatabases().values();
        if ( values.size() > CONCISE_DATABASE_DUMP_THRESHOLD )
        {
            dumpConciseDiagnostics( values, log );
        }
        else
        {
            values.forEach( dbCtx -> dumpDatabaseDiagnostics( dbCtx.database(), log, true ) );
        }
    }

    private void dumpConciseDiagnostics( Collection<? extends DatabaseContext> databaseContexts, Log log )
    {
        var startedDbs = databaseContexts.stream().map( DatabaseContext::database ).filter( Database::isStarted ).collect( toList() );
        var stoppedDbs = databaseContexts.stream().map( DatabaseContext::database ).filter( not( Database::isStarted ) ).collect( toList() );

        dumpAsSingleMessage( log, stringJoiner ->
        {
            logDatabasesState( stringJoiner::add, startedDbs, "Started" );
            logDatabasesState( stringJoiner::add, stoppedDbs, "Stopped" );
        } );
    }

    private void logDatabasesState( DiagnosticsLogger log, List<Database> databases, String state )
    {
        DiagnosticsManager.section( log, state + " Databases" );
        if ( databases.isEmpty() )
        {
            log.log( format( "There are no %s databases", state.toLowerCase() ) );
            return;
        }
        int lastIndex = 0;
        for ( int i = CONCISE_DATABASE_NAMES_PER_ROW; i < databases.size(); i += CONCISE_DATABASE_NAMES_PER_ROW )
        {
            var subList = databases.subList( lastIndex, i );
            logDatabases( log, subList );
            lastIndex = i;
        }
        var lastDbs = databases.subList( lastIndex, databases.size() );
        logDatabases( log, lastDbs );
    }

    private void logDatabases( DiagnosticsLogger log, List<Database> subList )
    {
        log.log( subList
                .stream()
                .map( database -> database.getNamedDatabaseId().name() )
                .collect( joining( ", " ) ) );
    }

    private void dumpSystemDiagnostics( Log log )
    {
        dumpAsSingleMessage( log, stringJoiner ->
        {
            DiagnosticsManager.section( stringJoiner::add, "System diagnostics" );
            DiagnosticsManager.dump( SystemDiagnostics.class, log, stringJoiner::add );
            DiagnosticsManager.dump( new ConfigDiagnostics( dependencies.resolveDependency( Config.class ) ), log, stringJoiner::add );
            // dump any custom additional diagnostics that can be registered by specific edition
            dependencies.resolveTypeDependencies( DiagnosticsProvider.class )
                    .forEach( provider -> DiagnosticsManager.dump( provider, log, stringJoiner::add ) );
        } );
    }

    private void dumpDatabaseDiagnostics( Database database, Log log, boolean checkStatus )
    {
        dumpAsSingleMessageWithDbPrefix( log, stringJoiner ->
        {
            dumpDatabaseSectionName( database, stringJoiner::add );
            if ( checkStatus )
            {
                logDatabaseStatus( database, stringJoiner::add );

                if ( !database.isStarted() )
                {
                    return;
                }
            }
            Dependencies databaseResolver = database.getDependencyResolver();
            DbmsInfo dbmsInfo = databaseResolver.resolveDependency( DbmsInfo.class );
            FileSystemAbstraction fs = databaseResolver.resolveDependency( FileSystemAbstraction.class );
            StorageEngineFactory storageEngineFactory = databaseResolver.resolveDependency( StorageEngineFactory.class );
            StorageEngine storageEngine = databaseResolver.resolveDependency( StorageEngine.class );

            DiagnosticsManager.dump( new VersionDiagnostics( dbmsInfo, database.getStoreId() ), log, stringJoiner::add );
            DiagnosticsManager.dump( new StoreFilesDiagnostics( storageEngineFactory, fs, database.getDatabaseLayout() ), log, stringJoiner::add );
            DiagnosticsManager.dump( new TransactionRangeDiagnostics( database ), log, stringJoiner::add );
            storageEngine.dumpDiagnostics( log, stringJoiner::add );
        }, database.getNamedDatabaseId() );
    }

    private void dumpAsSingleMessageWithDbPrefix( Log log, Consumer<StringJoiner> dumpFunction, NamedDatabaseId db )
    {
        dumpAsSingleMessageWithPrefix( log, dumpFunction, "[" + db.logPrefix() + "] " );
    }

    private void dumpAsSingleMessage( Log log, Consumer<StringJoiner> dumpFunction )
    {
                dumpAsSingleMessageWithPrefix( log, dumpFunction, "" );
    }

    /**
     * Messages will be buffered and logged as one single message to make sure that diagnostics are grouped together in the log.
     */
    private void dumpAsSingleMessageWithPrefix( Log log, Consumer<StringJoiner> dumpFunction, String prefix )
    {
        // Optimization to skip diagnostics dumping (which is time consuming) if there's no log anyway.
        // This is first and foremost useful for speeding up testing.
        if ( log == NullLog.getInstance() )
        {
            return;
        }

        StringJoiner message =
                new StringJoiner( System.lineSeparator() + " ".repeat( 64 ) + prefix, prefix + System.lineSeparator() + " ".repeat( 64 ) + prefix, "" );
        dumpFunction.accept( message );
        log.info( message.toString() );
    }

    private static void logDatabaseStatus( Database database, DiagnosticsLogger log )
    {
        log.log( format( "Database is %s.", database.isStarted() ? "started" : "stopped" ) );
    }

    private void dumpDatabaseSectionName( Database database, DiagnosticsLogger log )
    {
        DiagnosticsManager.section( log, "Database: " + database.getNamedDatabaseId().name() );
    }

    private DatabaseManager<?> getDatabaseManager()
    {
        return dependencies.resolveDependency( DatabaseManager.class );
    }
}
