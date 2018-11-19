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
package org.neo4j.diagnostics.providers;

import java.util.List;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.internal.diagnostics.DiagnosticsManager;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.logging.Log;
import org.neo4j.storageengine.api.StorageEngine;

public class DbmsDiagnosticsManager
{
    private final Dependencies dependencies;
    private final DiagnosticsManager diagnosticsManager;

    public DbmsDiagnosticsManager( Dependencies dependencies, DiagnosticsManager diagnosticsManager )
    {
        this.dependencies = dependencies;
        this.diagnosticsManager = diagnosticsManager;
    }

    public void dumpSystemDiagnostics()
    {
        dumpSystemDiagnostics( diagnosticsManager.getLog() );
    }

    public void dumpDatabaseDiagnostics( NeoStoreDataSource neoStoreDataSource )
    {
        dumpDatabaseDiagnostics( neoStoreDataSource, diagnosticsManager.getLog() );
    }

    public void dumpAll()
    {
        dumpAll( diagnosticsManager.getLog() );
    }

    public void dumpAll( Log log )
    {
        dumpSystemDiagnostics( log );
        dumpAllDatabases( log );
    }

    public void dump( String databaseName )
    {
        dump( databaseName, diagnosticsManager.getLog() );
    }

    public void dump( String databaseName, Log log )
    {
        getDatabaseManager().getDatabaseFacade( databaseName ).map(
                facade -> facade.getDependencyResolver().resolveDependency( NeoStoreDataSource.class ) )
                .ifPresent( dataSource -> dumpDatabaseDiagnostics( dataSource, log ) );
    }

    private void dumpAllDatabases( Log log )
    {
        List<String> databases = getDatabaseManager().listDatabases();
        for ( String database : databases )
        {
            dump( database, log );
        }
    }

    private void dumpSystemDiagnostics( Log log )
    {
        diagnosticsManager.dump( SystemDiagnostics.class, log );
        diagnosticsManager.dump( new ConfigDiagnostics( dependencies.resolveDependency( Config.class ) ), log );
    }

    private void dumpDatabaseDiagnostics( NeoStoreDataSource neoStoreDataSource, Log log )
    {
        Dependencies databaseResolver = neoStoreDataSource.getDependencyResolver();
        DatabaseInfo databaseInfo = databaseResolver.resolveDependency( DatabaseInfo.class );
        StorageEngine storageEngine = databaseResolver.resolveDependency( StorageEngine.class );

        diagnosticsManager.dump( new KernelDiagnostics.Versions( databaseInfo, neoStoreDataSource.getStoreId() ), log );
        diagnosticsManager.dump( new KernelDiagnostics.StoreFiles( neoStoreDataSource.getDatabaseLayout() ), log );
        diagnosticsManager.dump( new DataSourceDiagnostics.TransactionRangeDiagnostics( neoStoreDataSource ), log );
        storageEngine.dumpDiagnostics( diagnosticsManager, log );
    }

    private DatabaseManager getDatabaseManager()
    {
        return dependencies.resolveDependency( DatabaseManager.class );
    }
}
