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

import java.util.List;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.internal.diagnostics.DiagnosticsManager;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.storageengine.api.StorageEngine;

public class DbmsDiagnosticsManager
{
    private final Dependencies dependencies;
    private final DiagnosticsManager diagnosticsManager;
    private final Log log;

    public DbmsDiagnosticsManager( Dependencies dependencies, LogService logService )
    {
        this.log = logService.getInternalLog( DiagnosticsManager.class );
        this.dependencies = dependencies;
        this.diagnosticsManager = new DiagnosticsManager( log );
    }

    public void dumpSystemDiagnostics()
    {
        dumpSystemDiagnostics( log );
    }

    public void dumpDatabaseDiagnostics( Database database )
    {
        dumpDatabaseDiagnostics( database, log );
    }

    public void dumpAll()
    {
        dumpAll( log );
    }

    public void dumpAll( Log log )
    {
        dumpSystemDiagnostics( log );
        dumpAllDatabases( log );
    }

    public void dump( String databaseName )
    {
        dump( databaseName, log );
    }

    public void dump( String databaseName, Log log )
    {
        getDatabaseManager().getDatabaseContext( databaseName ).map( DatabaseContext::getDatabase )
                .ifPresent( database -> dumpDatabaseDiagnostics( database, log ) );
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
        diagnosticsManager.section( log, "System diagnostics" );
        diagnosticsManager.dump( SystemDiagnostics.class, log );
        diagnosticsManager.dump( new ConfigDiagnostics( dependencies.resolveDependency( Config.class ) ), log );
    }

    private void dumpDatabaseDiagnostics( Database database, Log log )
    {
        Dependencies databaseResolver = database.getDependencyResolver();
        DatabaseInfo databaseInfo = databaseResolver.resolveDependency( DatabaseInfo.class );
        StorageEngine storageEngine = databaseResolver.resolveDependency( StorageEngine.class );

        diagnosticsManager.section( log, "Database: " + database.getDatabaseName() );
        diagnosticsManager.dump( new VersionDiagnostics( databaseInfo, database.getStoreId() ), log );
        diagnosticsManager.dump( new StoreFilesDiagnostics( database.getDatabaseLayout() ), log );
        diagnosticsManager.dump( new TransactionRangeDiagnostics( database ), log );
        storageEngine.dumpDiagnostics( diagnosticsManager, log );
    }

    private DatabaseManager getDatabaseManager()
    {
        return dependencies.resolveDependency( DatabaseManager.class );
    }
}
