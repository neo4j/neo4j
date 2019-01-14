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
package org.neo4j.kernel;

import org.neo4j.internal.diagnostics.DiagnosticsManager;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.internal.KernelDiagnostics;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class DatabaseDiagnostics extends LifecycleAdapter
{
    private final DiagnosticsManager diagnosticsManager;
    private final NeoStoreDataSource neoStoreDataSource;
    private final DatabaseInfo databaseInfo;

    DatabaseDiagnostics( DiagnosticsManager diagnosticsManager, NeoStoreDataSource neoStoreDataSource, DatabaseInfo databaseInfo )
    {
        this.diagnosticsManager = diagnosticsManager;
        this.neoStoreDataSource = neoStoreDataSource;

        this.databaseInfo = databaseInfo;
    }
    @Override
    public void start() throws Throwable
    {
        diagnosticsManager.prependProvider( new KernelDiagnostics.Versions( databaseInfo, neoStoreDataSource.getStoreId() ) );
        neoStoreDataSource.registerDiagnosticsWith( diagnosticsManager );
        diagnosticsManager.appendProvider( new KernelDiagnostics.StoreFiles( neoStoreDataSource.getDatabaseLayout() ) );
    }
}
