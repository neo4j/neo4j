/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.backup;

import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.logging.Logging;

public class OnlineBackupKernelExtension implements Lifecycle
{
    private Config config;
    private GraphDatabaseAPI graphDatabaseAPI;
    private BackupServer server;

    public OnlineBackupKernelExtension( Config config, GraphDatabaseAPI graphDatabaseAPI )
    {
        this.config = config;
        this.graphDatabaseAPI = graphDatabaseAPI;
    }

    @Override
    public void init() throws Throwable
    {
    }

    @Override
    public void start() throws Throwable
    {
        if ( config.<Boolean>get( OnlineBackupSettings.online_backup_enabled ) )
        {
            TheBackupInterface backup = new BackupImpl( graphDatabaseAPI );
            try
            {
                server = new BackupServer( backup,
                        config.get( OnlineBackupSettings.online_backup_port ),
                        graphDatabaseAPI.getDependencyResolver().resolveDependency( Logging.class ).getLogger(
                                OnlineBackupKernelExtension.class ) );
                server.init();
                server.start();
            }
            catch ( Throwable t )
            {
                throw new RuntimeException( t );
            }
        }
    }

    @Override
    public void stop() throws Throwable
    {
        if ( server != null )
        {
            server.stop();
            server.shutdown();
            server = null;
        }
    }

    @Override
    public void shutdown() throws Throwable
    {
    }
}
