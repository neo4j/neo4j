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

import static org.neo4j.kernel.Config.ENABLE_ONLINE_BACKUP;
import static org.neo4j.kernel.Config.KEEP_LOGICAL_LOGS;
import static org.neo4j.kernel.Config.parseMapFromConfigValue;

import org.neo4j.helpers.Args;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.KernelData;
import org.neo4j.kernel.KernelExtension;
import org.neo4j.kernel.impl.util.StringLogger;

@Service.Implementation( KernelExtension.class )
public class OnlineBackupExtension extends KernelExtension<BackupServer>
{
    static final String KEY = "online backup";

    public OnlineBackupExtension()
    {
        super( KEY );
    }

    @Override
    protected void loadConfiguration( KernelData kernel )
    {
        if ( parsePort( (String) kernel.getParam( ENABLE_ONLINE_BACKUP ) ) != null )
        {
            // Means that online backup will be enabled
            kernel.getConfig().getParams().put( KEEP_LOGICAL_LOGS, "true" );
        }
    }

    @Override
    protected BackupServer load( KernelData kernel )
    {
        Integer port = parsePort( (String) kernel.getParam( ENABLE_ONLINE_BACKUP ) );
        if ( port != null )
        {
            TheBackupInterface backup = new BackupImpl( kernel.graphDatabase() );
            BackupServer server = new BackupServer( backup, port.intValue(),
                    (StringLogger) kernel.getConfig().getParams().get( StringLogger.class ) );
            return server;
        }
        return null;
    }

    public static Integer parsePort( String backupConfigValue )
    {
        if ( backupConfigValue != null )
        {   // Backup is configured
            if ( Config.configValueContainsMultipleParameters( backupConfigValue ) )
            {   // Multi-value config, which means we have to parse the port
                Args args = parseMapFromConfigValue( ENABLE_ONLINE_BACKUP, backupConfigValue );
                return args.getNumber( "port", BackupServer.DEFAULT_PORT ).intValue();
            }
            else if ( Boolean.parseBoolean( backupConfigValue ) )
            {   // Single-value config, true/false
                return BackupServer.DEFAULT_PORT;
            }
        }
        return null;
    }

    @Override
    protected void unload( BackupServer server )
    {
        server.shutdown();
    }
}
