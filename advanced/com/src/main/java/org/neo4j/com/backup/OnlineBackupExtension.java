/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.com.backup;

import static java.util.regex.Pattern.quote;
import static org.neo4j.kernel.Config.ENABLE_ONLINE_BACKUP;
import static org.neo4j.kernel.Config.KEEP_LOGICAL_LOGS;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.KernelExtension;

@Service.Implementation( KernelExtension.class )
public class OnlineBackupExtension extends KernelExtension
{
    public OnlineBackupExtension()
    {
        super( "online backup" );
    }
    
    @Override
    protected void preInit( KernelData kernel )
    {
        if ( parsePort( kernel ) != null )
        {
            // Means that online backup will be enabled
            kernel.getConfig().getParams().put( KEEP_LOGICAL_LOGS, "true" );
        }
    }
    
    @Override
    protected void load( KernelData kernel )
    {
        Integer port = parsePort( kernel );
        if ( port != null )
        {
            TheBackupInterface backup = new BackupImpl( kernel.graphDatabase() );
            BackupServer server = new BackupServer( backup, port,
                    (String) kernel.getConfig().getParams().get( "store_dir" ) );
            kernel.setState( this, server );
        }
    }
    
    private Integer parsePort( KernelData kernel )
    {
        String configValue = (String) kernel.getParam( ENABLE_ONLINE_BACKUP );
        if ( configValue != null )
        {
            int port = BackupServer.DEFAULT_PORT;
            if ( configValue.contains( "=" ) )
            {
                Map<String, String> args = parseConfigValue( configValue );
                if ( args.containsKey( "port" ) )
                {
                    port = Integer.parseInt( args.get( "port" ) );
                }
            }
            else if ( !Boolean.parseBoolean( configValue ) )
            {
                return null;
            }
            return port;
        }
        return null;
    }
    
    private Map<String, String> parseConfigValue( String configValue )
    {
        Map<String, String> result = new HashMap<String, String>();
        for ( String part : configValue.split( quote( "," ) ) )
        {
            String[] tokens = part.split( quote( "=" ) );
            if ( tokens.length != 2 )
            {
                throw new RuntimeException( "Invalid configuration value '" + configValue + 
                        "' for " + ENABLE_ONLINE_BACKUP );
            }
            result.put( tokens[0], tokens[1] );
        }
        return result;
    }

    @Override
    protected void unload( KernelData kernel )
    {
        BackupServer server = (BackupServer) kernel.getState( this );
        if ( server != null )
        {
            server.shutdown();
        }
    }
}
