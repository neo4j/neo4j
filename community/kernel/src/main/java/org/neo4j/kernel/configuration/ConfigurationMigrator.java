/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

package org.neo4j.kernel.configuration;

import java.util.HashMap;
import java.util.Map;
import org.neo4j.helpers.Args;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * Migration of configuration settings. This allows old configurations to be read and converted into the new format.
 */
public class ConfigurationMigrator
{
    private StringLogger messageLog;
    private boolean anyDeprecatedSettings = false;

    public ConfigurationMigrator( StringLogger messageLog )
    {
        this.messageLog = messageLog;
    }

    public Map<String, String> migrateConfiguration( Map< String, String> inputParams )
    {
        Map<String, String> migratedConfiguration = new HashMap<String, String>( );
        
        for( Map.Entry<String, String> configEntry : inputParams.entrySet() )
        {
            String key = configEntry.getKey();
            String value = configEntry.getValue();

            if ( key.equals( Config.ENABLE_ONLINE_BACKUP ))
            {
                // Online backup
                Integer port = parseBackupPort( value );
                if (port != null)
                {
                    migratedConfiguration.put( "online_backup_enabled", "true" );
                    migratedConfiguration.put( "online_backup_port", port.toString() );
                }

                deprecationMessage( "enable_online_backup has been replaced with online_backup_enabled and online_backup_port" );
                continue;
            }

            // Flip the UDC enable setting
            if (key.equals( "neo4j.ext.udc.disable" ))
            {
                if ("true".equalsIgnoreCase( value ))
                {
                    migratedConfiguration.put( "neo4j.ext.udc.enabled", "false" );
                } else
                {
                    migratedConfiguration.put( "neo4j.ext.udc.enabled", "true" );
                }
                deprecationMessage( "neo4j.ext.udc.disable has been replaced with neo4j.ext.udc.enabled" );
                continue;
            }

            // TODO Add migration rules for Community here (e.g. keep_logical_logs needs to be added here)

            migratedConfiguration.put( key, value );
        }
        
        return migratedConfiguration;
    }
    
    protected void deprecationMessage(String message)
    {
        if (!anyDeprecatedSettings)
        {
            anyDeprecatedSettings = true;
            messageLog.logMessage( "WARNING! Deprecated configuration options used. See manual for details" );
        }

        messageLog.logMessage( message );
    }

    private Integer parseBackupPort( String backupConfigValue )
    {
        if ( backupConfigValue != null )
        {   // Backup is configured
            if ( Config.configValueContainsMultipleParameters( backupConfigValue ) )
            {   // Multi-value config, which means we have to parse the port
                Args args = Config.parseMapFromConfigValue( Config.ENABLE_ONLINE_BACKUP, backupConfigValue );
                return args.getNumber( "port", 6362 ).intValue();
            }
            else if ( Boolean.parseBoolean( backupConfigValue ) )
            {   // Single-value config, true/false
                return 6362;
            }
        }
        return null;
    }

}
