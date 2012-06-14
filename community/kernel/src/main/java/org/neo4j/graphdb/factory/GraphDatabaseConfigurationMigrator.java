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
package org.neo4j.graphdb.factory;

import static java.util.regex.Pattern.quote;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.helpers.Args;
import org.neo4j.kernel.configuration.BaseConfigurationMigrator;

public class GraphDatabaseConfigurationMigrator extends BaseConfigurationMigrator 
{
    {
        add(new SpecificPropertyMigration("enable_online_backup", "enable_online_backup has been replaced with online_backup_enabled and online_backup_port")
        {
            @Override
            public void setValueWithOldSetting(String value, Map<String, String> rawConfiguration)
            {
                if ( value != null )
                {
                    String port = null;
                    
                    // Backup is configured
                    if ( value.contains("=") )
                    {   // Multi-value config, which means we have to parse the port
                        Args args = parseMapFromConfigValue( "enable_online_backup", value );
                        port = args.get("port", "6362" );
                    }
                    else if ( Boolean.parseBoolean( value ) == true )
                    {   // Single-value config, true/false
                        port = "6362";
                    }
                    
                    if( port != null) 
                    {
                        rawConfiguration.put( "online_backup_port", port );
                        rawConfiguration.put( GraphDatabaseSettings.online_backup_enabled.name(), GraphDatabaseSetting.TRUE );
                    }
                }
            }
        });
        
        add(new SpecificPropertyMigration("neo4j.ext.udc.disable", "neo4j.ext.udc.disable has been replaced with neo4j.ext.udc.enabled")
        {
            @Override
            public void setValueWithOldSetting(String value, Map<String, String> rawConfiguration)
            {
                if ("true".equalsIgnoreCase( value ))
                {
                    rawConfiguration.put( "neo4j.ext.udc.enabled", "false" );
                } else
                {
                    rawConfiguration.put( "neo4j.ext.udc.enabled", "true" );
                }
            }
        });
        
        add(new SpecificPropertyMigration("enable_remote_shell", "neo4j.ext.udc.disable has been replaced with neo4j.ext.udc.enabled")
        {
            @Override
            public void setValueWithOldSetting(String value, Map<String, String> rawConfiguration)
            {
                Map<String, Serializable> config = null;
                boolean enable = false;

                if ( configValueContainsMultipleParameters( value ) )
                {
                    rawConfiguration.put( "remote_shell_enabled", GraphDatabaseSetting.TRUE );
                    
                    Args parsed = parseMapFromConfigValue( "enable_remote_shell", value );
                    Map<String, String> map = new HashMap<String, String>();
                    map.put( "remote_shell_port", parsed.get( "port", "1337" ) );
                    map.put( "remote_shell_name", parsed.get( "name", "shell" ) );
                    map.put( "remote_shell_read_only", parsed.get( "readonly", "false" ) );
                    
                    rawConfiguration.putAll(map);
                }
                else
                {
                    rawConfiguration.put( "remote_shell_enabled", Boolean.parseBoolean( value ) ? GraphDatabaseSetting.TRUE : GraphDatabaseSetting.FALSE );
                }
            }
        });
    }
    
    @Deprecated
    public static boolean configValueContainsMultipleParameters( String configValue )
    {
        return configValue != null && configValue.contains( "=" );
    }
    
    @Deprecated
    public static Args parseMapFromConfigValue( String name, String configValue )
    {
        Map<String, String> result = new HashMap<String, String>();
        for ( String part : configValue.split( quote( "," ) ) )
        {
            String[] tokens = part.split( quote( "=" ) );
            if ( tokens.length != 2 )
            {
                throw new RuntimeException( "Invalid configuration value '" + configValue +
                        "' for " + name + ". The format is [true/false] or [key1=value1,key2=value2...]" );
            }
            result.put( tokens[0], tokens[1] );
        }
        return new Args( result );
    }
}