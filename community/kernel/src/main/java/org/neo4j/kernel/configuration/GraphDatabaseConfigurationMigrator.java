/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.neo4j.helpers.Settings;

import static java.util.regex.Pattern.quote;

public class GraphDatabaseConfigurationMigrator extends BaseConfigurationMigrator
{

    private static final String KEEP_LOGICAL_LOGS = "keep_logical_logs";

    {
        add( new SpecificPropertyMigration( "enable_online_backup",
                "enable_online_backup has been replaced with online_backup_enabled and online_backup_port" )
        {
            @Override
            public void setValueWithOldSetting( String value, Map<String, String> rawConfiguration )
            {
                if ( value != null )
                {
                    String port = null;

                    // Backup is configured
                    if ( value.contains( "=" ) )
                    {   // Multi-value config, which means we have to parse the port
                        Args args = parseMapFromConfigValue( "enable_online_backup", value );
                        port = args.get( "port", "6362" );
                        port = "0.0.0.0:"+port;
                    }
                    else if ( Boolean.parseBoolean( value ) )
                    {   // Single-value config, true/false
                        port = "0.0.0.0:6362-6372";
                    }

                    if ( port != null )
                    {
                        rawConfiguration.put( "online_backup_server", port );
                        rawConfiguration.put( "online_backup_enabled", Settings.TRUE );
                    }
                }
            }
        } );

        add( new SpecificPropertyMigration( "online_backup_port",
                "online_backup_port has been replaced with online_backup_server, which is a hostname:port setting" )
        {
            @Override
            public void setValueWithOldSetting( String value, Map<String, String> rawConfiguration )
            {
                if ( value != null )
                {
                    rawConfiguration.put( "online_backup_server", "0.0.0.0:"+value );
                }
            }
        } );

        add( new SpecificPropertyMigration( "neo4j.ext.udc.disable", "neo4j.ext.udc.disable has been replaced with " +
                "neo4j.ext.udc.enabled" )
        {
            @Override
            public void setValueWithOldSetting( String value, Map<String, String> rawConfiguration )
            {
                if ( "true".equalsIgnoreCase( value ) )
                {
                    rawConfiguration.put( "neo4j.ext.udc.enabled", "false" );
                }
                else
                {
                    rawConfiguration.put( "neo4j.ext.udc.enabled", "true" );
                }
            }
        } );

        add( new SpecificPropertyMigration( "enable_remote_shell",
                                            "enable_remote_shell has been replaced with remote_shell_enabled" )
        {
            @Override
            public void setValueWithOldSetting( String value, Map<String, String> rawConfiguration )
            {
                if ( configValueContainsMultipleParameters( value ) )
                {
                    rawConfiguration.put( "remote_shell_enabled", Settings.TRUE );

                    Args parsed = parseMapFromConfigValue( "enable_remote_shell", value );
                    Map<String, String> map = new HashMap<String, String>();
                    map.put( "remote_shell_port", parsed.get( "port", "1337" ) );
                    map.put( "remote_shell_name", parsed.get( "name", "shell" ) );
                    map.put( "remote_shell_read_only", parsed.get( "readonly", "false" ) );

                    rawConfiguration.putAll( map );
                }
                else
                {
                    rawConfiguration.put( "remote_shell_enabled", Boolean.parseBoolean( value ) ? Settings.TRUE :
                            Settings.FALSE );
                }
            }
        } );

        add( new SpecificPropertyMigration( KEEP_LOGICAL_LOGS, "multi-value configuration of keep_logical_logs" +
                " has been removed, any configuration specified will apply to all data sources" )
        {
            @Override
            public boolean appliesTo( Map<String, String> rawConfiguration )
            {
                return configValueContainsMultipleParameters( rawConfiguration.get( KEEP_LOGICAL_LOGS ) );
            }

            @Override
            public void setValueWithOldSetting( String value, Map<String, String> rawConfiguration )
            {
                boolean keep = false;
                Args map = parseMapFromConfigValue( KEEP_LOGICAL_LOGS, value );
                for ( Map.Entry<String, String> entry : map.asMap().entrySet() )
                {
                    if ( Boolean.parseBoolean( entry.getValue() ) )
                    {
                        keep = true;
                        break;
                    }
                }
                rawConfiguration.put( KEEP_LOGICAL_LOGS, String.valueOf( keep ) );
            }
        } );

        add( new SpecificPropertyMigration( "lucene_writer_cache_size", "cannot configure writers and searchers " +
                "individually since they go together" )
        {
            @Override
            public void setValueWithOldSetting( String value, Map<String, String> rawConfiguration )
            {
            }
        } );

        add( new ConfigValueChanged( "cache_type", "gcr", "hpc",
                "'gcr' cache type has been renamed to 'hpc', High Performance Cache." ));
    }

    @Deprecated
    public static boolean configValueContainsMultipleParameters( String configValue )
    {
        return configValue != null && configValue.contains( "=" );
    }

    @Deprecated
    public static Args parseMapFromConfigValue( String name, String configValue )
    {
        Map<String, String> result = new HashMap<>();
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