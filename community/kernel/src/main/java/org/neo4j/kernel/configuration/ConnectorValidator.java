/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.config.SettingGroup;

public abstract class ConnectorValidator implements SettingGroup<Object>
{
    public static final List<String> validTypes =
            Arrays.stream( Connector.ConnectorType.values() )
                    .map( Enum::name )
                    .collect( Collectors.toList() );
    protected final Connector.ConnectorType type;

    public ConnectorValidator( @Nonnull Connector.ConnectorType type )
    {
        this.type = type;
    }

    /**
     * @return a list of the internal settings of this connector. For example, if this connector is
     * "dbms.connector.bolt.*" then this method returns all strings which can replace the final '*'.
     */
    @Nonnull
    public abstract List<String> subSettings();

    /**
     * Determine if this instance is responsible for validating a setting.
     *
     * @param key the key of the setting
     * @param rawConfig raw map of config settings to validate
     * @return true if this instance is responsible for parsing the setting, false otherwise.
     * @throws InvalidSettingException if an answer can not be determined, for example in case of a missing second
     * mandatory setting.
     */
    public boolean owns( @Nonnull String key, @Nonnull Map<String,String> rawConfig ) throws InvalidSettingException
    {
        String[] parts = key.split( "\\." );
        if ( parts.length != 4 )
        {
            return false;
        }
        if ( !parts[0].equals( "dbms" ) || !parts[1].equals( "connector" ) )
        {
            return false;
        }

        if ( !subSettings().contains( parts[3] ) )
        {
            return false;
        }

        // A type must be specified, or it is not possible to know who owns this setting
        String groupKey = parts[2];
        String typeKey = String.join( ".", parts[0], parts[1], groupKey, "type" );
        String typeValue = rawConfig.get( typeKey );

        if ( typeValue == null )
        {
            // We can infer the type of the connector from some names
            if ( groupKey.equalsIgnoreCase( "http" ) || groupKey.equalsIgnoreCase( "https" ) )
            {
                typeValue = Connector.ConnectorType.HTTP.name();
            }
            else if ( groupKey.equalsIgnoreCase( "bolt" ) )
            {
                typeValue = Connector.ConnectorType.BOLT.name();
            }
        }

        // If this is a connector not called bolt or http, then we require the type
        if ( typeValue == null )
        {
            throw new InvalidSettingException( String.format( "Missing mandatory value for '%s'",
                    typeKey ) );
        }

        if ( !validTypes.contains( typeValue ) )
        {
            throw new InvalidSettingException(
                    String.format( "'%s' must be one of %s; not '%s'",
                            typeKey, String.join( ", ", validTypes ), typeValue ) );
        }

        return this.type.name().equals( typeValue );
    }

    @Nonnull
    public Stream<Entry<String,String>> ownedEntries( @Nonnull Map<String,String> params )
            throws InvalidSettingException
    {
        return params.entrySet().stream()
                .filter( it -> owns( it.getKey(), params ) );
    }
}
