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
package org.neo4j.kernel.configuration;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.config.SettingGroup;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public abstract class ConnectorValidator implements SettingGroup<Object>
{
    private static final Set<String> validTypes =
            Arrays.stream( Connector.ConnectorType.values() )
                    .map( Enum::name )
                    .collect( toSet() );
    static final String DEPRECATED_CONNECTOR_MSG =
            "Warning: connectors with names other than [http,https,bolt] are%n" +
                    "deprecated and support for them will be removed in a future%n" +
                    "version of Neo4j. Offending lines in " + Config.DEFAULT_CONFIG_FILE_NAME + ":%n%n%s";
    protected final Connector.ConnectorType type;

    public ConnectorValidator( @Nonnull Connector.ConnectorType type )
    {
        this.type = type;
    }

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
        if ( parts.length < 2 )
        {
            return false;
        }
        if ( !parts[0].equals( "dbms" ) || !parts[1].equals( "connector" ) )
        {
            return false;
        }

        // Do not allow invalid settings under 'dbms.connector.**'
        if ( parts.length != 4 )
        {
            throw new InvalidSettingException( format( "Invalid connector setting: %s", key ) );
        }

        /*if ( !subSettings().contains( parts[3] ) )
        {
            return false;
        }*/

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
            throw new InvalidSettingException( format( "Missing mandatory value for '%s'", typeKey ) );
        }

        if ( !validTypes.contains( typeValue ) )
        {
            throw new InvalidSettingException(
                    format( "'%s' must be one of %s; not '%s'",
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

    @Override
    @Nonnull
    public Map<String,String> validate( @Nonnull Map<String,String> rawConfig, @Nonnull Consumer<String> warningConsumer )
            throws InvalidSettingException
    {
        final HashMap<String,String> result = new HashMap<>();

        ownedEntries( rawConfig ).forEach( s ->
                result.putAll( getSettingFor( s.getKey(), rawConfig )
                        .orElseThrow( () -> new InvalidSettingException(
                                format( "Invalid connector setting: %s", s.getKey() ) ) )
                        .validate( rawConfig, warningConsumer ) ) );

        warnAboutDeprecatedConnectors( result, warningConsumer );

        return result;
    }

    private void warnAboutDeprecatedConnectors( @Nonnull Map<String,String> connectorSettings,
            @Nonnull Consumer<String> warningConsumer )
    {
        final HashSet<String> nonDefaultConnectors = new HashSet<>();
        connectorSettings.entrySet().stream()
                .map( Entry::getKey )
                .filter( settingKey ->
                {
                    String name = settingKey.split( "\\." )[2];
                    return isDeprecatedConnectorName( name );
                } )
                .forEach( nonDefaultConnectors::add );

        if ( !nonDefaultConnectors.isEmpty() )
        {
            warningConsumer.accept( format(
                    DEPRECATED_CONNECTOR_MSG,
                    nonDefaultConnectors.stream()
                            .sorted()
                            .map( s -> format( ">  %s%n", s ) )
                            .collect( joining() ) ) );
        }
    }

    protected boolean isDeprecatedConnectorName( String name )
    {
        return !( name.equalsIgnoreCase( "http" ) || name.equalsIgnoreCase( "https" ) || name
                .equalsIgnoreCase( "bolt" ) );
    }

    @Override
    @Nonnull
    public Map<String,Object> values( @Nonnull Map<String,String> params )
    {
        final HashMap<String,Object> result = new HashMap<>();

        ownedEntries( params ).forEach( s ->
                result.putAll( getSettingFor( s.getKey(), params )
                        .orElseThrow( () -> new InvalidSettingException(
                                format( "Invalid connector setting: %s", s.getKey() ) ) )
                        .values( params ) ) );

        return result;
    }

    /**
     *
     * @return a setting which is not necessarily literally defined in the map provided
     */
    @Nonnull
    protected abstract Optional<Setting<Object>> getSettingFor( @Nonnull String settingName,
            @Nonnull Map<String,String> params );

    @Override
    public List<Setting<Object>> settings( @Nonnull Map<String,String> params )
    {
        return ownedEntries( params )
                .map( e -> getSettingFor( e.getKey(), params ) )
                .filter( Optional::isPresent )
                .map( Optional::get )
                .collect( toList() );
    }

    @Override
    public boolean deprecated()
    {
        return false;
    }

    @Override
    public Optional<String> replacement()
    {
        return Optional.empty();
    }

    @Override
    public boolean internal()
    {
        return false;
    }

    @Override
    public boolean secret()
    {
        return false;
    }

    @Override
    public Optional<String> documentedDefaultValue()
    {
        return Optional.empty();
    }

    @Override
    public String valueDescription()
    {
        return "a group of connector settings";
    }

    @Override
    public Optional<String> description()
    {
        return Optional.empty();
    }

    @Override
    public boolean dynamic()
    {
        return false;
    }
}
