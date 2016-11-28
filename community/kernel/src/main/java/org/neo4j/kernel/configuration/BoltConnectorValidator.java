/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.ListenSocketAddress;

import static org.neo4j.kernel.configuration.BoltConnector.EncryptionLevel.OPTIONAL;
import static org.neo4j.kernel.configuration.Connector.ConnectorType.BOLT;
import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.NO_DEFAULT;
import static org.neo4j.kernel.configuration.Settings.advertisedAddress;
import static org.neo4j.kernel.configuration.Settings.listenAddress;
import static org.neo4j.kernel.configuration.Settings.options;
import static org.neo4j.kernel.configuration.Settings.setting;

public class BoltConnectorValidator extends ConnectorValidator
{
    public BoltConnectorValidator()
    {
        super( BOLT );
    }

    @Override
    @Nonnull
    public Map<String,String> validate( @Nonnull Map<String,String> rawConfig )
            throws InvalidSettingException
    {
        final HashMap<String,String> result = new HashMap<>();

        ownedEntries( rawConfig ).forEach( s ->
        {
            // owns has already verified that 'type' is correct and that this split is possible
            final String subsetting = s.getKey().split( "\\." )[3];

            Setting<ListenSocketAddress> las = listenAddress( s.getKey(), 7687 );
            switch ( subsetting )
            {
            case "enabled":
                result.putAll( setting( s.getKey(), BOOLEAN, "false" ).validate( rawConfig ) );
                break;
            case "tls_level":
                result.putAll( setting( s.getKey(), options( BoltConnector.EncryptionLevel.class ),
                        OPTIONAL.name() ).validate( rawConfig ) );
                break;
            case "address":
            case "listen_address":
                // Exact default port doesn't matter for validation purposes
                result.putAll( las.validate( rawConfig ) );
                break;
            case "advertised_address":
                result.putAll( advertisedAddress( s.getKey(), las ).validate( rawConfig ) );
                break;
            case "type":
                result.putAll( assertTypeIsBolt(
                        setting( s.getKey(), options( Connector.ConnectorType.class ),
                                NO_DEFAULT ), rawConfig ) );
                break;
            default:
                throw new InvalidSettingException( String.format( "Unknown configuration passed to %s: %s",
                        getClass().getName(), s.getKey() ) );
            }
        } );

        return result;
    }

    @Nonnull
    private Map<String,String> assertTypeIsBolt( @Nonnull Setting<?> setting, @Nonnull Map<String,String> rawConfig )
    {
        Map<String,String> result = setting.validate( rawConfig );

        Optional<?> typeValue = Optional.ofNullable( setting.apply( rawConfig::get ) );

        if ( typeValue.isPresent() && !BOLT.equals( typeValue.get() ) )
        {
            throw new InvalidSettingException(
                    String.format( "'%s' is only allowed to be '%s'; not '%s'",
                            setting.name(), BOLT, typeValue.get() ) );

        }
        return result;
    }

    @Override
    @Nonnull
    public List<String> subSettings()
    {
        return Arrays.asList( "type", "enabled", "tls_level", "address", "listen_address", "advertised_address" );
    }

    @Override
    @Nonnull
    public Map<String,Object> values( @Nonnull Map<String,String> params )
    {
        final HashMap<String,Object> result = new HashMap<>();

        ownedEntries( params ).forEach( s ->
        {
            // owns has already verified that 'type' is correct and that this split is possible
            String[] parts = s.getKey().split( "\\." );
            final String subsetting = parts[3];

            Setting<ListenSocketAddress> las = listenAddress( s.getKey(), 7687 );
            switch ( subsetting )
            {
            case "enabled":
                result.putAll( setting( s.getKey(), BOOLEAN, "false" ).values( params ) );
                break;
            case "tls_level":
                result.putAll( setting( s.getKey(), options( BoltConnector.EncryptionLevel.class ),
                        OPTIONAL.name() ).values( params ) );
                break;
            case "address":
            case "listen_address":
                result.putAll( las.values( params ) );
                break;
            case "advertised_address":
                result.putAll( advertisedAddress( s.getKey(), las ).values( params ) );
                break;
            case "type":
                result.putAll( setting( s.getKey(), options( Connector.ConnectorType.class ), NO_DEFAULT )
                        .values( params ) );
                break;
            default:
                throw new InvalidSettingException( String.format( "Unknown configuration passed to %s: %s",
                        getClass().getName(), s.getKey() ) );
            }
        } );

        return result;
    }
}
