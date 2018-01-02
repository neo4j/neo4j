/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.discovery;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.helpers.SocketAddressParser;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.HttpConnector.Encryption;

import static org.neo4j.causalclustering.discovery.ClientConnectorAddresses.Scheme.bolt;
import static org.neo4j.causalclustering.discovery.ClientConnectorAddresses.Scheme.http;
import static org.neo4j.causalclustering.discovery.ClientConnectorAddresses.Scheme.https;

public class ClientConnectorAddresses implements Iterable<ClientConnectorAddresses.ConnectorUri>
{
    private final List<ConnectorUri> connectorUris;

    public ClientConnectorAddresses( List<ConnectorUri> connectorUris )
    {
        this.connectorUris = connectorUris;
    }

    static ClientConnectorAddresses extractFromConfig( Config config )
    {
        List<ConnectorUri> connectorUris = new ArrayList<>();

        List<BoltConnector> boltConnectors = config.enabledBoltConnectors();

        if ( boltConnectors.isEmpty() )
        {
            throw new IllegalArgumentException( "A Bolt connector must be configured to run a cluster" );
        }

        boltConnectors
                .forEach( c -> connectorUris.add( new ConnectorUri( bolt, config.get( c.advertised_address ) ) ) );

        config.enabledHttpConnectors()
                .forEach( c -> connectorUris.add( new ConnectorUri( Encryption.NONE.equals(c.encryptionLevel() ) ?
                        http : https, config.get( c.advertised_address ) ) ) );

        return new ClientConnectorAddresses( connectorUris );
    }

    public AdvertisedSocketAddress boltAddress()
    {
        return connectorUris.stream().filter( connectorUri -> connectorUri.scheme == bolt ).findFirst().orElseThrow(
                () -> new IllegalArgumentException( "A Bolt connector must be configured to run a cluster" ) )
                .socketAddress;
    }

    public List<URI> uriList()
    {
        return connectorUris.stream().map( ConnectorUri::toUri ).collect( Collectors.toList() );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        ClientConnectorAddresses that = (ClientConnectorAddresses) o;
        return Objects.equals( connectorUris, that.connectorUris );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( connectorUris );
    }

    @Override
    public String toString()
    {
        return connectorUris.stream().map( ConnectorUri::toString ).collect( Collectors.joining( "," ) );
    }

    static ClientConnectorAddresses fromString( String value )
    {
        return new ClientConnectorAddresses( Stream.of( value.split( "," ) )
                .map( ConnectorUri::fromString ).collect( Collectors.toList() ) );
    }

    @Override
    public Iterator<ConnectorUri> iterator()
    {
        return connectorUris.iterator();
    }

    public enum Scheme
    {
        bolt, http, https
    }

    public static class ConnectorUri
    {
        private final Scheme scheme;
        private final AdvertisedSocketAddress socketAddress;

        public ConnectorUri( Scheme scheme, AdvertisedSocketAddress socketAddress )
        {
            this.scheme = scheme;
            this.socketAddress = socketAddress;
        }

        private URI toUri()
        {
            try
            {
                return new URI( scheme.name().toLowerCase(), null, socketAddress.getHostname(), socketAddress.getPort(),
                        null, null, null );
            }
            catch ( URISyntaxException e )
            {
                throw new IllegalArgumentException( e );
            }
        }

        @Override
        public String toString()
        {
            return toUri().toString();
        }

        private static ConnectorUri fromString( String string )
        {
            URI uri = URI.create( string );
            AdvertisedSocketAddress advertisedSocketAddress = SocketAddressParser.socketAddress( uri.getAuthority(), AdvertisedSocketAddress::new );
            return new ConnectorUri( Scheme.valueOf( uri.getScheme() ), advertisedSocketAddress );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            ConnectorUri that = (ConnectorUri) o;
            return scheme == that.scheme &&
                    Objects.equals( socketAddress, that.socketAddress );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( scheme, socketAddress );
        }
    }
}
