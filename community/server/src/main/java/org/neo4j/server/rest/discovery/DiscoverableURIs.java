/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.server.rest.discovery;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConnectorPortRegister;

import static org.neo4j.server.rest.discovery.DiscoverableURIs.Precedence.HIGH;
import static org.neo4j.server.rest.discovery.DiscoverableURIs.Precedence.HIGHEST;
import static org.neo4j.server.rest.discovery.DiscoverableURIs.Precedence.LOW;
import static org.neo4j.server.rest.discovery.DiscoverableURIs.Precedence.LOWEST;

/**
 * Repository of URIs that the REST API publicly advertises at the root endpoint.
 */
public class DiscoverableURIs
{
    public enum Precedence
    {
        LOWEST,
        LOW,
        NORMAL,
        HIGH,
        HIGHEST
    }

    private final Collection<URIEntry> entries;

    private DiscoverableURIs( Collection<URIEntry> entries )
    {
        this.entries = entries;
    }

    public void forEach( BiConsumer<String,URI> consumer )
    {
        entries.stream().collect( Collectors.groupingBy( e -> e.key ) )
                .forEach( ( key, list ) -> list.stream()
                        .max( Comparator.comparing( e -> e.precedence ) )
                        .ifPresent( e -> consumer.accept( key, e.uri ) ) );
    }

    private static class URIEntry
    {
        private String key;
        private Precedence precedence;
        private URI uri;

        private URIEntry( String key, URI uri, Precedence precedence )
        {
            this.key = key;
            this.uri = uri;
            this.precedence = precedence;
        }
    }

    public static class Builder
    {
        private final Collection<URIEntry> entries;

        public Builder()
        {
            entries = new ArrayList<>();
        }

        public Builder( DiscoverableURIs copy )
        {
            entries = new ArrayList<>( copy.entries );
        }

        public Builder add( String key, URI uri, Precedence precedence )
        {
            if ( entries.stream().anyMatch( e -> e.key.equals( key ) && e.precedence == precedence ) )
            {
                throw new InvalidSettingException(
                        String.format( "Unable to add two entries with the same precedence using key '%s' and precedence '%s'", key, precedence ) );
            }

            entries.add( new URIEntry( key, uri, precedence ) );
            return this;
        }

        public Builder add( String key, String uri, Precedence precedence )
        {
            try
            {
                return add( key, new URI( uri ), precedence );
            }
            catch ( URISyntaxException e )
            {
                throw new InvalidSettingException( String.format( "Unable to construct bolt discoverable URI using '%s' as uri: " + "%s", uri, e.getMessage() ),
                        e );
            }
        }

        public Builder add( String key, String scheme, String hostname, int port, Precedence precedence )
        {
            try
            {
                return add( key, new URI( scheme, null, hostname, port, null, null, null ), precedence );
            }
            catch ( URISyntaxException e )
            {
                throw new InvalidSettingException(
                        String.format( "Unable to construct bolt discoverable URI using '%s' as hostname: " + "%s", hostname, e.getMessage() ), e );
            }
        }

        public Builder addBoltConnectorFromConfig( String key, String scheme, Config config, Setting<URI> override, ConnectorPortRegister portRegister )
        {
            // If an override is configured, add it with the HIGHEST precedence
            if ( config.isConfigured( override ) )
            {
                add( key, config.get( override ), HIGHEST );
            }

            config.enabledBoltConnectors().stream().findFirst().ifPresent( c ->
            {
                AdvertisedSocketAddress address = config.get( c.advertised_address );
                int port = address.getPort();
                if ( port == 0 )
                {
                    port = portRegister.getLocalAddress( c.key() ).getPort();
                }

                // If advertised address is explicitly set, set the precedence to HIGH - eitherwise set it as LOWEST (default)
                add( key, scheme, address.getHostname(), port, config.isConfigured( c.advertised_address ) ? HIGH : LOWEST );
            } );

            return this;
        }

        public Builder overrideAbsolutesFromRequest( URI requestUri )
        {
            // Find all default entries with absolute URIs and replace the corresponding host name entries with the one from the request uri.
            List<URIEntry> defaultEntries = entries.stream().filter( e -> e.uri.isAbsolute() && e.precedence == LOWEST ).collect( Collectors.toList() );

            for ( URIEntry entry : defaultEntries )
            {
                add( entry.key, entry.uri.getScheme(), requestUri.getHost(), entry.uri.getPort(), LOW );
            }

            return this;
        }

        public DiscoverableURIs build()
        {
            return new DiscoverableURIs( entries );
        }
    }

}
