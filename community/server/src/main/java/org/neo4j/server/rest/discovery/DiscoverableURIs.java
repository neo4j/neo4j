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
package org.neo4j.server.rest.discovery;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.server.configuration.ServerSettings;

import static org.neo4j.server.rest.repr.Serializer.joinBaseWithRelativePath;

/**
 * Repository of URIs that the REST API publicly advertises at the root endpoint.
 */
public class DiscoverableURIs
{
    private final Map<String,URITemplate> entries;

    private DiscoverableURIs( Map<String,URITemplate> entries )
    {
        this.entries = entries;
    }

    public void forEach( BiConsumer<String,String> consumer )
    {
        entries.forEach( ( key, value ) -> consumer.accept( key, value.uriString() ) );
    }

    /**
     * Update http/https by adding the scheme, host, port
     * Update bolt if host is not explicitly set.
     */
    public DiscoverableURIs update( URI baseUri )
    {
        entries.forEach( ( key, value ) -> value.update( baseUri ) );
        return this;
    }

    private interface URITemplate
    {
        String uriString();

        void update( URI baseUri );
    }

    private static class RelativePathBasedURITemplate implements URITemplate
    {
        private final String relativePath;
        private String fullPath;

        private RelativePathBasedURITemplate( String relativePath )
        {
            this.relativePath = relativePath;
        }

        @Override
        public String uriString()
        {
            return fullPath == null ? relativePath : fullPath;
        }

        @Override
        public void update( URI baseUri )
        {
            fullPath = joinBaseWithRelativePath( baseUri, relativePath );
        }
    }

    private static class URIBasedURITemplate implements URITemplate
    {
        private URI uri;
        private final boolean isHostOverridable;

        private URIBasedURITemplate( URI uri, boolean isHostOverridable )
        {
            this.uri = uri;
            this.isHostOverridable = isHostOverridable;
        }

        @Override
        public String uriString()
        {
            return uri.toASCIIString();
        }

        @Override
        public void update( URI baseUri )
        {
            if ( isHostOverridable )
            {
                uri = URI.create( String.format( "%s://%s:%s", uri.getScheme(), baseUri.getHost(), uri.getPort() ) );
            }
        }
    }

    public static class Builder
    {
        private Map<String,URITemplate> entries;

        public Builder()
        {
            entries = new HashMap<>();
        }

        /**
         * http and/or https endpoints are always relative.
         * The full path will be completed with users' request base uri with {@link DiscoverableURIs#update} method.
         */
        public Builder addEndpoint( String key, String endpoint )
        {
            var path = new RelativePathBasedURITemplate( endpoint );
            entries.put( key, path );
            return this;
        }

        public Builder addBoltEndpoint( Config config, ConnectorPortRegister portRegister )
        {
            if ( !config.get( BoltConnector.enabled ) )
            {
                // if bolt is not enabled, then no bolt connector entry is discoverable
                return this;
            }

            addBoltEndpoint( "bolt_direct", "bolt", ServerSettings.bolt_discoverable_address, config, portRegister );
            addBoltEndpoint( "bolt_routing", "neo4j", ServerSettings.bolt_routing_discoverable_address, config, portRegister );

            return this;
        }

        public DiscoverableURIs build()
        {
            return new DiscoverableURIs( entries );
        }

        private void addBoltEndpoint( String key, String scheme, Setting<URI> override, Config config, ConnectorPortRegister portRegister )
        {
            URITemplate path;

            if ( config.isExplicitlySet( override ) )
            {
                var uri = config.get( override );
                path = new URIBasedURITemplate( uri, false );
            }
            else
            {
                var address = config.get( BoltConnector.advertised_address );
                var port = address.getPort() == 0 ? portRegister.getLocalAddress( BoltConnector.NAME ).getPort() : address.getPort();
                var host = address.getHostname();

                path = new URIBasedURITemplate( URI.create( String.format( "%s://%s:%s", scheme, host, port ) ), !isBoltHostNameExplicitlySet( config ) );
            }

            entries.put( key, path );
        }

        private boolean isBoltHostNameExplicitlySet( Config config )
        {
            if ( config.isExplicitlySet( GraphDatabaseSettings.default_advertised_address ) )
            {
                return true;
            }
            else if ( config.isExplicitlySet( BoltConnector.advertised_address ) )
            {
                var defaultAddress = config.get( GraphDatabaseSettings.default_advertised_address );
                var boltAddress = config.get( BoltConnector.advertised_address );
                // It is possible that we only set the bolt port without setting the host name
                return !boltAddress.getHostname().equals( defaultAddress.getHostname() );
            }

            return false;
        }
    }

}
