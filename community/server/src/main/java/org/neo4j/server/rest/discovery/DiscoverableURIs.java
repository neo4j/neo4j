/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import java.util.Optional;
import java.util.function.BiConsumer;

import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConnectorPortRegister;

/**
 * Repository of URIs that the REST API publicly advertises at the root endpoint.
 */
public class DiscoverableURIs
{
    private final Collection<Pair<String,String>> relativeUris = new ArrayList<>();
    private final Collection<Pair<String,URI>> absoluteUris = new ArrayList<>();

    public DiscoverableURIs addRelative( String key, String uri )
    {
        relativeUris.add( Pair.pair( key, uri ) );
        return this;
    }

    public DiscoverableURIs addAbsolute( String key, URI uri )
    {
        absoluteUris.add( Pair.pair( key, uri ) );
        return this;
    }

    public static Optional<URI> discoverableBoltUri( String scheme, Config config, Setting<URI> override,
            ConnectorPortRegister connectorPortRegister )
    {
        // Note that this whole function would be much cleaner to implement as a default function for the
        // bolt_discoverable_address setting; however the current config design makes it hard to do
        // "find any bolt connector", it can only do "find exactly this config key".. Something to refactor
        // when we refactor config API for 4.0.
        if ( config.isConfigured( override ) )
        {
            return Optional.ofNullable( config.get( override ) );
        }

        return config.enabledBoltConnectors().stream().findFirst().map( c ->
        {
            AdvertisedSocketAddress advertisedSocketAddress = config.get( c.advertised_address );

            // If port is 0 it's been assigned a random port from the OS, list this instead
            if ( advertisedSocketAddress.getPort() == 0 )
            {
                int boltPort = connectorPortRegister.getLocalAddress( c.key() ).getPort();
                return boltURI( scheme, advertisedSocketAddress.getHostname(), boltPort );
            }

            // Use the config verbatim since it seems sane
            return boltURI( scheme, advertisedSocketAddress.getHostname(), advertisedSocketAddress.getPort() );
        } );
    }

    public void forEachRelativeUri( BiConsumer<String,String> consumer )
    {
        relativeUris.forEach( p -> consumer.accept( p.first(), p.other() ) );
    }

    public void forEachAbsoluteUri( BiConsumer<String,URI> consumer )
    {
        absoluteUris.forEach( p -> consumer.accept( p.first(), p.other() ) );
    }

    private static URI boltURI( String scheme, String host, int port )
    {
        try
        {
            return new URI( scheme, null, host, port, null, null, null );
        }
        catch ( URISyntaxException e )
        {
            throw new InvalidSettingException(
                    String.format( "Unable to construct bolt discoverable URI using '%s' as hostname: " + "%s", host,
                            e.getMessage() ), e );
        }
    }
}
