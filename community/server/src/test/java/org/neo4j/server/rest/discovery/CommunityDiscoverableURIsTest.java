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

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.server.configuration.ServerSettings;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.server.rest.discovery.CommunityDiscoverableURIs.communityDiscoverableURIs;

class CommunityDiscoverableURIsTest
{
    @Test
    void shouldAdvertiseTransactionAndManagementURIs()
    {
        var uris = communityDiscoverableURIs( Config.defaults(), null );
        assertEquals( map( "transaction", "/db/{databaseName}/tx" ), toMap( uris ) );
    }

    @Test
    void shouldAdvertiseBoltIfExplicitlyConfigured()
    {
        var uris = communityDiscoverableURIs(
                Config.newBuilder()
                        .set( BoltConnector.enabled, true )
                        .set( ServerSettings.bolt_discoverable_address, URI.create( "bolt://banana.com:1234" ) )
                        .build(), null );

        var map = toMap( uris );
        assertEquals( "bolt://banana.com:1234", map.get( "bolt_direct" ) );
        assertEquals( "neo4j://localhost:7687", map.get( "bolt_routing" ) );
    }

    @Test
    void shouldLookupBoltPortInRegisterIfConfigured()
    {
        var register = new ConnectorPortRegister();
        register.register( BoltConnector.NAME, new InetSocketAddress( 1337 ) );

        var uris = communityDiscoverableURIs(
                Config.newBuilder()
                        .set( BoltConnector.advertised_address, new SocketAddress( "apple.com", 0 ) )
                        .set( BoltConnector.enabled, true )
                        .build(), register );

        var map = toMap( uris );
        assertEquals( "bolt://apple.com:1337", map.get( "bolt_direct" ) );
        assertEquals( "neo4j://apple.com:1337", map.get( "bolt_routing" ) );
    }

    private Map<String,String> toMap( DiscoverableURIs uris )
    {
        Map<String,String> out = new HashMap<>();
        uris.forEach( out::put );
        return out;
    }
}
