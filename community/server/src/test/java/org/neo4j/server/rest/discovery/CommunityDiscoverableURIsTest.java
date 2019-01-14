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

import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConnectorPortRegister;
import org.neo4j.server.configuration.ServerSettings;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.server.rest.discovery.CommunityDiscoverableURIs.communityDiscoverableURIs;

public class CommunityDiscoverableURIsTest
{
    @Test
    public void shouldAdvertiseDataAndManagementURIs() throws Exception
    {
        DiscoverableURIs uris = communityDiscoverableURIs( Config.defaults(), null );

        assertEquals( map( "data", "/db/data/", "management", "/db/manage/" ), toMap(uris) );
    }

    @Test
    public void shouldAdvertiseBoltIfExplicitlyConfigured() throws Exception
    {
        DiscoverableURIs uris = communityDiscoverableURIs(
                Config.defaults( ServerSettings.bolt_discoverable_address, "bolt://banana.com:1234" ), null );

        assertEquals( "bolt://banana.com:1234", toMap(uris).get("bolt") );
    }

    @Test
    public void shouldLookupBoltPortInRegisterIfConfiguredTo0() throws Exception
    {
        BoltConnector bolt = new BoltConnector( "honestJakesBoltConnector" );
        ConnectorPortRegister register = new ConnectorPortRegister();
        register.register( bolt.key(), new InetSocketAddress( 1337 ) );

        DiscoverableURIs uris = communityDiscoverableURIs(
                Config.builder()
                        .withSetting( bolt.advertised_address, "apple.com:0" )
                        .withSetting( bolt.enabled, "true" )
                        .withSetting( bolt.type, BoltConnector.ConnectorType.BOLT.name() )
                        .build(), register );

        assertEquals( "bolt://apple.com:1337", toMap(uris).get("bolt")  );
    }

    @Test
    public void shouldOmitBoltIfNoConnectorConfigured() throws Exception
    {
        DiscoverableURIs uris = communityDiscoverableURIs( Config.builder().build(), null );

        assertFalse( toMap( uris ).containsKey( "bolt" ) );
    }

    private Map<String,Object> toMap( DiscoverableURIs uris )
    {
        Map<String,Object> out = new HashMap<>();
        uris.forEach( ( k, v ) -> out.put( k, v.toASCIIString() ) );
        return out;
    }
}
