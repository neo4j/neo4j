/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.discovery;

import org.junit.Test;

import org.neo4j.causalclustering.discovery.ClientConnectorAddresses.ConnectorUri;
import org.neo4j.helpers.AdvertisedSocketAddress;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.neo4j.causalclustering.discovery.ClientConnectorAddresses.Scheme.bolt;
import static org.neo4j.causalclustering.discovery.ClientConnectorAddresses.Scheme.http;
import static org.neo4j.causalclustering.discovery.ClientConnectorAddresses.Scheme.https;

public class ClientConnectorAddressesTest
{
    @Test
    public void shouldSerializeToString()
    {
        // given
        ClientConnectorAddresses connectorAddresses = new ClientConnectorAddresses( asList(
                new ConnectorUri( bolt, new AdvertisedSocketAddress( "host", 1 ) ),
                new ConnectorUri( http, new AdvertisedSocketAddress( "host", 2 ) ),
                new ConnectorUri( https, new AdvertisedSocketAddress( "host", 3 ) ),
                new ConnectorUri( bolt, new AdvertisedSocketAddress( "::1", 4 ) ),
                new ConnectorUri( http, new AdvertisedSocketAddress( "::", 5 ) ),
                new ConnectorUri( https, new AdvertisedSocketAddress( "fe80:1:2::3", 6 ) ) )
        );

        String expectedString = "bolt://host:1,http://host:2,https://host:3,bolt://[::1]:4,http://[::]:5,https://[fe80:1:2::3]:6";

        // when
        String connectorAddressesString = connectorAddresses.toString();

        // then
        assertEquals( expectedString, connectorAddressesString );

        // when
        ClientConnectorAddresses out = ClientConnectorAddresses.fromString( connectorAddressesString );

        // then
        assertEquals( connectorAddresses, out );
    }

    @Test
    public void shouldSerializeWithNoHttpsAddress()
    {
        // given
        ClientConnectorAddresses connectorAddresses = new ClientConnectorAddresses( asList(
                new ConnectorUri( bolt, new AdvertisedSocketAddress( "host", 1 ) ),
                new ConnectorUri( http, new AdvertisedSocketAddress( "host", 2 ) )
        ) );

        // when
        ClientConnectorAddresses out = ClientConnectorAddresses.fromString( connectorAddresses.toString() );

        // then
        assertEquals( connectorAddresses, out );
    }
}
