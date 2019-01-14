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
package org.neo4j.causalclustering.routing.load_balancing.plugins.server_policies;

import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class AnyGroupFilterTest
{
    @Test
    public void shouldReturnServersMatchingAnyGroup()
    {
        // given
        AnyGroupFilter groupFilter = new AnyGroupFilter( asSet( "china-west", "europe" ) );

        ServerInfo serverA = new ServerInfo( new AdvertisedSocketAddress( "bolt", 1 ),
                new MemberId( UUID.randomUUID() ), asSet( "china-west" ) );
        ServerInfo serverB = new ServerInfo( new AdvertisedSocketAddress( "bolt", 2 ),
                        new MemberId( UUID.randomUUID() ), asSet( "europe" ) );
        ServerInfo serverC = new ServerInfo( new AdvertisedSocketAddress( "bolt", 3 ),
                new MemberId( UUID.randomUUID() ), asSet( "china", "china-west" ) );
        ServerInfo serverD = new ServerInfo( new AdvertisedSocketAddress( "bolt", 4 ),
                new MemberId( UUID.randomUUID() ), asSet( "china-west", "china" ) );
        ServerInfo serverE = new ServerInfo( new AdvertisedSocketAddress( "bolt", 5 ),
                new MemberId( UUID.randomUUID() ), asSet( "china-east", "asia" ) );
        ServerInfo serverF = new ServerInfo( new AdvertisedSocketAddress( "bolt", 6 ),
                new MemberId( UUID.randomUUID() ), asSet( "europe-west" ) );
        ServerInfo serverG = new ServerInfo( new AdvertisedSocketAddress( "bolt", 7 ),
                new MemberId( UUID.randomUUID() ), asSet( "china-west", "europe" ) );
        ServerInfo serverH = new ServerInfo( new AdvertisedSocketAddress( "bolt", 8 ),
                new MemberId( UUID.randomUUID() ), asSet( "africa" ) );

        Set<ServerInfo> data = asSet( serverA, serverB, serverC, serverD, serverE, serverF, serverG, serverH );

        // when
        Set<ServerInfo> output = groupFilter.apply( data );

        // then
        Set<Integer> ports = new HashSet<>();
        for ( ServerInfo info : output )
        {
            ports.add( info.boltAddress().getPort() );
        }

        assertEquals( asSet( 1, 2, 3, 4, 7 ), ports );
    }
}
