/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.server.edge;

import java.util.Iterator;
import java.util.Random;

import org.neo4j.coreedge.discovery.ClusterTopology;
import org.neo4j.coreedge.discovery.EdgeDiscoveryService;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.CoreMember;

public class ConnectToRandomCoreServer implements EdgeToCoreConnectionStrategy
{
    private final EdgeDiscoveryService discoveryService;

    public ConnectToRandomCoreServer( EdgeDiscoveryService discoveryService)
    {
        this.discoveryService = discoveryService;
    }


    @Override
    public AdvertisedSocketAddress coreServer()
    {
        final Random random = new Random();

        final ClusterTopology clusterTopology = discoveryService.currentTopology();
        int randomSize = random.nextInt( clusterTopology.getMembers().size() );

        final Iterator<CoreMember> iterator = clusterTopology.getMembers().iterator();
        AdvertisedSocketAddress result = null;
        for ( int i = 0; i <= randomSize; i++ )
        {
            result = iterator.next().getCoreAddress();
        }
        return result;
    }
}
