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
import org.neo4j.coreedge.discovery.CoreServerSelectionException;
import org.neo4j.coreedge.discovery.EdgeTopologyService;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.server.AdvertisedSocketAddress;

public class ConnectToRandomCoreServer implements CoreServerSelectionStrategy
{
    private final EdgeTopologyService discoveryService;
    private final Random random = new Random();

    public ConnectToRandomCoreServer( EdgeTopologyService discoveryService )
    {
        this.discoveryService = discoveryService;
    }

    @Override
    public AdvertisedSocketAddress coreServer() throws CoreServerSelectionException
    {
        final ClusterTopology clusterTopology = discoveryService.currentTopology();

        if ( clusterTopology.coreMembers().size() == 0 )
        {
            throw new CoreServerSelectionException( "No core servers available" );
        }

        int skippedServers = random.nextInt( clusterTopology.coreMembers().size() );

        final Iterator<CoreMember> iterator = clusterTopology.coreMembers().iterator();

        CoreMember member;
        do
        {
            member = iterator.next();
        }
        while ( skippedServers-- > 0 );

        return member.getCoreAddress();
    }
}
