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
package org.neo4j.coreedge.raft.net;

import java.util.Collection;

import org.neo4j.coreedge.discovery.ClusterTopology;
import org.neo4j.coreedge.discovery.CoreAddresses;
import org.neo4j.coreedge.discovery.CoreTopologyService;
import org.neo4j.coreedge.network.Message;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.CoreMember;

public class CoreOutbound implements Outbound<CoreMember, Message>,CoreTopologyService.Listener
{
    private final CoreTopologyService discoveryService;
    private final Outbound<AdvertisedSocketAddress, Message> outbound;
    private ClusterTopology clusterTopology;

    public CoreOutbound( CoreTopologyService discoveryService, Outbound<AdvertisedSocketAddress, Message> outbound )
    {
        this.discoveryService = discoveryService;
        this.outbound = outbound;
        this.discoveryService.addMembershipListener( this );
        clusterTopology = discoveryService.currentTopology();
    }

    @Override
    public void send( CoreMember to, Message message )
    {
        CoreAddresses coreAddresses = clusterTopology.coreAddresses( to );
        if ( coreAddresses != null )
        {
            outbound.send( coreAddresses.getCoreServer(), message );
        }
        // Drop messages for servers that are missing from the cluster topology;
        // discovery service thinks that they are offline, so it's not worth trying to send them anything.
    }

    @Override
    public void send( CoreMember to, Collection<Message> messages )
    {
        CoreAddresses coreAddresses = clusterTopology.coreAddresses( to );
        if ( coreAddresses != null )
        {
            outbound.send( coreAddresses.getCoreServer(), messages );
        }
        // Drop messages for servers that are missing from the cluster topology;
        // discovery service thinks that they are offline, so it's not worth trying to send them anything.
    }

    @Override
    public void onTopologyChange()
    {
        clusterTopology = discoveryService.currentTopology();
    }
}
