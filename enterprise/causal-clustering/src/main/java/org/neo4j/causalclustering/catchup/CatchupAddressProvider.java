/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.catchup;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.causalclustering.core.consensus.NoLeaderFoundException;
import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;

/**
 * Address provider for catchup client.
 */
public interface CatchupAddressProvider
{
    /**
     * @return The address to the primary location where up to date requests are required. For a cluster aware provider the obvious choice would be the
     * leader address.
     * @throws CatchupAddressResolutionException if the provider was unable to find an address to this location.
     */
    AdvertisedSocketAddress primary() throws CatchupAddressResolutionException;

    /**
     * @return The address to a secondary location that are not required to be up to date. If there are multiple secondary locations it is recommended to
     * do some simple load balancing for returned addresses. This is to avoid re-sending failed requests to the same instance immediately.
     * @throws CatchupAddressResolutionException if the provider was unable to find an address to this location.
     */
    AdvertisedSocketAddress secondary() throws CatchupAddressResolutionException;

    class SingleAddressProvider implements CatchupAddressProvider
    {
        private final AdvertisedSocketAddress socketAddress;

        public SingleAddressProvider( AdvertisedSocketAddress socketAddress )
        {
            this.socketAddress = socketAddress;
        }

        @Override
        public AdvertisedSocketAddress primary()
        {
            return socketAddress;
        }

        @Override
        public AdvertisedSocketAddress secondary()
        {
            return socketAddress;
        }
    }

    class TopologyBasedAddressProvider implements CatchupAddressProvider
    {
        private final RaftMachine raftMachine;
        private final TopologyService topologyService;

        public TopologyBasedAddressProvider( RaftMachine raftMachine, TopologyService topologyService )
        {
            this.raftMachine = raftMachine;
            this.topologyService = topologyService;
        }

        @Override
        public AdvertisedSocketAddress primary() throws CatchupAddressResolutionException
        {
            try
            {
                MemberId leadMember = raftMachine.getLeader();
                return topologyService.findCatchupAddress( leadMember ).orElseThrow( () -> new CatchupAddressResolutionException( leadMember ) );
            }
            catch ( NoLeaderFoundException e )
            {
                throw new CatchupAddressResolutionException( e );
            }
        }

        @Override
        public AdvertisedSocketAddress secondary() throws CatchupAddressResolutionException
        {
            List<MemberId> potentialCoresAndReplicas = new ArrayList<>( raftMachine.votingMembers() );
            potentialCoresAndReplicas.addAll( raftMachine.replicationMembers() );
            int accountForRoundingDown = 1;
            MemberId randomlySelectedCatchupServer =
                    potentialCoresAndReplicas.get( (int) (Math.random() * potentialCoresAndReplicas.size() + accountForRoundingDown) );
            return topologyService.findCatchupAddress( randomlySelectedCatchupServer ).orElseThrow(
                    () -> new CatchupAddressResolutionException( randomlySelectedCatchupServer ) );
        }
    }

    static CatchupAddressProvider fromSingleAddress( AdvertisedSocketAddress advertisedSocketAddress )
    {
        return new SingleAddressProvider( advertisedSocketAddress );
    }
}
