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

import org.neo4j.causalclustering.core.consensus.LeaderLocator;
import org.neo4j.causalclustering.core.consensus.NoLeaderFoundException;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionException;
import org.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;
import org.neo4j.function.ThrowingSupplier;
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

    static CatchupAddressProvider fromSingleAddress( AdvertisedSocketAddress advertisedSocketAddress )
    {
        return new SingleAddressProvider( advertisedSocketAddress );
    }

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

    /**
     * Uses given strategy for both primary and secondary address.
     */
    class UpstreamStrategyBoundAddressProvider implements CatchupAddressProvider
    {
        private final UpstreamStrategyAddressSupplier upstreamStrategyAddressSupplier;

        public UpstreamStrategyBoundAddressProvider( TopologyService topologyService, UpstreamDatabaseStrategySelector strategySelector )
        {
            upstreamStrategyAddressSupplier = new UpstreamStrategyAddressSupplier( strategySelector, topologyService );
        }

        @Override
        public AdvertisedSocketAddress primary() throws CatchupAddressResolutionException
        {
            return upstreamStrategyAddressSupplier.get();
        }

        @Override
        public AdvertisedSocketAddress secondary() throws CatchupAddressResolutionException
        {
            return upstreamStrategyAddressSupplier.get();
        }
    }

    /**
     * Uses leader address as primary and given upstream strategy as secondary address.
     */
    class PrioritisingUpstreamStrategyBasedAddressProvider implements CatchupAddressProvider
    {
        private final LeaderLocator leaderLocator;
        private final TopologyService topologyService;
        private UpstreamStrategyAddressSupplier secondaryUpstreamStrategyAddressSupplier;

        public PrioritisingUpstreamStrategyBasedAddressProvider( LeaderLocator leaderLocator, TopologyService topologyService,
                UpstreamDatabaseStrategySelector strategySelector )
        {
            this.leaderLocator = leaderLocator;
            this.topologyService = topologyService;
            this.secondaryUpstreamStrategyAddressSupplier = new UpstreamStrategyAddressSupplier( strategySelector, topologyService );
        }

        @Override
        public AdvertisedSocketAddress primary() throws CatchupAddressResolutionException
        {
            try
            {
                MemberId leadMember = leaderLocator.getLeader();
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
            return secondaryUpstreamStrategyAddressSupplier.get();
        }
    }

    class UpstreamStrategyAddressSupplier implements ThrowingSupplier<AdvertisedSocketAddress,CatchupAddressResolutionException>
    {
        private final UpstreamDatabaseStrategySelector strategySelector;
        private final TopologyService topologyService;

        private UpstreamStrategyAddressSupplier( UpstreamDatabaseStrategySelector strategySelector, TopologyService topologyService )
        {
            this.strategySelector = strategySelector;
            this.topologyService = topologyService;
        }

        @Override
        public AdvertisedSocketAddress get() throws CatchupAddressResolutionException
        {
            try
            {
                MemberId upstreamMember = strategySelector.bestUpstreamDatabase();
                return topologyService.findCatchupAddress( upstreamMember ).orElseThrow( () -> new CatchupAddressResolutionException( upstreamMember ) );
            }
            catch ( UpstreamDatabaseSelectionException e )
            {
                throw new CatchupAddressResolutionException( e );
            }
        }
    }
}
