/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.catchup;

import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionException;
import org.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.helpers.AdvertisedSocketAddress;

public class UpstreamStrategyAddressSupplier implements ThrowingSupplier<AdvertisedSocketAddress,CatchupAddressResolutionException>
{
    private final UpstreamDatabaseStrategySelector strategySelector;
    private final TopologyService topologyService;

    UpstreamStrategyAddressSupplier( UpstreamDatabaseStrategySelector strategySelector, TopologyService topologyService )
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
