/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.discovery;

import java.util.Optional;
import java.util.function.Function;

import org.neo4j.causalclustering.core.state.snapshot.TopologyLookupException;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;

public class MultiRetryTopologyServiceStrategy implements TopologyServiceRetryStrategy
{
    private final int delayInMillis;
    private final int retries;

    public MultiRetryTopologyServiceStrategy( int delayInMillis, int retries )
    {
        this.delayInMillis = delayInMillis;
        this.retries = retries;
    }

    @Override
    public AdvertisedSocketAddress findCatchupAddress( MemberId memberId,
            Function<MemberId,Optional<AdvertisedSocketAddress>> topologyServiceFindCatchupAddress )
    {
        Optional<AdvertisedSocketAddress> advertisedSocketAddress = topologyServiceFindCatchupAddress.apply( memberId );
        int currentIteration = 0;
        while ( !advertisedSocketAddress.isPresent() && currentIteration < retries )
        {
            try
            {
                Thread.sleep( delayInMillis );
            }
            catch ( InterruptedException e )
            {
                throw new RuntimeException( e );
            }
            advertisedSocketAddress = topologyServiceFindCatchupAddress.apply( memberId );
        }
        return advertisedSocketAddress.orElseThrow( () -> new TopologyLookupException( memberId ) );
    }
}
