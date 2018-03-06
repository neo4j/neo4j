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

import java.util.function.Supplier;

import org.neo4j.causalclustering.core.state.snapshot.IdentityMetaData;
import org.neo4j.causalclustering.core.state.snapshot.TopologyLookupException;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.readreplica.UpstreamDatabaseSelectionException;
import org.neo4j.causalclustering.readreplica.UpstreamDatabaseStrategySelector;
import org.neo4j.helpers.AdvertisedSocketAddress;

@FunctionalInterface
public interface CatchupAddressProvider extends Supplier<IdentityMetaData>
{
    class TopologyCatchupAddressProvider implements CatchupAddressProvider
    {
        UpstreamDatabaseStrategySelector upstreamDatabaseStrategySelector;
        TopologyService topologyService;

        public TopologyCatchupAddressProvider( UpstreamDatabaseStrategySelector upstreamDatabaseStrategySelector )
        {
            this.upstreamDatabaseStrategySelector = upstreamDatabaseStrategySelector;
        }

        @Override
        public IdentityMetaData get()
        {
            try
            {
                MemberId who = upstreamDatabaseStrategySelector.bestUpstreamDatabase();
                AdvertisedSocketAddress address =
                        topologyService.findCatchupAddress( who ).orElseThrow( () -> new RuntimeException( new TopologyLookupException( who ) ) );
                ClusterId clusterId = topologyService.coreServers().clusterId();
                return new IdentityMetaData( address, clusterId, null, who, null );
            }
            catch ( UpstreamDatabaseSelectionException e )
            {
                throw new RuntimeException( e );
            }
        }
    }
}
