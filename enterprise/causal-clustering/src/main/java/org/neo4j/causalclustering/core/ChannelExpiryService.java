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
package org.neo4j.causalclustering.core;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.causalclustering.discovery.CoreServerInfo;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.CoreTopologyService;
import org.neo4j.causalclustering.discovery.Difference;
import org.neo4j.causalclustering.discovery.TopologyDifference;
import org.neo4j.causalclustering.messaging.ChannelExpiryListener;

class ChannelExpiryService implements CoreTopologyService.Listener
{
    private final List<ChannelExpiryListener> listeners = new ArrayList<>();

    void addChannelExpiryListener( ChannelExpiryListener listener )
    {
        listeners.add( listener );
    }

    @Override
    public void onCoreTopologyChange( CoreTopology oldCoreTopology, CoreTopology newCoreTopology )
    {
        TopologyDifference<Difference<CoreServerInfo>> difference = newCoreTopology.difference( oldCoreTopology );

        for ( Difference<CoreServerInfo> coreServerInfo : difference.removed() )
        {
            for ( ChannelExpiryListener listener : listeners )
            {
                listener.onChannelExpiry( coreServerInfo.getServer().getRaftServer() );
            }
        }
    }
}
