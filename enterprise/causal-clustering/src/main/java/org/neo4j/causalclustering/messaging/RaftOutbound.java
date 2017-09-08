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
package org.neo4j.causalclustering.messaging;

import java.util.Optional;
import java.util.function.Supplier;

import org.neo4j.causalclustering.core.consensus.RaftMessages.ClusterIdAwareMessage;
import org.neo4j.causalclustering.core.consensus.RaftMessages.RaftMessage;
import org.neo4j.causalclustering.discovery.CoreServerInfo;
import org.neo4j.causalclustering.discovery.CoreTopologyService;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.address.UnknownAddressMonitor;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.time.Clocks;

public class RaftOutbound implements Outbound<MemberId, RaftMessage>
{
    private final CoreTopologyService coreTopologyService;
    private final Outbound<AdvertisedSocketAddress,Message> outbound;
    private final Supplier<Optional<ClusterId>> clusterIdentity;
    private final UnknownAddressMonitor unknownAddressMonitor;
    private final Log log;

    public RaftOutbound( CoreTopologyService coreTopologyService, Outbound<AdvertisedSocketAddress,Message> outbound,
                         Supplier<Optional<ClusterId>> clusterIdentity, LogProvider logProvider, long logThresholdMillis )
    {
        this.coreTopologyService = coreTopologyService;
        this.outbound = outbound;
        this.clusterIdentity = clusterIdentity;
        this.log = logProvider.getLog( getClass() );
        this.unknownAddressMonitor = new UnknownAddressMonitor( log, Clocks.systemClock(), logThresholdMillis );
    }

    @Override
    public void send( MemberId to, RaftMessage message, boolean block )
    {
        Optional<ClusterId> clusterId = clusterIdentity.get();
        if ( !clusterId.isPresent() )
        {
            log.warn( "Attempting to send a message before bound to a cluster" );
            return;
        }

        Optional<CoreServerInfo> coreServerInfo = coreTopologyService.coreServers().find( to );
        if ( coreServerInfo.isPresent() )
        {
            outbound.send( coreServerInfo.get().getRaftServer(), new ClusterIdAwareMessage( clusterId.get(), message ), block );
        }
        else
        {
            unknownAddressMonitor.logAttemptToSendToMemberWithNoKnownAddress( to );
        }
    }
}
