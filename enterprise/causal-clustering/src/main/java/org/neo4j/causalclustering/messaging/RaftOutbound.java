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
package org.neo4j.causalclustering.messaging;

import java.util.Optional;

import org.neo4j.causalclustering.core.consensus.RaftMessages.ClusterIdAwareMessage;
import org.neo4j.causalclustering.core.consensus.RaftMessages.RaftMessage;
import org.neo4j.causalclustering.discovery.CoreAddresses;
import org.neo4j.causalclustering.discovery.CoreTopologyService;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.ClusterIdentity;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.address.UnknownAddressMonitor;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.time.Clocks;

public class RaftOutbound implements Outbound<MemberId, RaftMessage>
{
    private final CoreTopologyService discoveryService;
    private final Outbound<AdvertisedSocketAddress,Message> outbound;
    private final ClusterIdentity clusterIdentity;
    private final UnknownAddressMonitor unknownAddressMonitor;
    private final Log log;

    public RaftOutbound( CoreTopologyService discoveryService, Outbound<AdvertisedSocketAddress,Message> outbound,
                         ClusterIdentity clusterIdentity, LogProvider logProvider, long logThresholdMillis )
    {
        this.discoveryService = discoveryService;
        this.outbound = outbound;
        this.clusterIdentity = clusterIdentity;
        this.log = logProvider.getLog( getClass() );
        this.unknownAddressMonitor = new UnknownAddressMonitor( log, Clocks.systemClock(), logThresholdMillis );
    }

    @Override
    public void send( MemberId to, RaftMessage message )
    {
        ClusterId clusterId = clusterIdentity.clusterId();
        if ( clusterId == null )
        {
            log.warn( "Attempting to send a message before bound to a cluster" );
            return;
        }

        Optional<CoreAddresses> coreAddresses = discoveryService.coreServers().find( to );
        if ( coreAddresses.isPresent() )
        {
            outbound.send( coreAddresses.get().getRaftServer(), new ClusterIdAwareMessage( clusterId, message ) );
        }
        else
        {
            unknownAddressMonitor.logAttemptToSendToMemberWithNoKnownAddress( to );
        }
    }
}
