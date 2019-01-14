/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.messaging;

import java.util.Optional;
import java.util.function.Supplier;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
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

        Optional<CoreServerInfo> coreServerInfo = coreTopologyService.localCoreServers().find( to );
        if ( coreServerInfo.isPresent() )
        {
            outbound.send( coreServerInfo.get().getRaftServer(), RaftMessages.ClusterIdAwareMessage.of( clusterId.get(), message ), block );
        }
        else
        {
            unknownAddressMonitor.logAttemptToSendToMemberWithNoKnownAddress( to );
        }
    }
}
