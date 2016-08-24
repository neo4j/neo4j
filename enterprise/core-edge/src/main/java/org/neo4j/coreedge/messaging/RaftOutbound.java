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
package org.neo4j.coreedge.messaging;

import java.util.Collection;

import org.neo4j.coreedge.catchup.storecopy.LocalDatabase;
import org.neo4j.coreedge.core.consensus.RaftMessages.RaftMessage;
import org.neo4j.coreedge.core.consensus.RaftMessages.StoreIdAwareMessage;
import org.neo4j.coreedge.discovery.CoreAddresses;
import org.neo4j.coreedge.discovery.CoreTopologyService;
import org.neo4j.coreedge.discovery.NoKnownAddressesException;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.coreedge.messaging.address.AdvertisedSocketAddress;
import org.neo4j.coreedge.messaging.address.UnknownAddressMonitor;
import org.neo4j.logging.LogProvider;
import org.neo4j.time.Clocks;

import static java.util.stream.Collectors.toList;

public class RaftOutbound implements Outbound<MemberId, RaftMessage>
{
    private final CoreTopologyService discoveryService;
    private final Outbound<AdvertisedSocketAddress,Message> outbound;
    private final LocalDatabase localDatabase;
    private final UnknownAddressMonitor unknownAddressMonitor;

    public RaftOutbound( CoreTopologyService discoveryService, Outbound<AdvertisedSocketAddress,Message> outbound,
            LocalDatabase localDatabase, LogProvider logProvider, long logThresholdMillis )
    {
        this.discoveryService = discoveryService;
        this.outbound = outbound;
        this.localDatabase = localDatabase;
        this.unknownAddressMonitor = new UnknownAddressMonitor(
                logProvider.getLog( this.getClass() ), Clocks.systemClock(), logThresholdMillis );
    }

    @Override
    public void send( MemberId to, RaftMessage message )
    {
        try
        {
            CoreAddresses coreAddresses = discoveryService.currentTopology().coreAddresses( to );
            outbound.send( coreAddresses.getRaftServer(), decorateWithStoreId( message ) );
        }
        catch ( NoKnownAddressesException e )
        {
            unknownAddressMonitor.logAttemptToSendToMemberWithNoKnownAddress( to );
        }
    }

    @Override
    public void send( MemberId to, Collection<RaftMessage> messages )
    {
        try
        {
            CoreAddresses coreAddresses = discoveryService.currentTopology().coreAddresses( to );
            outbound.send( coreAddresses.getRaftServer(),
                    messages.stream().map( this::decorateWithStoreId ).collect( toList() ) );
        }
        catch ( NoKnownAddressesException e )
        {
            unknownAddressMonitor.logAttemptToSendToMemberWithNoKnownAddress( to );
        }
    }

    private StoreIdAwareMessage decorateWithStoreId( RaftMessage m )
    {
        return new StoreIdAwareMessage( localDatabase.storeId(), m );
    }
}
