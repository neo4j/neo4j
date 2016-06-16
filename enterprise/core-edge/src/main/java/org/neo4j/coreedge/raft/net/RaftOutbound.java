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
package org.neo4j.coreedge.raft.net;

import java.util.Collection;

import org.neo4j.coreedge.catchup.storecopy.LocalDatabase;
import org.neo4j.coreedge.network.Message;
import org.neo4j.coreedge.raft.RaftMessages.RaftMessage;
import org.neo4j.coreedge.raft.RaftMessages.StoreIdAwareMessage;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.CoreMember;

import static java.util.stream.Collectors.toList;

public class RaftOutbound implements Outbound<CoreMember, RaftMessage<CoreMember>>
{
    private final Outbound<AdvertisedSocketAddress,Message> outbound;
    private final LocalDatabase localDatabase;

    public RaftOutbound( Outbound<AdvertisedSocketAddress,Message> outbound, LocalDatabase localDatabase )
    {
        this.outbound = outbound;
        this.localDatabase = localDatabase;
    }

    @Override
    public void send( CoreMember to, RaftMessage<CoreMember> message )
    {
        outbound.send( to.getRaftAddress(), storeIdify( message ) );
    }

    @Override
    public void send( CoreMember to, Collection<RaftMessage<CoreMember>> raftMessages )
    {
        outbound.send( to.getRaftAddress(), raftMessages.stream().map( this::storeIdify ).collect( toList() ) );
    }

    private StoreIdAwareMessage<CoreMember> storeIdify( RaftMessage<CoreMember> m )
    {
        return new StoreIdAwareMessage<>( localDatabase.storeId(), m );
    }
}
