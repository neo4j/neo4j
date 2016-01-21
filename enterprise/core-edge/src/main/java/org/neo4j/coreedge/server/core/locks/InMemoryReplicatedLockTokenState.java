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
package org.neo4j.coreedge.server.core.locks;

import java.io.IOException;

import org.neo4j.coreedge.raft.state.ChannelMarshal;
import org.neo4j.storageengine.api.ReadPastEndException;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

import static org.neo4j.coreedge.server.core.locks.ReplicatedLockTokenRequest.INVALID_REPLICATED_LOCK_TOKEN_REQUEST;

public class InMemoryReplicatedLockTokenState<MEMBER> implements ReplicatedLockTokenState<MEMBER>
{
    private ReplicatedLockTokenRequest<MEMBER> currentToken = INVALID_REPLICATED_LOCK_TOKEN_REQUEST;
    private long logIndex = -1L;

    public InMemoryReplicatedLockTokenState()
    {
    }

    public InMemoryReplicatedLockTokenState( InMemoryReplicatedLockTokenState<MEMBER> other )
    {
        this.currentToken = other.currentToken;
    }

    public InMemoryReplicatedLockTokenState( long logIndex, int candidateId, MEMBER member )
    {
        this.logIndex = logIndex;
        this.currentToken = new ReplicatedLockTokenRequest<>( member, candidateId );
    }

    @Override
    public void set( ReplicatedLockTokenRequest<MEMBER> currentToken, long logIndex )
    {
        this.currentToken = currentToken;
        this.logIndex = logIndex;
    }

    @Override
    public ReplicatedLockTokenRequest<MEMBER> get()
    {
        return currentToken;
    }

    long logIndex()
    {
        return logIndex;
    }

    public static class InMemoryReplicatedLockStateChannelMarshal<MEMBER>
            implements ChannelMarshal<InMemoryReplicatedLockTokenState<MEMBER>>
    {
        private final ChannelMarshal<MEMBER> memberMarshal;

        public InMemoryReplicatedLockStateChannelMarshal( ChannelMarshal<MEMBER> marshal )
        {
            this.memberMarshal = marshal;
        }

        @Override
        public void marshal( InMemoryReplicatedLockTokenState<MEMBER> state,
                             WritableChannel channel ) throws IOException
        {
            channel.putLong( state.logIndex );
            channel.putInt( state.get().id() );
            memberMarshal.marshal( state.get().owner(), channel );
        }

        @Override
        public InMemoryReplicatedLockTokenState<MEMBER> unmarshal( ReadableChannel source ) throws IOException
        {
            try
            {
                long logIndex = source.getLong();
                int candidateId = source.getInt();

                final MEMBER member = memberMarshal.unmarshal( source );

                return new InMemoryReplicatedLockTokenState<>( logIndex, candidateId, member );
            }
            catch ( ReadPastEndException ex )
            {
                return null;
            }
        }
    }
}
