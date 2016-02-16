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
import org.neo4j.coreedge.raft.state.StateMarshal;
import org.neo4j.storageengine.api.ReadPastEndException;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

import static org.neo4j.coreedge.server.core.locks.ReplicatedLockTokenRequest.INVALID_REPLICATED_LOCK_TOKEN_REQUEST;

public class ReplicatedLockTokenState<MEMBER>
{
    private ReplicatedLockTokenRequest<MEMBER> currentToken = INVALID_REPLICATED_LOCK_TOKEN_REQUEST;
    private long ordinal = -1L;

    public ReplicatedLockTokenState()
    {
    }

    public ReplicatedLockTokenState( long ordinal, int candidateId, MEMBER member )
    {
        this.ordinal = ordinal;
        this.currentToken = new ReplicatedLockTokenRequest<>( member, candidateId );
    }

    public void set( ReplicatedLockTokenRequest<MEMBER> currentToken, long ordinal )
    {
        this.currentToken = currentToken;
        this.ordinal = ordinal;
    }

    public ReplicatedLockTokenRequest<MEMBER> get()
    {
        return currentToken;
    }

    long ordinal()
    {
        return ordinal;
    }

    public static class Marshal<MEMBER> implements
            StateMarshal<ReplicatedLockTokenState<MEMBER>>
    {
        private final ChannelMarshal<MEMBER> memberMarshal;

        public Marshal( ChannelMarshal<MEMBER> marshal )
        {
            this.memberMarshal = marshal;
        }

        @Override
        public void marshal( ReplicatedLockTokenState<MEMBER> state,
                             WritableChannel channel ) throws IOException
        {
            channel.putLong( state.ordinal );
            channel.putInt( state.get().id() );
            memberMarshal.marshal( state.get().owner(), channel );
        }

        @Override
        public ReplicatedLockTokenState<MEMBER> unmarshal( ReadableChannel source ) throws IOException
        {
            try
            {
                long logIndex = source.getLong();
                int candidateId = source.getInt();

                final MEMBER member = memberMarshal.unmarshal( source );

                return new ReplicatedLockTokenState<>( logIndex, candidateId, member );
            }
            catch ( ReadPastEndException ex )
            {
                return null;
            }
        }

        @Override
        public ReplicatedLockTokenState<MEMBER> startState()
        {
            return new ReplicatedLockTokenState<>();
        }

        @Override
        public long ordinal( ReplicatedLockTokenState<MEMBER> state )
        {
            return state.ordinal();
        }
    }
}
