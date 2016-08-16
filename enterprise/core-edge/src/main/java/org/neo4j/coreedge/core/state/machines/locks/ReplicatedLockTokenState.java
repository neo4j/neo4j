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
package org.neo4j.coreedge.core.state.machines.locks;

import java.io.IOException;

import org.neo4j.coreedge.messaging.marshalling.ChannelMarshal;
import org.neo4j.coreedge.messaging.EndOfStreamException;
import org.neo4j.coreedge.core.state.storage.SafeStateMarshal;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

import static org.neo4j.coreedge.core.state.machines.locks.ReplicatedLockTokenRequest.INVALID_REPLICATED_LOCK_TOKEN_REQUEST;

public class ReplicatedLockTokenState
{
    private ReplicatedLockTokenRequest currentToken = INVALID_REPLICATED_LOCK_TOKEN_REQUEST;
    private long ordinal = -1L;

    ReplicatedLockTokenState()
    {
    }

    ReplicatedLockTokenState( long ordinal, ReplicatedLockTokenRequest currentToken )
    {
        this.ordinal = ordinal;
        this.currentToken = currentToken;
    }

    public void set( ReplicatedLockTokenRequest currentToken, long ordinal )
    {
        this.currentToken = currentToken;
        this.ordinal = ordinal;
    }

    public ReplicatedLockTokenRequest get()
    {
        return currentToken;
    }

    long ordinal()
    {
        return ordinal;
    }

    @Override
    public String toString()
    {
        return "ReplicatedLockTokenState{" +
                "currentToken=" + currentToken +
                ", ordinal=" + ordinal +
                '}';
    }

    ReplicatedLockTokenState newInstance()
    {
        return new ReplicatedLockTokenState( ordinal, currentToken );
    }

    public static class Marshal extends SafeStateMarshal<ReplicatedLockTokenState>
    {
        private final ChannelMarshal<MemberId> memberMarshal;

        public Marshal( ChannelMarshal<MemberId> memberMarshal )
        {
            this.memberMarshal = memberMarshal;
        }

        @Override
        public void marshal( ReplicatedLockTokenState state,
                             WritableChannel channel ) throws IOException
        {
            channel.putLong( state.ordinal );
            channel.putInt( state.get().id() );
            memberMarshal.marshal( state.get().owner(), channel );
        }

        @Override
        public ReplicatedLockTokenState unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
        {
            long logIndex = channel.getLong();
            int candidateId = channel.getInt();
            MemberId member = memberMarshal.unmarshal( channel );

            return new ReplicatedLockTokenState( logIndex, new ReplicatedLockTokenRequest( member, candidateId ) );
        }

        @Override
        public ReplicatedLockTokenState startState()
        {
            return new ReplicatedLockTokenState();
        }

        @Override
        public long ordinal( ReplicatedLockTokenState state )
        {
            return state.ordinal();
        }
    }
}
