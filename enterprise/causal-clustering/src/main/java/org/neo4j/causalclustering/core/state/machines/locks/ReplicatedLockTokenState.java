/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core.state.machines.locks;

import java.io.IOException;
import java.util.Objects;

import org.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.causalclustering.core.state.storage.SafeStateMarshal;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

import static org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenRequest.INVALID_REPLICATED_LOCK_TOKEN_REQUEST;

public class ReplicatedLockTokenState
{
    private ReplicatedLockTokenRequest currentToken = INVALID_REPLICATED_LOCK_TOKEN_REQUEST;
    private long ordinal = -1L;

    public ReplicatedLockTokenState()
    {
    }

    public ReplicatedLockTokenState( long ordinal, ReplicatedLockTokenRequest currentToken )
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
        return String.format( "ReplicatedLockTokenState{currentToken=%s, ordinal=%d}", currentToken, ordinal );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        ReplicatedLockTokenState that = (ReplicatedLockTokenState) o;
        return ordinal == that.ordinal &&
                Objects.equals( currentToken, that.currentToken );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( currentToken, ordinal );
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
