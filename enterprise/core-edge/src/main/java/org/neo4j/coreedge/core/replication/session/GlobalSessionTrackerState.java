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
package org.neo4j.coreedge.core.replication.session;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.neo4j.coreedge.messaging.marshalling.ChannelMarshal;
import org.neo4j.coreedge.messaging.EndOfStreamException;
import org.neo4j.coreedge.core.state.storage.SafeStateMarshal;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

/**
 * In memory implementation of {@link GlobalSessionTrackerState}.
 */
public class GlobalSessionTrackerState
{
    /**
     * Each owner can only have one local session tracker, identified by the unique global session ID.
     */
    private Map<MemberId, LocalSessionTracker> sessionTrackers = new HashMap<>();

    private long logIndex = -1L;

    /**
     * Tracks the operation and returns true iff this operation should be allowed.
     */
    public boolean validateOperation( GlobalSession globalSession, LocalOperationId localOperationId )
    {
        LocalSessionTracker existingSessionTracker = sessionTrackers.get( globalSession.owner() );
        if ( isNewSession( globalSession, existingSessionTracker ) )
        {
            return isFirstOperation( localOperationId );
        }
        else
        {
            return existingSessionTracker.isValidOperation( localOperationId );
        }
    }

    public void update( GlobalSession globalSession, LocalOperationId localOperationId, long logIndex )
    {
        LocalSessionTracker localSessionTracker = validateGlobalSessionAndGetLocalSessionTracker( globalSession );
        localSessionTracker.validateAndTrackOperation( localOperationId );
        this.logIndex = logIndex;
    }

    private boolean isNewSession( GlobalSession globalSession, LocalSessionTracker existingSessionTracker )
    {
        return existingSessionTracker == null ||
                !existingSessionTracker.globalSessionId.equals( globalSession.sessionId() );
    }

    private boolean isFirstOperation( LocalOperationId id )
    {
        return id.sequenceNumber() == 0;
    }

    public long logIndex()
    {
        return logIndex;
    }

    private LocalSessionTracker validateGlobalSessionAndGetLocalSessionTracker( GlobalSession globalSession )
    {
        LocalSessionTracker localSessionTracker = sessionTrackers.get( globalSession.owner() );

        if ( localSessionTracker == null ||
                !localSessionTracker.globalSessionId.equals( globalSession.sessionId() ) )
        {
            localSessionTracker = new LocalSessionTracker( globalSession.sessionId(), new HashMap<>() );
            sessionTrackers.put( globalSession.owner(), localSessionTracker );
        }

        return localSessionTracker;
    }

    public GlobalSessionTrackerState newInstance()
    {
        GlobalSessionTrackerState copy = new GlobalSessionTrackerState();
        for ( Map.Entry<MemberId,LocalSessionTracker> entry : sessionTrackers.entrySet() )
        {
            copy.sessionTrackers.put( entry.getKey(), entry.getValue().newInstance() );
        }
        return copy;
    }

    @Override
    public String toString()
    {
        return String.format( "GlobalSessionTrackerState{sessionTrackers=%s, logIndex=%d}", sessionTrackers, logIndex );
    }

    public static class Marshal extends SafeStateMarshal<GlobalSessionTrackerState>
    {
        private final ChannelMarshal<MemberId> memberMarshal;

        public Marshal( ChannelMarshal<MemberId> marshal )
        {
            this.memberMarshal = marshal;
        }

        @Override
        public void marshal( GlobalSessionTrackerState target, WritableChannel channel )
                throws IOException
        {
            final Map<MemberId, LocalSessionTracker> sessionTrackers = target.sessionTrackers;

            channel.putLong( target.logIndex );
            channel.putInt( sessionTrackers.size() );

            for ( Map.Entry<MemberId, LocalSessionTracker> entry : sessionTrackers.entrySet() )
            {
                memberMarshal.marshal( entry.getKey(), channel );
                final LocalSessionTracker localSessionTracker = entry.getValue();

                final UUID uuid = localSessionTracker.globalSessionId;
                channel.putLong( uuid.getMostSignificantBits() );
                channel.putLong( uuid.getLeastSignificantBits() );

                final Map<Long, Long> map = localSessionTracker.lastSequenceNumberPerSession;

                channel.putInt( map.size() );

                for ( Map.Entry<Long, Long> sessionSequence : map.entrySet() )
                {
                    channel.putLong( sessionSequence.getKey() );
                    channel.putLong( sessionSequence.getValue() );
                }
            }
        }

        @Override
        public GlobalSessionTrackerState unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
        {
            final long logIndex = channel.getLong();
            final int sessionTrackerSize = channel.getInt();
            final Map<MemberId, LocalSessionTracker> sessionTrackers = new HashMap<>();

            for ( int i = 0; i < sessionTrackerSize; i++ )
            {
                final MemberId member = memberMarshal.unmarshal( channel );
                if ( member == null )
                {
                    throw new IllegalStateException( "Null member" );
                }

                long mostSigBits = channel.getLong();
                long leastSigBits = channel.getLong();
                UUID globalSessionId = new UUID( mostSigBits, leastSigBits );

                final int localSessionTrackerSize = channel.getInt();
                final Map<Long, Long> lastSequenceNumberPerSession = new HashMap<>();
                for ( int j = 0; j < localSessionTrackerSize; j++ )
                {
                    long localSessionId = channel.getLong();
                    long sequenceNumber = channel.getLong();
                    lastSequenceNumberPerSession.put( localSessionId, sequenceNumber );
                }
                final LocalSessionTracker localSessionTracker = new LocalSessionTracker( globalSessionId, lastSequenceNumberPerSession );
                sessionTrackers.put( member, localSessionTracker );
            }
            GlobalSessionTrackerState result = new GlobalSessionTrackerState();
            result.sessionTrackers = sessionTrackers;
            result.logIndex = logIndex;
            return result;
        }

        @Override
        public GlobalSessionTrackerState startState()
        {
            return new GlobalSessionTrackerState();
        }

        @Override
        public long ordinal( GlobalSessionTrackerState state )
        {
            return state.logIndex();
        }
    }

    private static class LocalSessionTracker
    {
        final UUID globalSessionId;
        final Map<Long,Long> lastSequenceNumberPerSession; /* localSessionId -> lastSequenceNumber */

        LocalSessionTracker( UUID globalSessionId, Map<Long,Long> lastSequenceNumberPerSession )
        {
            this.globalSessionId = globalSessionId;
            this.lastSequenceNumberPerSession = lastSequenceNumberPerSession;
        }

        boolean validateAndTrackOperation( LocalOperationId operationId )
        {
            if ( !isValidOperation( operationId ) )
            {
                return false;
            }

            lastSequenceNumberPerSession.put( operationId.localSessionId(), operationId.sequenceNumber() );
            return true;
        }

        /**
         * The sequence numbers under a single local session must come strictly in order and are only valid once only.
         */
        private boolean isValidOperation( LocalOperationId operationId )
        {
            Long lastSequenceNumber = lastSequenceNumberPerSession.get( operationId.localSessionId() );

            if ( lastSequenceNumber == null )
            {
                if ( operationId.sequenceNumber() != 0 )
                {
                    return false;
                }
            }
            else if ( operationId.sequenceNumber() != lastSequenceNumber + 1 )
            {
                return false;
            }

            return true;
        }

        public LocalSessionTracker newInstance()
        {
            return new LocalSessionTracker( globalSessionId, new HashMap<>( lastSequenceNumberPerSession ) );
        }

        @Override
        public String toString()
        {
            return String.format( "LocalSessionTracker{globalSessionId=%s, lastSequenceNumberPerSession=%s}",
                    globalSessionId, lastSequenceNumberPerSession );
        }
    }
}
