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
package org.neo4j.coreedge.raft.replication.session;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.neo4j.coreedge.raft.state.ChannelMarshal;
import org.neo4j.storageengine.api.ReadPastEndException;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

/**
 * In memory implementation of {@link GlobalSessionTrackerState}.
 *
 * @param <MEMBER> The type of members tracked by this session tracker state
 */
public class InMemoryGlobalSessionTrackerState<MEMBER> implements GlobalSessionTrackerState<MEMBER>
{
    /**
     * Each owner can only have one local session tracker, identified by the unique global session ID.
     */
    private Map<MEMBER, LocalSessionTracker> sessionTrackers = new HashMap<>();

    private long logIndex = -1L;

    public InMemoryGlobalSessionTrackerState() {  }

    public InMemoryGlobalSessionTrackerState( InMemoryGlobalSessionTrackerState<MEMBER> other )
    {
        this.sessionTrackers = new HashMap<>();
        for ( Map.Entry<MEMBER, LocalSessionTracker> otherEntry : other.sessionTrackers.entrySet() )
        {
            sessionTrackers.put( otherEntry.getKey(), new LocalSessionTracker( otherEntry.getValue() ) );
        }
        this.logIndex = other.logIndex;
    }

    @Override
    public boolean validateOperation( GlobalSession<MEMBER> globalSession, LocalOperationId localOperationId )
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

    @Override
    public void update( GlobalSession<MEMBER> globalSession, LocalOperationId localOperationId, long logIndex )
    {
        LocalSessionTracker localSessionTracker = validateGlobalSessionAndGetLocalSessionTracker( globalSession );
        localSessionTracker.validateAndTrackOperation( localOperationId );
        this.logIndex = logIndex;
    }

    private boolean isNewSession( GlobalSession<MEMBER> globalSession, LocalSessionTracker existingSessionTracker )
    {
        return existingSessionTracker == null ||
                !existingSessionTracker.globalSessionId.equals( globalSession.sessionId() );
    }

    private boolean isFirstOperation( LocalOperationId id )
    {
        return id.sequenceNumber() == 0;
    }

    @Override
    public long logIndex()
    {
        return logIndex;
    }

    private LocalSessionTracker validateGlobalSessionAndGetLocalSessionTracker( GlobalSession<MEMBER> globalSession )
    {
        LocalSessionTracker localSessionTracker = sessionTrackers.get( globalSession.owner() );

        if ( localSessionTracker == null ||
                !localSessionTracker.globalSessionId.equals( globalSession.sessionId() ) )
        {
            localSessionTracker = new LocalSessionTracker( globalSession.sessionId() );
            sessionTrackers.put( globalSession.owner(), localSessionTracker );
        }

        return localSessionTracker;
    }

    static class InMemoryGlobalSessionTrackerStateChannelMarshal<MEMBER> implements
            ChannelMarshal<InMemoryGlobalSessionTrackerState<MEMBER>>
    {
        private final ChannelMarshal<MEMBER> memberMarshal;

        public InMemoryGlobalSessionTrackerStateChannelMarshal( ChannelMarshal<MEMBER> marshal )
        {
            this.memberMarshal = marshal;
        }

        @Override
        public void marshal( InMemoryGlobalSessionTrackerState<MEMBER> target, WritableChannel channel )
                throws IOException
        {
            final Map<MEMBER, LocalSessionTracker> sessionTrackers = target.sessionTrackers;

            channel.putLong( target.logIndex );
            channel.putInt( sessionTrackers.size() );

            for ( Map.Entry<MEMBER, LocalSessionTracker> entry : sessionTrackers.entrySet() )
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
        public InMemoryGlobalSessionTrackerState<MEMBER> unmarshal( ReadableChannel source ) throws IOException
        {
            try
            {
                final long logIndex = source.getLong();
                final int sessionTrackerSize = source.getInt();
                final Map<MEMBER, LocalSessionTracker> theMainMap = new HashMap<>();

                for ( int i = 0; i < sessionTrackerSize; i++ )
                {
                    final MEMBER unmarshal = memberMarshal.unmarshal( source );
                    if ( unmarshal == null )
                    {
                        return null;
                    }

                    final LocalSessionTracker localSessionTracker =
                            new LocalSessionTracker( new UUID( source.getLong(), source.getLong() ) );

                    final int localSessionTrackerSize = source.getInt();
                    final HashMap<Long, Long> notSureAboutTheName = new HashMap<>();
                    for ( int j = 0; j < localSessionTrackerSize; j++ )
                    {
                        notSureAboutTheName.put( source.getLong(), source.getLong() );
                    }
                    localSessionTracker.lastSequenceNumberPerSession = notSureAboutTheName;
                    theMainMap.put( unmarshal, localSessionTracker );
                }
                InMemoryGlobalSessionTrackerState<MEMBER> result = new InMemoryGlobalSessionTrackerState<>();
                result.sessionTrackers = theMainMap;
                result.logIndex = logIndex;
                return result;
            }
            catch ( ReadPastEndException notEnoughBytes )
            {
                return null;
            }
        }
    }

    private static class LocalSessionTracker
    {
        UUID globalSessionId;
        Map<Long, Long> lastSequenceNumberPerSession = new HashMap<>(); /* localSessionId -> lastSequenceNumber */

        LocalSessionTracker( UUID globalSessionId )
        {
            this.globalSessionId = globalSessionId;
        }

        public LocalSessionTracker( LocalSessionTracker other )
        {
            this.globalSessionId = other.globalSessionId;
            this.lastSequenceNumberPerSession = new HashMap<>( other.lastSequenceNumberPerSession );
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
    }
}
