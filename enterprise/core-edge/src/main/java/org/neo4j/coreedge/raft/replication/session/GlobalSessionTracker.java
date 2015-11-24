/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.neo4j.coreedge.server.CoreMember;

/**
 * Each instance has a global session as well as several local sessions. Each local session
 * tracks its operation by assigning a unique sequence number to each operation. This allows
 * an operation originating from an instance to be uniquely identified and duplicate attempts
 * at performing that operation can be filtered out.
 *
 * The session tracker defines the strategy for which local operations are allowed to be performed
 * and the strategy is to only allow operations to occur in strict order, that is with no gaps,
 * starting with sequence number zero. This is done for reasons of efficiency and creates a very
 * direct coupling between session tracking and operation validation. This class is in charge
 * of both.
 */
public class GlobalSessionTracker
{
    class LocalSessionTracker
    {
        UUID globalSessionId;
        Map<Long,Long> lastSequenceNumberPerSession = new HashMap<>(); /* localSessionId -> lastSequenceNumber */

        LocalSessionTracker( UUID globalSessionId )
        {
            this.globalSessionId = globalSessionId;
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
         * The sequence numbers under a single local session must come strictly in order
         * and are only valid exactly once.*/
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

    /** Each owner can only have one local session tracker, identified by the unique global session ID. */
    Map<CoreMember,LocalSessionTracker> sessionTrackers = new HashMap<>();

    /** Tracks the operation and returns whether or not this operation should be allowed. */
    public boolean validateAndTrackOperation( GlobalSession globalSession, LocalOperationId localOperationId )
    {
        LocalSessionTracker localSessionTracker = validateGlobalSessionAndGetLocalSessionTracker( globalSession );
        return localSessionTracker.validateAndTrackOperation( localOperationId );
    }

    private LocalSessionTracker validateGlobalSessionAndGetLocalSessionTracker( GlobalSession globalSession )
    {
        LocalSessionTracker localSessionTracker = sessionTrackers.get( globalSession.owner() );

        if( localSessionTracker == null )
        {
            localSessionTracker = new LocalSessionTracker( globalSession.sessionId() );
            sessionTrackers.put( globalSession.owner(), localSessionTracker );
        }
        else if ( !localSessionTracker.globalSessionId.equals( globalSession.sessionId() ) )
        {
            localSessionTracker = new LocalSessionTracker( globalSession.sessionId() );
            sessionTrackers.put( globalSession.owner(), localSessionTracker );
        }

        return localSessionTracker;
    }
}
