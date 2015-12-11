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
package org.neo4j.coreedge.server.core;

import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.replication.Replicator;

/**
 * Listens for {@link ReplicatedLockRequest}. Keeps track of the current holder of the replicated exclusive lock,
 * which is identified by a monotonically increasing session id, and an owning member.
 */
public class ReplicatedLockStateMachine<MEMBER>
        implements Replicator.ReplicatedContentListener, CurrentReplicatedLockState
{
    private final MEMBER myself;
    private LockSession currentLockSession = new LockSession( 0, null );

    public ReplicatedLockStateMachine( MEMBER myself, Replicator replicator )
    {
        this.myself = myself;
        replicator.subscribe( this );
    }

    @Override
    public synchronized void onReplicated( ReplicatedContent content, long logIndex )
    {
        if ( content instanceof ReplicatedLockRequest )
        {
            ReplicatedLockRequest<MEMBER> lockRequest = (ReplicatedLockRequest<MEMBER>) content;
            int requestedLockSessionId = lockRequest.requestedLockSessionId;

            if ( requestedLockSessionId > currentLockSession.id() )
            {
                currentLockSession = new LockSession(requestedLockSessionId, lockRequest.owner());
            }

            notifyAll();
        }
    }

    public int nextId()
    {
        return currentLockSession.id + 1;
    }

    @Override
    public CurrentReplicatedLockState.LockSession currentLockSession()
    {
        return currentLockSession;
    }

    public final class LockSession implements CurrentReplicatedLockState.LockSession
    {
        private final int id;
        private final MEMBER owner;

        public LockSession( int id, MEMBER owner )
        {
            this.id = id;
            this.owner = owner;
        }

        @Override
        public int id()
        {
            return id;
        }

        @Override
        public boolean isMine()
        {
            return myself.equals( owner );
        }
    }
}
