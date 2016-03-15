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
import java.util.Map;
import java.util.Optional;

import org.neo4j.coreedge.catchup.storecopy.core.RaftStateType;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.state.StateMachine;
import org.neo4j.coreedge.raft.state.StateStorage;
import org.neo4j.coreedge.raft.state.id_allocation.IdAllocationState;

import static java.util.Collections.singletonMap;

/**
 * Listens for {@link ReplicatedLockTokenRequest}. Keeps track of the current holder of the replicated token,
 * which is identified by a monotonically increasing id, and an owning member.
 */
public class ReplicatedLockTokenStateMachine<MEMBER> implements StateMachine
{
    private ReplicatedLockTokenState<MEMBER> state;
    private final StateStorage<ReplicatedLockTokenState<MEMBER>> storage;
    private final PendingLockTokensRequests<MEMBER> pendingLockTokensRequests;

    public ReplicatedLockTokenStateMachine( StateStorage<ReplicatedLockTokenState<MEMBER>> storage,
                                            PendingLockTokensRequests<MEMBER> pendingLockTokensRequests )
    {
        this.storage = storage;
        this.state = storage.getInitialState();
        this.pendingLockTokensRequests = pendingLockTokensRequests;
        this.pendingLockTokensRequests.setCurrentToken( state.get() );
    }

    @Override
    public synchronized void applyCommand( ReplicatedContent content, long logIndex )
    {
        if ( content instanceof ReplicatedLockTokenRequest && logIndex > state.ordinal() )
        {
            ReplicatedLockTokenRequest<MEMBER> tokenRequest = (ReplicatedLockTokenRequest<MEMBER>) content;
            Optional<PendingLockTokenRequest> future = Optional.ofNullable( pendingLockTokensRequests.retrieve( tokenRequest ) );
            if ( tokenRequest.id() == LockToken.nextCandidateId( currentToken().id() ) )
            {
                state.set( tokenRequest, logIndex );
                pendingLockTokensRequests.setCurrentToken( tokenRequest );
                future.ifPresent( PendingLockTokenRequest::notifyAcquired );
            }
            else
            {
                future.ifPresent( PendingLockTokenRequest::notifyLost );
            }
        }
    }

    @Override
    public void flush() throws IOException
    {
        storage.persistStoreData( state );
    }

    @Override
    public Map<RaftStateType, Object> snapshot()
    {
        return singletonMap( RaftStateType.LOCK_TOKEN, state );
    }

    @Override
    public void installSnapshot( Map<RaftStateType, Object> snapshot )
    {
        if ( snapshot.containsKey( RaftStateType.ID_ALLOCATION ) )
        {
            state = (ReplicatedLockTokenState<MEMBER>) snapshot.get( RaftStateType.LOCK_TOKEN );
        }
    }

    /**
     * @return The currently valid token.
     */
    public synchronized ReplicatedLockTokenRequest<MEMBER> currentToken()
    {
        return state.get();
    }
}
