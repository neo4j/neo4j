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
import java.util.function.Consumer;

import org.neo4j.causalclustering.core.state.Result;
import org.neo4j.causalclustering.core.state.machines.StateMachine;
import org.neo4j.causalclustering.core.state.storage.StateStorage;

/**
 * Listens for {@link ReplicatedLockTokenRequest}. Keeps track of the current holder of the replicated token,
 * which is identified by a monotonically increasing id, and an owning member.
 */
public class ReplicatedLockTokenStateMachine implements StateMachine<ReplicatedLockTokenRequest>
{
    private final StateStorage<ReplicatedLockTokenState> storage;
    private ReplicatedLockTokenState state;

    public ReplicatedLockTokenStateMachine( StateStorage<ReplicatedLockTokenState> storage )
    {
        this.storage = storage;
    }

    @Override
    public synchronized void applyCommand( ReplicatedLockTokenRequest tokenRequest, long commandIndex,
            Consumer<Result> callback )
    {
        if ( commandIndex <= state().ordinal() )
        {
            return;
        }

        boolean requestAccepted = tokenRequest.id() == LockToken.nextCandidateId( currentToken().id() );
        if ( requestAccepted )
        {
            state().set( tokenRequest, commandIndex );
        }

        callback.accept( Result.of( requestAccepted ) );
    }

    @Override
    public synchronized void flush() throws IOException
    {
        storage.persistStoreData( state() );
    }

    @Override
    public long lastAppliedIndex()
    {
        return state().ordinal();
    }

    private ReplicatedLockTokenState state()
    {
        if ( state == null )
        {
            state = storage.getInitialState();
        }
        return state;
    }

    public synchronized ReplicatedLockTokenState snapshot()
    {
        return state().newInstance();
    }

    public synchronized void installSnapshot( ReplicatedLockTokenState snapshot )
    {
        state = snapshot;
    }

    /**
     * @return The currently valid token.
     */
    public synchronized ReplicatedLockTokenRequest currentToken()
    {
        return state().get();
    }
}
