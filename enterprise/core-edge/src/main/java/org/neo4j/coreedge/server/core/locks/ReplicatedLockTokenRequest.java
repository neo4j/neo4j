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

import java.util.Objects;
import java.util.Optional;

import org.neo4j.coreedge.raft.replication.tx.CoreReplicatedContent;
import org.neo4j.coreedge.raft.state.CoreStateMachines;
import org.neo4j.coreedge.raft.state.Result;

import static java.lang.String.format;

public class ReplicatedLockTokenRequest<MEMBER> implements CoreReplicatedContent, LockToken
{
    private final MEMBER owner;
    private final int candidateId;

    public static final ReplicatedLockTokenRequest INVALID_REPLICATED_LOCK_TOKEN_REQUEST =
            new ReplicatedLockTokenRequest<>( null, LockToken.INVALID_LOCK_TOKEN_ID );

    public ReplicatedLockTokenRequest( MEMBER owner, int candidateId )
    {
        this.owner = owner;
        this.candidateId = candidateId;
    }

    @Override
    public int id()
    {
        return candidateId;
    }

    public MEMBER owner()
    {
        return owner;
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
        ReplicatedLockTokenRequest that = (ReplicatedLockTokenRequest) o;
        return candidateId == that.candidateId && Objects.equals( owner, that.owner );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( owner, candidateId );
    }

    @Override
    public String toString()
    {
        return format( "ReplicatedLockTokenRequest{owner=%s, candidateId=%d}", owner, candidateId );
    }

    @Override
    public Optional<Result> dispatch( CoreStateMachines coreStateMachines, long commandIndex )
    {
        return coreStateMachines.dispatch( this, commandIndex );
    }
}
