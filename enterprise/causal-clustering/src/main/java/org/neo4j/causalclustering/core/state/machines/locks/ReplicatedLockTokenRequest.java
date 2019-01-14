/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core.state.machines.locks;

import java.util.Objects;
import java.util.function.Consumer;

import org.neo4j.causalclustering.core.state.CommandDispatcher;
import org.neo4j.causalclustering.core.state.Result;
import org.neo4j.causalclustering.core.state.machines.tx.CoreReplicatedContent;
import org.neo4j.causalclustering.identity.MemberId;

import static java.lang.String.format;

public class ReplicatedLockTokenRequest implements CoreReplicatedContent, LockToken
{
    private final MemberId owner;
    private final int candidateId;

    static final ReplicatedLockTokenRequest INVALID_REPLICATED_LOCK_TOKEN_REQUEST =
            new ReplicatedLockTokenRequest( null, LockToken.INVALID_LOCK_TOKEN_ID );

    public ReplicatedLockTokenRequest( MemberId owner, int candidateId )
    {
        this.owner = owner;
        this.candidateId = candidateId;
    }

    @Override
    public int id()
    {
        return candidateId;
    }

    @Override
    public MemberId owner()
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
    public void dispatch( CommandDispatcher commandDispatcher, long commandIndex, Consumer<Result> callback )
    {
        commandDispatcher.dispatch( this, commandIndex, callback );
    }
}
