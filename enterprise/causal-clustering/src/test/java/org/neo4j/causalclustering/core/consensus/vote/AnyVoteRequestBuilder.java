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
package org.neo4j.causalclustering.core.consensus.vote;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.identity.MemberId;

public abstract class AnyVoteRequestBuilder<T extends RaftMessages.AnyVote.Request>
{
    protected AnyVoteRequestBuilder( Constructor<T> constructor )
    {
        this.constructor = constructor;
    }

    @FunctionalInterface
    interface Constructor<T extends RaftMessages.AnyVote.Request>
    {
        T construct( MemberId from, long term, MemberId candidate, long lastLogIndex, long lastLogTerm );
    }

    private long term = -1;
    private MemberId from;
    private MemberId candidate;
    private long lastLogIndex;
    private long lastLogTerm;

    private final Constructor<T> constructor;

    public T build()
    {
        return constructor.construct( from, term, candidate, lastLogIndex, lastLogTerm );
    }

    public AnyVoteRequestBuilder<T> from( MemberId from )
    {
        this.from = from;
        return this;
    }

    public AnyVoteRequestBuilder<T> term( long term )
    {
        this.term = term;
        return this;
    }

    public AnyVoteRequestBuilder<T> candidate( MemberId candidate )
    {
        this.candidate = candidate;
        return this;
    }

    public AnyVoteRequestBuilder<T> lastLogIndex( long lastLogIndex )
    {
        this.lastLogIndex = lastLogIndex;
        return this;
    }

    public AnyVoteRequestBuilder<T> lastLogTerm( long lastLogTerm )
    {
        this.lastLogTerm = lastLogTerm;
        return this;
    }
}
