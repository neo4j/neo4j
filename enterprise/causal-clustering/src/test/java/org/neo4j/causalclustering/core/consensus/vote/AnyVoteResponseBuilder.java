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
package org.neo4j.causalclustering.core.consensus.vote;

import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.core.consensus.RaftMessages;

public abstract class AnyVoteResponseBuilder<T extends RaftMessages.AnyVote.Response>
{
    protected AnyVoteResponseBuilder( Constructor<T> constructor )
    {
        this.constructor = constructor;
    }

    @FunctionalInterface
    interface Constructor<T extends RaftMessages.AnyVote.Response>
    {
        T construct( MemberId from, long term, boolean voteGranted );
    }

    private boolean voteGranted;
    private long term = -1;
    private MemberId from;
    private final Constructor<T> constructor;

    public T build()
    {
        return constructor.construct( from, term, voteGranted );
    }

    public AnyVoteResponseBuilder<T> from( MemberId from )
    {
        this.from = from;
        return this;
    }

    public AnyVoteResponseBuilder<T> term( long term )
    {
        this.term = term;
        return this;
    }

    public AnyVoteResponseBuilder<T> grant()
    {
        this.voteGranted = true;
        return this;
    }

    public AnyVoteResponseBuilder<T> deny()
    {
        this.voteGranted = false;
        return this;
    }
}
