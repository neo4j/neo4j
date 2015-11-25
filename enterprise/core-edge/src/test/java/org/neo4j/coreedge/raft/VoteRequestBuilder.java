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
package org.neo4j.coreedge.raft;

public class VoteRequestBuilder<MEMBER>
{
    private long term = -1;
    private MEMBER from = null;
    private MEMBER candidate;
    private long lastLogIndex;
    private long lastLogTerm;

    public RaftMessages.Vote.Request<MEMBER> build()
    {
        return new RaftMessages.Vote.Request<>( from, term, candidate, lastLogIndex, lastLogTerm );
    }

    public VoteRequestBuilder<MEMBER> from( MEMBER from )
    {
        this.from = from;
        return this;
    }

    public VoteRequestBuilder<MEMBER> term( long term )
    {
        this.term = term;
        return this;
    }

    public VoteRequestBuilder<MEMBER> candidate( MEMBER candidate )
    {
        this.candidate = candidate;
        return this;
    }

    public VoteRequestBuilder<MEMBER> lastLogIndex( long lastLogIndex )
    {
        this.lastLogIndex = lastLogIndex;
        return this;
    }

    public VoteRequestBuilder<MEMBER> lastLogTerm( long lastLogTerm )
    {
        this.lastLogTerm = lastLogTerm;
        return this;
    }
}
