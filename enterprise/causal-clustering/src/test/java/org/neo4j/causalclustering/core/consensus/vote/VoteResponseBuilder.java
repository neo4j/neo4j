/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.core.consensus.vote;

import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.core.consensus.RaftMessages;

public class VoteResponseBuilder
{
    boolean voteGranted = false;
    private long term = -1;
    private MemberId from = null;

    public RaftMessages.Vote.Response build()
    {
        return new RaftMessages.Vote.Response( from, term, voteGranted );
    }

    public VoteResponseBuilder from( MemberId from )
    {
        this.from = from;
        return this;
    }

    public VoteResponseBuilder term( long term )
    {
        this.term = term;
        return this;
    }

    public VoteResponseBuilder grant()
    {
        this.voteGranted = true;
        return this;
    }

    public VoteResponseBuilder deny()
    {
        this.voteGranted = false;
        return this;
    }
}
