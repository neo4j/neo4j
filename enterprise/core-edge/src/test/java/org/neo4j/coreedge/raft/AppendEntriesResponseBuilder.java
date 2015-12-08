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

public class AppendEntriesResponseBuilder<MEMBER>
{
    boolean success;
    private long term = -1;
    private MEMBER from = null;
    private long matchIndex = -1;
    private long appendIndex = -1;

    public RaftMessages.AppendEntries.Response<MEMBER> build()
    {
        // a response of false should always have a match index of -1
        assert !( success == false && matchIndex != -1 );
        return new RaftMessages.AppendEntries.Response<>( from, term, success, matchIndex, appendIndex );
    }

    public AppendEntriesResponseBuilder<MEMBER> from( MEMBER from )
    {
        this.from = from;
        return this;
    }

    public AppendEntriesResponseBuilder<MEMBER> term( long term )
    {
        this.term = term;
        return this;
    }

    public AppendEntriesResponseBuilder<MEMBER> matchIndex( long matchIndex )
    {
        this.matchIndex = matchIndex;
        return this;
    }

    public AppendEntriesResponseBuilder<MEMBER> appendIndex( long appendIndex )
    {
        this.appendIndex = appendIndex;
        return this;
    }

    public AppendEntriesResponseBuilder<MEMBER> success()
    {
        this.success = true;
        return this;
    }

    public AppendEntriesResponseBuilder<MEMBER> failure()
    {
        this.success = false;
        return this;
    }
}
