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
package org.neo4j.coreedge.raft.log;

import java.util.Objects;

import org.neo4j.coreedge.raft.replication.ReplicatedContent;

public class RaftLogEntry
{

    public static final RaftLogEntry[] empty = new RaftLogEntry[0];
    private final long term;
    private final ReplicatedContent content;

    public RaftLogEntry( long term, ReplicatedContent content )
    {
        Objects.requireNonNull( content );
        this.term = term;
        this.content = content;
    }

    public long term()
    {
        return this.term;
    }

    public ReplicatedContent content()
    {
        return this.content;
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

        RaftLogEntry that = (RaftLogEntry) o;

        return term == that.term && content.equals( that.content );
    }

    @Override
    public int hashCode()
    {
        int result = (int) (term ^ (term >>> 32));
        result = 31 * result + content.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return String.format( "RaftLogEntry{term=%d, content=%s}", term, content );
    }
}
