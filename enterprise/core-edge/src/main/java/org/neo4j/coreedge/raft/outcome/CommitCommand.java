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
package org.neo4j.coreedge.raft.outcome;

import java.util.Objects;

import org.neo4j.coreedge.raft.log.RaftLog;
import org.neo4j.coreedge.raft.log.RaftStorageException;

public class CommitCommand implements LogCommand
{
    private final long commitIndex;

    public CommitCommand( long commitIndex )
    {
        this.commitIndex = commitIndex;
    }

    @Override
    public void applyTo( RaftLog raftLog ) throws RaftStorageException
    {
        raftLog.commit( commitIndex );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }
        CommitCommand that = (CommitCommand) o;
        return commitIndex == that.commitIndex;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( commitIndex );
    }

    @Override
    public String toString()
    {
        return "CommitCommand{" +
                "commitIndex=" + commitIndex +
                '}';
    }
}
