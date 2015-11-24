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
package org.neo4j.coreedge.raft.log.debug;

import java.io.PrintStream;

import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.log.RaftStorageException;
import org.neo4j.coreedge.raft.log.ReadableRaftLog;

public class LogPrinter
{
    private final ReadableRaftLog raftLog;

    public LogPrinter( ReadableRaftLog raftLog )
    {
        this.raftLog = raftLog;
    }

    public void print( PrintStream out ) throws RaftStorageException
    {
        out.println( String.format( "%1$8s %2$5s  %3$2s %4$s", "Index", "Term", "C?", "Content"));
        for ( int i = 0; i <= raftLog.appendIndex(); i++ )
        {
            RaftLogEntry raftLogEntry = raftLog.readLogEntry( i );
            out.printf("%8d %5d  %s %s%n", i, raftLogEntry.term(), i <= raftLog.commitIndex() ? "Y" : "N", raftLogEntry.content());
        }
    }
}
