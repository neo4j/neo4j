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
package org.neo4j.causalclustering.core.consensus.log.debug;

import java.io.IOException;
import java.io.PrintStream;

import org.neo4j.causalclustering.core.consensus.log.RaftLogCursor;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.consensus.log.ReadableRaftLog;

public class LogPrinter
{
    private final ReadableRaftLog raftLog;

    public LogPrinter( ReadableRaftLog raftLog )
    {
        this.raftLog = raftLog;
    }

    public void print( PrintStream out ) throws IOException
    {
        out.println( String.format( "%1$8s %2$5s  %3$2s %4$s", "Index", "Term", "C?", "Content"));
        long index = 0L;
        try ( RaftLogCursor cursor = raftLog.getEntryCursor( 0 ) )
        {
            while ( cursor.next() )
            {
                RaftLogEntry raftLogEntry = cursor.get();
                out.printf("%8d %5d %s", index, raftLogEntry.term(), raftLogEntry.content());
                index++;
            }
        }
    }
}
