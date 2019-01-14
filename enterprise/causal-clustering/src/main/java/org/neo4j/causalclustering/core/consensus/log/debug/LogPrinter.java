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
