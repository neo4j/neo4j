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
package org.neo4j.coreedge.raft.log;

import java.io.IOException;

import org.neo4j.coreedge.raft.log.monitoring.RaftLogAppendIndexMonitor;
import org.neo4j.coreedge.raft.log.monitoring.RaftLogCommitIndexMonitor;
import org.neo4j.cursor.IOCursor;
import org.neo4j.kernel.monitoring.Monitors;

public class MonitoredRaftLog implements RaftLog
{
    private final RaftLog delegate;
    private final RaftLogAppendIndexMonitor appendIndexMonitor;

    public MonitoredRaftLog( RaftLog delegate, Monitors monitors )
    {
        this.delegate = delegate;
        this.appendIndexMonitor = monitors.newMonitor( RaftLogAppendIndexMonitor.class, getClass() );
    }

    @Override
    public long append( RaftLogEntry entry ) throws IOException
    {
        long appendIndex = delegate.append( entry );
        appendIndexMonitor.appendIndex( appendIndex );
        return appendIndex;
    }

    @Override
    public void truncate( long fromIndex ) throws IOException, RaftLogCompactedException
    {
        delegate.truncate( fromIndex );
        appendIndexMonitor.appendIndex( delegate.appendIndex() );
    }

    @Override
    public long prune( long safeIndex ) throws IOException, RaftLogCompactedException
    {
        return delegate.prune( safeIndex );
    }

    @Override
    public long appendIndex()
    {
        return delegate.appendIndex();
    }

    @Override
    public long prevIndex()
    {
        return delegate.prevIndex();
    }

    @Override
    public long readEntryTerm( long logIndex ) throws IOException, RaftLogCompactedException
    {
        return delegate.readEntryTerm( logIndex );
    }

    @Override
    public RaftLogCursor getEntryCursor( long fromIndex ) throws IOException, RaftLogCompactedException
    {
        return delegate.getEntryCursor( fromIndex );
    }

    @Override
    public long skip( long index, long term ) throws IOException
    {
        return delegate.skip( index, term );
    }
}
