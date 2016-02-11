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

import org.neo4j.coreedge.raft.log.monitoring.RaftLogAppendIndexMonitor;
import org.neo4j.coreedge.raft.log.monitoring.RaftLogCommitIndexMonitor;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.kernel.monitoring.Monitors;

public class MonitoredRaftLog implements RaftLog
{
    public static final String APPEND_INDEX_TAG = "appendIndex";
    public static final String COMMIT_INDEX_TAG = "commitIndex";

    private final RaftLog delegate;
    private final RaftLogAppendIndexMonitor appendIndexMonitor;
    private final RaftLogCommitIndexMonitor commitIndexMonitor;

    public MonitoredRaftLog( RaftLog delegate, Monitors monitors )
    {
        this.delegate = delegate;
        this.appendIndexMonitor = monitors.newMonitor( RaftLogAppendIndexMonitor.class, getClass(), APPEND_INDEX_TAG );
        this.commitIndexMonitor = monitors.newMonitor( RaftLogCommitIndexMonitor.class, getClass(), COMMIT_INDEX_TAG );
    }

    @Override
    public long append( RaftLogEntry entry ) throws RaftStorageException
    {
        long appendIndex = delegate.append( entry );
        appendIndexMonitor.appendIndex( appendIndex );
        return appendIndex;
    }

    @Override
    public void truncate( long fromIndex ) throws RaftStorageException
    {
        delegate.truncate( fromIndex );
        appendIndexMonitor.appendIndex( delegate.appendIndex() );
    }

    @Override
    public void commit( long commitIndex ) throws RaftStorageException
    {
        delegate.commit( commitIndex );
        commitIndexMonitor.commitIndex( delegate.commitIndex() );
    }

    @Override
    public long appendIndex()
    {
        return delegate.appendIndex();
    }

    @Override
    public long commitIndex()
    {
        return delegate.commitIndex();
    }

    @Override
    public RaftLogEntry readLogEntry( long logIndex ) throws RaftStorageException
    {
        return delegate.readLogEntry( logIndex );
    }

    @Override
    public ReplicatedContent readEntryContent( long logIndex ) throws RaftStorageException
    {
        return delegate.readEntryContent( logIndex );
    }

    @Override
    public long readEntryTerm( long logIndex ) throws RaftStorageException
    {
        return delegate.readEntryTerm( logIndex );
    }

    @Override
    public boolean entryExists( long logIndex )
    {
        return delegate.entryExists( logIndex );
    }
}
