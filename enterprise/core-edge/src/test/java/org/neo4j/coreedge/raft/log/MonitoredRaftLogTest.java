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

import org.junit.Test;

import java.io.File;

import org.neo4j.coreedge.raft.ReplicatedInteger;
import org.neo4j.coreedge.raft.log.monitoring.RaftLogAppendIndexMonitor;
import org.neo4j.coreedge.raft.log.monitoring.RaftLogCommitIndexMonitor;
import org.neo4j.coreedge.raft.log.naive.NaiveDurableRaftLog;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertEquals;

public class MonitoredRaftLogTest
{
    @Test
    public void shouldMonitorAppendIndexAndCommitIndex() throws Exception
    {
        // Given
        FileSystemAbstraction fsa = new EphemeralFileSystemAbstraction();

        File directory = new File( "." );
        fsa.create( directory );

        Monitors monitors = new Monitors();
        StubRaftLogAppendIndexMonitor appendMonitor = new StubRaftLogAppendIndexMonitor();
        monitors.addMonitorListener( appendMonitor );

        StubRaftLogCommitIndexMonitor commitMonitor = new StubRaftLogCommitIndexMonitor();
        monitors.addMonitorListener( commitMonitor );

        MonitoredRaftLog log = new MonitoredRaftLog(
                new NaiveDurableRaftLog( fsa, directory, new DummyRaftableContentSerializer(),
                        NullLogProvider.getInstance() ), monitors );

        // When
        log.append( new RaftLogEntry( 0, ReplicatedInteger.valueOf( 1 ) ) );
        log.append( new RaftLogEntry( 0, ReplicatedInteger.valueOf( 1 ) ) );

        assertEquals( 1, appendMonitor.appendIndex() );
        assertEquals( 0, commitMonitor.commitIndex() );

        log.truncate( 1 );
        assertEquals( 0, appendMonitor.appendIndex() );
    }

    private static class StubRaftLogCommitIndexMonitor implements RaftLogCommitIndexMonitor
    {
        private long commitIndex;

        @Override
        public long commitIndex()
        {
            return commitIndex;
        }

        @Override
        public void commitIndex( long commitIndex )
        {
            this.commitIndex = commitIndex;
        }
    }

    private static class StubRaftLogAppendIndexMonitor implements RaftLogAppendIndexMonitor
    {
        private long appendIndex;

        @Override
        public long appendIndex()
        {
            return appendIndex;
        }

        @Override
        public void appendIndex( long appendIndex )
        {
            this.appendIndex = appendIndex;
        }
    }
}
