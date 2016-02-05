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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Test;
import org.neo4j.coreedge.raft.ReplicatedInteger;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLogProvider;

public class PhysicalRaftLogContractTest extends RaftLogContractTest
{
    private PhysicalRaftLog raftLog;
    private LifeSupport life = new LifeSupport();
    private FileSystemAbstraction fileSystem;

    @Override
    public RaftLog createRaftLog() throws IOException
    {
        this.raftLog = createRaftLog( 100 );
        return raftLog;
    }

    @After
    public void tearDown() throws Throwable
    {
        life.stop();
        life.shutdown();
    }

    private PhysicalRaftLog createRaftLog( int cacheSize )
    {
        if ( fileSystem == null )
        {
            fileSystem = new EphemeralFileSystemAbstraction();
        }
        File directory = new File( "raft-log" );
        fileSystem.mkdir( directory );

        PhysicalRaftLog newRaftLog = new PhysicalRaftLog( fileSystem, directory, 1000000, cacheSize,
                new PhysicalLogFile.Monitor.Adapter(), new DummyRaftableContentSerializer(), mock( Supplier.class ),
                NullLogProvider.getInstance() );
        life.add( newRaftLog );
        life.init();
        life.start();
        return newRaftLog;
    }

    @Test
    public void shouldReadBackInCachedEntry() throws Throwable
    {
        // Given
        PhysicalRaftLog raftLog = (PhysicalRaftLog) createRaftLog();
        int term = 0;
        ReplicatedInteger content = ReplicatedInteger.valueOf( 4 );

        // When
        long entryIndex = raftLog.append( new RaftLogEntry( term, content ) );

        // Then
        assertTrue( raftLog.entryExists( entryIndex ) );
        assertEquals( content, raftLog.readEntryContent( entryIndex ) );
        assertEquals( term, raftLog.readEntryTerm( entryIndex ) );
    }

    @Test
    public void shouldReadBackNonCachedEntry() throws Exception
    {
        // Given
        int cacheSize = 1;
        PhysicalRaftLog raftLog = createRaftLog( cacheSize );
        int term = 0;
        ReplicatedInteger content1 = ReplicatedInteger.valueOf( 4 );
        ReplicatedInteger content2 = ReplicatedInteger.valueOf( 5 );

        // When
        long entryIndex1 = raftLog.append( new RaftLogEntry( term, content1 ) );
        long entryIndex2 = raftLog.append( new RaftLogEntry( term, content2 ) ); // this will push the first entry out of cache

        // Then
        // entry 1 should be there
        assertTrue( raftLog.entryExists( entryIndex1 ) );
        assertEquals( content1, raftLog.readEntryContent( entryIndex1 ) );
        assertEquals( term, raftLog.readEntryTerm( entryIndex1 ) );

        // entry 2 should be there also
        assertTrue( raftLog.entryExists( entryIndex2 ) );
        assertEquals( content2, raftLog.readEntryContent( entryIndex2 ) );
        assertEquals( term, raftLog.readEntryTerm( entryIndex2 ) );
    }

    @Test
    public void shouldRestoreCommitIndexOnStartup() throws Throwable
    {
        // Given
        PhysicalRaftLog raftLog = createRaftLog( 100 /* cache size */  );
        int term = 0;
        ReplicatedInteger content1 = ReplicatedInteger.valueOf( 4 );
        ReplicatedInteger content2 = ReplicatedInteger.valueOf( 5 );
        long entryIndex1 = raftLog.append( new RaftLogEntry( term, content1 ) );
        long entryIndex2 = raftLog.append( new RaftLogEntry( term, content2 ) );

        raftLog.commit( entryIndex1 );

        // When
        // we restart the raft log
        life.remove( raftLog ); // stops the removed instance
        raftLog = createRaftLog( 100 );

        // Then
        assertEquals( entryIndex1, raftLog.commitIndex() );
        assertEquals( entryIndex2, raftLog.appendIndex() );
    }
}
