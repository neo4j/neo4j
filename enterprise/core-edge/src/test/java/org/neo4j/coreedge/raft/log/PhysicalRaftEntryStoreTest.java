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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.junit.Test;
import org.neo4j.coreedge.raft.ReplicatedInteger;
import org.neo4j.cursor.IOCursor;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.LogFile;
import org.neo4j.kernel.impl.transaction.log.LogHeaderCache;
import org.neo4j.kernel.impl.transaction.log.LogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.logging.NullLogProvider;

public class PhysicalRaftEntryStoreTest
{
    @Test
    public void shouldReturnCursorProperlyPositionedIfThereIsACacheMiss() throws Throwable
    {
        // Given
        EphemeralFileSystemAbstraction fsa = new EphemeralFileSystemAbstraction();

        File baseDirectory = new File( "raft-logs" );
        fsa.mkdir( baseDirectory );
        PhysicalRaftLog log = new PhysicalRaftLog( fsa, baseDirectory, 10000, 1, 10, mock( PhysicalLogFile.Monitor.class),
                new DummyRaftableContentSerializer(), mock( Supplier.class ), NullLogProvider.getInstance() );
        AtomicLong appendIndex = new AtomicLong( 0 );

        log.init();
        log.start();

        for ( int i = 0; i < 5; i++ )
        {
            appendIndex.set( log.append( new RaftLogEntry( i, ReplicatedInteger.valueOf( i ) ) ) );
        }

        log.stop();
        log.shutdown();


        PhysicalLogFiles logFiles = new PhysicalLogFiles( baseDirectory, PhysicalRaftLog.BASE_FILE_NAME, fsa );
        LogVersionRepository repo = new FilenameBasedLogVersionRepository( logFiles );
        LogFile logFile = new PhysicalLogFile( fsa, logFiles, 100000, appendIndex::get, repo, mock( PhysicalLogFile.Monitor.class ), new LogHeaderCache( 1 ) );


        PhysicalRaftEntryStore theStore = new PhysicalRaftEntryStore( logFile, mock( RaftLogMetadataCache.class ),
                new DummyRaftableContentSerializer() );

        // when
        long indexToStartFrom = appendIndex.get() - 2;
        IOCursor<RaftLogAppendRecord> cursor = theStore.getEntriesFrom( indexToStartFrom );

        // then
        assertTrue( cursor.next() );
        assertEquals( indexToStartFrom, cursor.get().getLogIndex() );

    }
}
