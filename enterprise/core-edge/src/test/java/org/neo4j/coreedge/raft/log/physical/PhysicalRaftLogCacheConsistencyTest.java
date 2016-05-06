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
package org.neo4j.coreedge.raft.log.physical;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

import java.io.File;

import org.junit.After;
import org.junit.Test;
import org.neo4j.coreedge.raft.ReplicatedInteger;
import org.neo4j.coreedge.raft.log.DummyRaftableContentSerializer;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.log.RaftLogMetadataCache;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLogProvider;

public class PhysicalRaftLogCacheConsistencyTest
{
    private LifeSupport life = new LifeSupport();
    private FileSystemAbstraction fileSystem;

    @After
    public void tearDown() throws Throwable
    {
        life.stop();
        life.shutdown();
    }

    private PhysicalRaftLog createRaftLog( long rotateAtSize, PhysicalRaftLogFile.Monitor logFileMonitor,
                                           RaftLogMetadataCache raftLogMetadataCache )
    {
        if ( fileSystem == null )
        {
            fileSystem = new EphemeralFileSystemAbstraction();
        }
        File directory = new File( "raft-log" );
        fileSystem.mkdir( directory );

        PhysicalRaftLog newRaftLog = new PhysicalRaftLog( fileSystem, directory, rotateAtSize, "1 files", 100, 10,
                logFileMonitor, new DummyRaftableContentSerializer(),  () -> mock( DatabaseHealth.class ),
                NullLogProvider.getInstance(), raftLogMetadataCache );
        life.add( newRaftLog );
        life.init();
        life.start();
        return newRaftLog;
    }

    @Test
    public void shouldUpdateMetaDataCacheWhenLogsArePruned() throws Exception
    {
        // Given
        int entryCount = 100;
        RaftLogMetadataCache metadataCache = new RaftLogMetadataCache( entryCount * 2 );

        PhysicalRaftLog log = createRaftLog( 100, mock( PhysicalRaftLogFile.Monitor.class ), metadataCache );

        // When
        for ( int i = 0; i < entryCount; i++ )
        {
            log.append( new RaftLogEntry( i, ReplicatedInteger.valueOf(i) ) );
        }

        long newPrevIndex = log.prune( 50L );

        // Then
        for ( int i = 0; i < newPrevIndex; i++ )
        {
            assertNull( metadataCache.getMetadata( i ) );
        }
        for ( long i = newPrevIndex; i < entryCount; i++ )
        {
            assertNotNull( metadataCache.getMetadata( i ) );
        }
    }

    @Test
    public void shouldUpdateMetaDataCacheWhenLogIsTruncated() throws Exception
    {
        // Given
        int entryCount = 100;
        RaftLogMetadataCache metadataCache = new RaftLogMetadataCache( entryCount * 2 );

        PhysicalRaftLog log = createRaftLog( 100, mock( PhysicalRaftLogFile.Monitor.class ), metadataCache );

        // When
        for ( long i = 0; i < entryCount; i++ )
        {
            log.append( new RaftLogEntry( i, ReplicatedInteger.valueOf( (int) i ) ) );
        }

        long truncateIndex = 50L;
        log.truncate( truncateIndex );

        // Then
        for ( long i = 0; i < truncateIndex; i++ )
        {
            assertNotNull( metadataCache.getMetadata( i ) );
        }

        for ( long i = truncateIndex; i < entryCount; i++ )
        {
            assertNull( metadataCache.getMetadata( i ) );
        }
    }
}
