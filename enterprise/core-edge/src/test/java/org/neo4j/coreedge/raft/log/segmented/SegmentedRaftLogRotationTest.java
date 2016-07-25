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
package org.neo4j.coreedge.raft.log.segmented;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.coreedge.raft.ReplicatedInteger;
import org.neo4j.coreedge.raft.ReplicatedString;
import org.neo4j.coreedge.raft.log.DummyRaftableContentSerializer;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.Resources;
import org.neo4j.time.FakeClock;

import static org.junit.Assert.assertEquals;
import static org.neo4j.coreedge.server.CoreEdgeClusterSettings.raft_log_pruning_strategy;
import static org.neo4j.test.rule.Resources.InitialLifecycle.STARTED;
import static org.neo4j.test.rule.Resources.TestPath.EXISTING_DIRECTORY;

public class SegmentedRaftLogRotationTest
{
    private static final int ROTATE_AT_SIZE_IN_BYTES = 100;

    @Rule
    public final Resources resourceManager = new Resources( EXISTING_DIRECTORY );

    private SegmentedRaftLog createRaftLog( long rotateAtSize ) throws IOException
    {
        return new SegmentedRaftLog( resourceManager.fileSystem(), resourceManager.testPath(), rotateAtSize,
                new DummyRaftableContentSerializer(), NullLogProvider.getInstance(),
                raft_log_pruning_strategy.getDefaultValue(), 0, new FakeClock() );
    }

    @Test
    @Resources.Life( STARTED )
    public void shouldRotateOnAppendWhenRotateSizeIsReached() throws Exception
    {
        // When
        SegmentedRaftLog log = resourceManager.managed( createRaftLog( ROTATE_AT_SIZE_IN_BYTES ) );
        log.append( new RaftLogEntry( 0, replicatedStringOfBytes( ROTATE_AT_SIZE_IN_BYTES ) ) );

        // Then
        File[] files = resourceManager.fileSystem().listFiles( resourceManager.testPath() );
        assertEquals( 2, files.length );
    }

    @Test
    @Resources.Life( STARTED )
    public void shouldBeAbleToRecoverToLatestStateAfterRotation() throws Throwable
    {
        // Given
        int term = 0;
        long indexToRestoreTo;
        try ( Lifespan lifespan = new Lifespan() )
        {
            SegmentedRaftLog log = lifespan.add( createRaftLog( ROTATE_AT_SIZE_IN_BYTES ) );
            log.append( new RaftLogEntry( term, replicatedStringOfBytes( ROTATE_AT_SIZE_IN_BYTES - 40 ) ) );
            indexToRestoreTo = log.append( new RaftLogEntry( term, ReplicatedInteger.valueOf( 1 ) ) );
        }

        // When
        SegmentedRaftLog log = resourceManager.managed( createRaftLog( ROTATE_AT_SIZE_IN_BYTES ) );

        // Then
        assertEquals( indexToRestoreTo, log.appendIndex() );
        assertEquals( term, log.readEntryTerm( indexToRestoreTo ) );
    }

    private ReplicatedString replicatedStringOfBytes( int size )
    {
        StringBuilder builder = new StringBuilder();
        for ( int i = 0; i < size; i++ )
        {
            builder.append( "i" );
        }
        return new ReplicatedString( builder.toString() );
    }
}
