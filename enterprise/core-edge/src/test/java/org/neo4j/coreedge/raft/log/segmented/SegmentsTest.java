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

import org.junit.Test;

import java.io.File;
import java.util.Collections;

import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.state.ChannelMarshal;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.LogProvider;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class SegmentsTest
{
    @Test
    public void shouldCreateNext() throws Exception
    {
        // Given
        FileSystemAbstraction fsa = mock( FileSystemAbstraction.class, RETURNS_MOCKS );
        FileNames fileNames = mock( FileNames.class );
        ChannelMarshal<ReplicatedContent> contentMarshal = mock( ChannelMarshal.class );
        LogProvider logProvider = mock( LogProvider.class );

        Segments segments = new Segments( fsa, fileNames, Collections.emptyList(), contentMarshal, logProvider, -1 );

        // When
        segments.rotate( 10, 10, 12 );
        segments.last().closeWriter();
        SegmentFile last = segments.last();

        // Then
        assertEquals( 10, last.header().prevFileLastIndex() );
        assertEquals( 10, last.header().prevIndex() );
        assertEquals( 12, last.header().prevTerm() );
    }

    @Test
    public void shouldDeleteOnPrune() throws Exception
    {
        // Given
        FileSystemAbstraction fsa = mock( FileSystemAbstraction.class, RETURNS_MOCKS );
        File baseDirectory = new File( "." );
        FileNames fileNames = new FileNames( baseDirectory );
        ChannelMarshal<ReplicatedContent> contentMarshal = mock( ChannelMarshal.class );
        LogProvider logProvider = mock( LogProvider.class );

        Segments segments = new Segments( fsa, fileNames, Collections.emptyList(), contentMarshal, logProvider, -1 );

        SegmentFile toPrune = segments.rotate( -1, -1, -1 ); // this is version 0 and will be deleted on prune later
        segments.last().closeWriter(); // need to close writer otherwise dispose will not be called
        segments.rotate( 10, 10, 2 );
        segments.last().closeWriter(); // ditto
        segments.rotate( 20, 20, 2 );

        // When
        segments.prune( 11 );

        verify( fsa, times( 1 ) ).deleteFile( fileNames.getForVersion( toPrune.header().version() ) );
    }

    @Test
    public void shouldNeverDeleteOnTruncate() throws Exception
    {
        // Given
        FileSystemAbstraction fsa = mock( FileSystemAbstraction.class, RETURNS_MOCKS );
        File baseDirectory = new File( "." );
        FileNames fileNames = new FileNames( baseDirectory );
        ChannelMarshal<ReplicatedContent> contentMarshal = mock( ChannelMarshal.class );
        LogProvider logProvider = mock( LogProvider.class );

        Segments segments = new Segments( fsa, fileNames, Collections.emptyList(), contentMarshal, logProvider, -1 );

        segments.rotate( -1, -1, -1 );
        segments.last().closeWriter(); // need to close writer otherwise dispose will not be called
        segments.rotate( 10, 10, 2 ); // we will truncate this whole file away
        segments.last().closeWriter();

        // When
        segments.truncate( 20, 9, 4 );

        // Then
        verify( fsa, times( 0 ) ).deleteFile( any() );
    }

    @Test
    public void shouldDeleteTruncatedFilesOnPrune() throws Exception
    {
        // Given
        FileSystemAbstraction fsa = mock( FileSystemAbstraction.class, RETURNS_MOCKS );
        File baseDirectory = new File( "." );
        FileNames fileNames = new FileNames( baseDirectory );
        ChannelMarshal<ReplicatedContent> contentMarshal = mock( ChannelMarshal.class );
        LogProvider logProvider = mock( LogProvider.class );

        Segments segments = new Segments( fsa, fileNames, Collections.emptyList(), contentMarshal, logProvider, -1 );

        SegmentFile toBePruned = segments.rotate( -1, -1, -1 );
        segments.last().closeWriter(); // need to close writer otherwise dispose will not be called
        SegmentFile toBeTruncated = segments.rotate( 10, 10, 2 );// we will truncate this whole file away
        segments.last().closeWriter();

        // When
        // We truncate a whole file
        segments.truncate( 20, 9, 4 );
        // And we prune all files before that file
        segments.prune( 10 );

        // Then
        // the truncate file is part of the deletes that happen while prunning
        verify( fsa, times( 1 ) ).deleteFile( fileNames.getForVersion( toBePruned.header().version() ) );
        verify( fsa, times( 1 ) ).deleteFile( fileNames.getForVersion( toBeTruncated.header().version() ) );
    }

    @Test
    public void shouldAllowOutOfBoundsPruneIndex() throws Exception
    {
        //Given a prune index of n, if the smallest value for a segment file is n+c, the pruning should not remove
        // any files and not result in a failure.

        // Given
        FileSystemAbstraction fsa = mock( FileSystemAbstraction.class, RETURNS_MOCKS );
        File baseDirectory = new File( "." );
        FileNames fileNames = new FileNames( baseDirectory );
        ChannelMarshal<ReplicatedContent> contentMarshal = mock( ChannelMarshal.class );
        LogProvider logProvider = mock( LogProvider.class );

        Segments segments = new Segments( fsa, fileNames, Collections.emptyList(), contentMarshal, logProvider, -1 );

        segments.rotate( -1, -1, -1 );
        segments.last().closeWriter(); // need to close writer otherwise dispose will not be called
        segments.rotate( 10, 10, 2 ); // we will truncate this whole file away
        segments.last().closeWriter();

        segments.prune( 11 );

        segments.rotate( 20, 20, 3 ); // we will truncate this whole file away
        segments.last().closeWriter();

        //when
        SegmentFile oldestNotDisposed = segments.prune( -1 );

        //then
        SegmentHeader header = oldestNotDisposed.header();
        assertEquals( 10, header.prevFileLastIndex() );
        assertEquals( 10, header.prevIndex() );
        assertEquals( 2, header.prevTerm() );
    }
}
