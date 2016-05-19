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
import java.util.ArrayList;
import java.util.List;

import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.state.ChannelMarshal;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.LogProvider;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class SegmentsTest
{
    private final FileSystemAbstraction fsa = mock( FileSystemAbstraction.class, RETURNS_MOCKS );
    private final File baseDirectory = new File( "." );
    private final FileNames fileNames = new FileNames( baseDirectory );
    @SuppressWarnings( "unchecked" )
    private final ChannelMarshal<ReplicatedContent> contentMarshal = mock( ChannelMarshal.class );
    private final LogProvider logProvider = mock( LogProvider.class, RETURNS_MOCKS );
    private final SegmentHeader header = mock( SegmentHeader.class );
    private final List<SegmentFile> segmentFiles = asList(
            new SegmentFile( fsa, fileNames.getForVersion( 0 ), contentMarshal, logProvider, header ),
            new SegmentFile( fsa, fileNames.getForVersion( 1 ), contentMarshal, logProvider, header ) );

    @Test
    public void shouldCreateNext() throws Exception
    {
        // Given
        try( Segments segments = new Segments( fsa, fileNames, segmentFiles, contentMarshal, logProvider, -1 ) )
        {
            // When
            segments.rotate( 10, 10, 12 );
            segments.last().closeWriter();
            SegmentFile last = segments.last();

            // Then
            assertEquals( 10, last.header().prevFileLastIndex() );
            assertEquals( 10, last.header().prevIndex() );
            assertEquals( 12, last.header().prevTerm() );
        }
    }

    @Test
    public void shouldDeleteOnPrune() throws Exception
    {
        verifyZeroInteractions( fsa );
        // Given
        try( Segments segments = new Segments( fsa, fileNames, segmentFiles, contentMarshal, logProvider, -1 ) )
        {
            SegmentFile toPrune = segments.rotate( -1, -1, -1 ); // this is version 0 and will be deleted on prune later
            segments.last().closeWriter(); // need to close writer otherwise dispose will not be called
            segments.rotate( 10, 10, 2 );
            segments.last().closeWriter(); // ditto
            segments.rotate( 20, 20, 2 );

            // When
            segments.prune( 11 );

            verify( fsa, times( segmentFiles.size() ) ).deleteFile( fileNames.getForVersion( toPrune.header().version() ) );
        }
    }

    @Test
    public void shouldNeverDeleteOnTruncate() throws Exception
    {
        // Given
        try( Segments segments = new Segments( fsa, fileNames, segmentFiles, contentMarshal, logProvider, -1 ) )
        {
            segments.rotate( -1, -1, -1 );
            segments.last().closeWriter(); // need to close writer otherwise dispose will not be called
            segments.rotate( 10, 10, 2 ); // we will truncate this whole file away
            segments.last().closeWriter();

            // When
            segments.truncate( 20, 9, 4 );

            // Then
            verify( fsa, times( 0 ) ).deleteFile( any() );
        }
    }

    @Test
    public void shouldDeleteTruncatedFilesOnPrune() throws Exception
    {
        // Given
        try( Segments segments = new Segments( fsa, fileNames, segmentFiles, contentMarshal, logProvider, -1 ) )
        {
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
            verify( fsa, times( segmentFiles.size() ) ).deleteFile(
                    fileNames.getForVersion( toBePruned.header().version() ) );
            verify( fsa, times( segmentFiles.size() ) ).deleteFile(
                    fileNames.getForVersion( toBeTruncated.header().version() ) );
        }
    }

    @Test
    public void shouldCloseTheSegments() throws Exception
    {
        // Given
        Segments segments = new Segments( fsa, fileNames, segmentFiles, contentMarshal, logProvider, -1 );

        // When
        segments.close();

        // Then
        segments.getSegmentFileIteratorAtEnd().forEachRemaining( segment -> assertTrue( segment.isDisposed() ) );
    }

    @Test
    public void shouldNotSwallowExceptionOnClose() throws Exception
    {
        // Given
        List<SegmentFile> segmentFiles = new ArrayList<>( this.segmentFiles.size() );
        for ( SegmentFile segmentFile: this.segmentFiles )
        {
            SegmentFile spy = spy( segmentFile );
            doThrow( new DisposedException() ).when( spy ).close();
            segmentFiles.add( spy );
        }
        Segments segments = new Segments( fsa, fileNames, segmentFiles, contentMarshal, logProvider, -1 );

        // When
        try
        {
            segments.close();
            fail( "should have thrown" );
        }
        catch ( DisposedException ex)
        {
            // Then
            Throwable[] suppressed = ex.getSuppressed();
            assertEquals( 1, suppressed.length );
            assertTrue( suppressed[0] instanceof DisposedException );
        }
    }

    @Test
    public void shouldAllowOutOfBoundsPruneIndex() throws Exception
    {
        //Given a prune index of n, if the smallest value for a segment file is n+c, the pruning should not remove
        // any files and not result in a failure.
        Segments segments = new Segments( fsa, fileNames, segmentFiles, contentMarshal, logProvider, -1 );

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
