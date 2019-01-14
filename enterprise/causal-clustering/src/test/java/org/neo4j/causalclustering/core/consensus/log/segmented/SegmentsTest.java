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
package org.neo4j.causalclustering.core.consensus.log.segmented;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Collections;
import java.util.List;

import org.neo4j.causalclustering.core.consensus.log.EntryRecord;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;
import org.neo4j.cursor.IOCursor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.time.Clocks;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.logging.NullLogProvider.getInstance;

public class SegmentsTest
{
    private final FileSystemAbstraction fsa = mock( FileSystemAbstraction.class, RETURNS_MOCKS );
    private final File baseDirectory = new File( "." );
    private final FileNames fileNames = new FileNames( baseDirectory );
    @SuppressWarnings( "unchecked" )
    private final ChannelMarshal<ReplicatedContent> contentMarshal = mock( ChannelMarshal.class );
    private final LogProvider logProvider = NullLogProvider.getInstance();
    private final SegmentHeader header = mock( SegmentHeader.class );
    private final ReaderPool readerPool = new ReaderPool( 0, getInstance(), fileNames, fsa,
            Clocks.fakeClock() );

    private final SegmentFile fileA = spy( new SegmentFile( fsa, fileNames.getForVersion( 0 ), readerPool, 0,
            contentMarshal, logProvider, header ) );
    private final SegmentFile fileB = spy( new SegmentFile( fsa, fileNames.getForVersion( 1 ), readerPool, 1,
            contentMarshal, logProvider, header ) );

    private final List<SegmentFile> segmentFiles = asList( fileA, fileB );

    @Before
    public void before()
    {
        when( fsa.deleteFile( any() ) ).thenReturn( true );
    }

    @Test
    public void shouldCreateNext() throws Exception
    {
        // Given
        try ( Segments segments = new Segments( fsa, fileNames, readerPool, segmentFiles, contentMarshal,
                logProvider, -1 ) )
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
        try ( Segments segments = new Segments( fsa, fileNames, readerPool, segmentFiles, contentMarshal,
                logProvider, -1 ) )
        {
            // this is version 0 and will be deleted on prune later
            SegmentFile toPrune = segments.rotate( -1, -1, -1 );
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
        try ( Segments segments = new Segments( fsa, fileNames, readerPool, segmentFiles, contentMarshal,
                logProvider, -1 ) )
        {
            segments.rotate( -1, -1, -1 );
            segments.last().closeWriter(); // need to close writer otherwise dispose will not be called
            segments.rotate( 10, 10, 2 ); // we will truncate this whole file away
            segments.last().closeWriter();

            // When
            segments.truncate( 20, 9, 4 );

            // Then
            verify( fsa, never() ).deleteFile( any() );
        }
    }

    @Test
    public void shouldDeleteTruncatedFilesOnPrune() throws Exception
    {
        // Given
        try ( Segments segments = new Segments( fsa, fileNames, readerPool, segmentFiles, contentMarshal,
                logProvider, -1 ) )
        {
            SegmentFile toBePruned = segments.rotate( -1, -1, -1 );
            segments.last().closeWriter(); // need to close writer otherwise dispose will not be called
            // we will truncate this whole file away
            SegmentFile toBeTruncated = segments.rotate( 10, 10, 2 );
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
        }
    }

    @Test
    public void shouldCloseTheSegments()
    {
        // Given
        Segments segments = new Segments( fsa, fileNames, readerPool, segmentFiles, contentMarshal, logProvider, -1 );

        // When
        segments.close();

        // Then
        for ( SegmentFile file : segmentFiles )
        {
            verify( file ).close();
        }
    }

    @Test
    public void shouldNotSwallowExceptionOnClose()
    {
        // Given
        doThrow( new RuntimeException() ).when( fileA ).close();
        doThrow( new RuntimeException() ).when( fileB ).close();

        Segments segments = new Segments( fsa, fileNames, readerPool, segmentFiles, contentMarshal, logProvider, -1 );

        // When
        try
        {
            segments.close();
            fail( "should have thrown" );
        }
        catch ( RuntimeException ex )
        {
            // Then
            Throwable[] suppressed = ex.getSuppressed();
            assertEquals( 1, suppressed.length );
            assertTrue( suppressed[0] instanceof RuntimeException );
        }
    }

    @Test
    public void shouldAllowOutOfBoundsPruneIndex() throws Exception
    {
        //Given a prune index of n, if the smallest value for a segment file is n+c, the pruning should not remove
        // any files and not result in a failure.
        Segments segments = new Segments( fsa, fileNames, readerPool, segmentFiles, contentMarshal, logProvider, -1 );

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

    @Test
    public void attemptsPruningUntilOpenFileIsFound() throws Exception
    {
        /**
         * prune stops attempting to prune files after finding one that is open.
         */

        // Given
        Segments segments = new Segments( fsa, fileNames, readerPool, Collections.emptyList(), contentMarshal, logProvider, -1 );

        /*
        create 0
        create 1
        create 2
        create 3

        closeWriter on all
        create reader on 1
        prune on 3

        only 0 should be deleted
         */

        segments.rotate( -1, -1, -1 );
        segments.last().closeWriter(); // need to close writer otherwise dispose will not be called

        segments.rotate( 10, 10, 2 ); // we will truncate this whole file away
        segments.last().closeWriter(); // need to close writer otherwise dispose will not be called
        IOCursor<EntryRecord> reader = segments.last().getCursor( 11 );

        segments.rotate( 20, 20, 3 ); // we will truncate this whole file away
        segments.last().closeWriter();

        segments.rotate( 30, 30, 4 ); // we will truncate this whole file away
        segments.last().closeWriter();

        segments.prune( 31 );

        //when
        OpenEndRangeMap.ValueRange<Long,SegmentFile> shouldBePruned = segments.getForIndex( 5 );
        OpenEndRangeMap.ValueRange<Long,SegmentFile> shouldNotBePruned = segments.getForIndex( 15 );
        OpenEndRangeMap.ValueRange<Long,SegmentFile> shouldAlsoNotBePruned = segments.getForIndex( 25 );

        //then
        assertFalse( shouldBePruned.value().isPresent() );
        assertTrue( shouldNotBePruned.value().isPresent() );
        assertTrue( shouldAlsoNotBePruned.value().isPresent() );

        //when
        reader.close();
        segments.prune( 31 );

        shouldBePruned = segments.getForIndex( 5 );
        shouldNotBePruned = segments.getForIndex( 15 );
        shouldAlsoNotBePruned = segments.getForIndex( 25 );

        //then
        assertFalse( shouldBePruned.value().isPresent() );
        assertFalse( shouldNotBePruned.value().isPresent() );
        assertFalse( shouldAlsoNotBePruned.value().isPresent() );
    }
}
