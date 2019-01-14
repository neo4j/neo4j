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
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.causalclustering.core.consensus.log.DummyRaftableContentSerializer;
import org.neo4j.causalclustering.core.consensus.log.EntryRecord;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.cursor.IOCursor;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;
import org.neo4j.time.Clocks;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.causalclustering.core.consensus.ReplicatedString.valueOf;
import static org.neo4j.causalclustering.core.consensus.log.segmented.SegmentFile.create;

public class SegmentFileTest
{
    @Rule
    public final EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    private final File baseDir = new File( "raft-log" );
    private final FileNames fileNames = new FileNames( baseDir );
    private final DummyRaftableContentSerializer contentMarshal = new DummyRaftableContentSerializer();
    private final NullLogProvider logProvider = NullLogProvider.getInstance();
    private final SegmentHeader segmentHeader = new SegmentHeader( -1, 0, -1, -1 );

    // various constants used throughout tests
    private final RaftLogEntry entry1 = new RaftLogEntry( 30, valueOf( "contentA" ) );
    private final RaftLogEntry entry2 = new RaftLogEntry( 31, valueOf( "contentB" ) );
    private final RaftLogEntry entry3 = new RaftLogEntry( 32, valueOf( "contentC" ) );
    private final RaftLogEntry entry4 = new RaftLogEntry( 33, valueOf( "contentD" ) );
    private final int version = 0;

    private ReaderPool readerPool = spy( new ReaderPool( 0, logProvider, fileNames, fsRule.get(), Clocks.fakeClock() ) );

    @Before
    public void before()
    {
        fsRule.get().mkdirs( baseDir );
    }

    @Test
    public void shouldReportCorrectInitialValues() throws Exception
    {
        try ( SegmentFile segment = create( fsRule.get(), fileNames.getForVersion( 0 ), readerPool, version,
                contentMarshal, logProvider, segmentHeader ) )
        {
            assertEquals( 0, segment.header().version() );

            IOCursor<EntryRecord> cursor = segment.getCursor( 0 );
            assertFalse( cursor.next() );

            cursor.close();
        }
    }

    @Test
    public void shouldBeAbleToWriteAndRead() throws Exception
    {
        try ( SegmentFile segment = create( fsRule.get(), fileNames.getForVersion( 0 ), readerPool, 0, contentMarshal,
                logProvider, segmentHeader ) )
        {
            // given
            segment.write( 0, entry1 );
            segment.flush();

            // when
            IOCursor<EntryRecord> cursor = segment.getCursor( 0 );

            // then
            assertTrue( cursor.next() );
            assertEquals( entry1, cursor.get().logEntry() );

            cursor.close();
        }
    }

    @Test
    public void shouldBeAbleToReadFromOffset() throws Exception
    {
        try ( SegmentFile segment = create( fsRule.get(), fileNames.getForVersion( 0 ), readerPool, 0, contentMarshal,
                logProvider, segmentHeader ) )
        {
            // given
            segment.write( 0, entry1 );
            segment.write( 1, entry2 );
            segment.write( 2, entry3 );
            segment.write( 3, entry4 );
            segment.flush();

            // when
            IOCursor<EntryRecord> cursor = segment.getCursor( 2 );

            // then
            assertTrue( cursor.next() );
            assertEquals( entry3, cursor.get().logEntry() );

            cursor.close();
        }
    }

    @Test
    public void shouldBeAbleToRepeatedlyReadWrittenValues() throws Exception
    {
        try ( SegmentFile segment = create( fsRule.get(), fileNames.getForVersion( 0 ), readerPool, 0, contentMarshal,
                logProvider, segmentHeader ) )
        {
            // given
            segment.write( 0, entry1 );
            segment.write( 1, entry2 );
            segment.write( 2, entry3 );
            segment.flush();

            for ( int i = 0; i < 3; i++ )
            {
                // when
                IOCursor<EntryRecord> cursor = segment.getCursor( 0 );

                // then
                assertTrue( cursor.next() );
                assertEquals( entry1, cursor.get().logEntry() );
                assertTrue( cursor.next() );
                assertEquals( entry2, cursor.get().logEntry() );
                assertTrue( cursor.next() );
                assertEquals( entry3, cursor.get().logEntry() );
                assertFalse( cursor.next() );

                cursor.close();
            }
        }
    }

    @Test
    public void shouldBeAbleToCloseOnlyAfterWriterIsClosed() throws Exception
    {
        try ( SegmentFile segment = create( fsRule.get(), fileNames.getForVersion( 0 ), readerPool, 0, contentMarshal,
                logProvider, segmentHeader ) )
        {
            // given
            assertFalse( segment.tryClose() );

            // when
            segment.closeWriter();

            // then
            assertTrue( segment.tryClose() );
        }
    }

    @Test
    public void shouldCallDisposeHandlerAfterLastReaderIsClosed() throws Exception
    {
        try ( SegmentFile segment = create( fsRule.get(), fileNames.getForVersion( 0 ), readerPool, 0, contentMarshal,
                logProvider, segmentHeader ) )
        {
            // given
            IOCursor<EntryRecord> cursor0 = segment.getCursor( 0 );
            IOCursor<EntryRecord> cursor1 = segment.getCursor( 0 );

            // when
            segment.closeWriter();
            cursor0.close();

            // then
            assertFalse( segment.tryClose() );

            // when
            cursor1.close();

            // then
            assertTrue( segment.tryClose() );
        }
    }

    @Test
    public void shouldHandleReaderPastEndCorrectly() throws Exception
    {
        try ( SegmentFile segment = create( fsRule.get(), fileNames.getForVersion( 0 ), readerPool, 0, contentMarshal,
                logProvider, segmentHeader ) )
        {
            // given
            segment.write( 0, entry1 );
            segment.write( 1, entry2 );
            segment.flush();
            segment.closeWriter();

            IOCursor<EntryRecord> cursor = segment.getCursor( 3 );

            // then
            assertFalse( cursor.next() );

            // when
            cursor.close();

            // then
            assertTrue( segment.tryClose() );
        }
    }

    @Test
    public void shouldHaveIdempotentCloseMethods() throws Exception
    {
        // given
        SegmentFile segment =
                create( fsRule.get(), fileNames.getForVersion( 0 ), readerPool, 0, contentMarshal, logProvider,
                        segmentHeader );
        IOCursor<EntryRecord> cursor = segment.getCursor( 0 );

        // when
        segment.closeWriter();
        cursor.close();

        // then
        assertTrue( segment.tryClose() );
        segment.close();
        assertTrue( segment.tryClose() );
        segment.close();
    }

    @Test
    public void shouldCatchDoubleCloseReaderErrors() throws Exception
    {
        try ( SegmentFile segment = create( fsRule.get(), fileNames.getForVersion( 0 ), readerPool, 0, contentMarshal,
                logProvider, segmentHeader ) )
        {
            // given
            IOCursor<EntryRecord> cursor = segment.getCursor( 0 );

            cursor.close();
            cursor.close();
            fail( "Should have caught double close error" );
        }
        catch ( IllegalStateException e )
        {
            // expected
        }
    }

    @Test
    public void shouldNotReturnReaderExperiencingErrorToPool() throws Exception
    {
        // given
        StoreChannel channel = mock( StoreChannel.class );
        Reader reader = mock( Reader.class );
        ReaderPool readerPool = mock( ReaderPool.class );

        when( channel.read( any( ByteBuffer.class ) ) ).thenThrow( new IOException() );
        when( reader.channel() ).thenReturn( channel );
        when( readerPool.acquire( anyLong(), anyLong() ) ).thenReturn( reader );

        try ( SegmentFile segment = create( fsRule.get(), fileNames.getForVersion( 0 ), readerPool, 0, contentMarshal,
                logProvider, segmentHeader ) )
        {
            // given
            IOCursor<EntryRecord> cursor = segment.getCursor( 0 );

            try
            {
                cursor.next();
                fail();
            }
            catch ( IOException e )
            {
                // expected from mocking
            }

            // when
            cursor.close();

            // then
            verify( readerPool, never() ).release( reader );
            verify( reader ).close();
        }
    }

    @Test
    public void shouldPruneReaderPoolOnClose() throws Exception
    {
        try ( SegmentFile segment = create( fsRule.get(), fileNames.getForVersion( 0 ), readerPool, 0, contentMarshal,
                logProvider, segmentHeader ) )
        {
            segment.write( 0, entry1 );
            segment.flush();
            segment.closeWriter();

            IOCursor<EntryRecord> cursor = segment.getCursor( 0 );
            cursor.next();
            cursor.close();
        }

        verify( readerPool ).prune( 0 );
    }
}
