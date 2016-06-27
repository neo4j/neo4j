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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.coreedge.raft.log.DummyRaftableContentSerializer;
import org.neo4j.coreedge.raft.log.EntryRecord;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.cursor.IOCursor;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;
import org.neo4j.time.FakeClock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.coreedge.raft.ReplicatedString.valueOf;
import static org.neo4j.coreedge.raft.log.segmented.SegmentFile.create;

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

    private ReaderPool readerPool = new ReaderPool( 0, logProvider, fileNames, fsRule.get(), new FakeClock() );

    @Before
    public void before()
    {
        fsRule.get().mkdirs( baseDir );
    }

    @Test
    public void shouldReportCorrectInitialValues() throws Exception
    {
        try ( SegmentFile segment = create( fsRule.get(), fileNames.getForVersion( 0 ), readerPool, version, contentMarshal, logProvider, segmentHeader ) )
        {
            assertEquals( 0, segment.header().version() );

            IOCursor<EntryRecord> reader = segment.getReader( 0 );
            assertFalse( reader.next() );

            reader.close();
        }
    }

    @Test
    public void shouldBeAbleToWriteAndRead() throws Exception
    {
        try ( SegmentFile segment = create( fsRule.get(), fileNames.getForVersion( 0 ), readerPool, 0, contentMarshal, logProvider, segmentHeader ) )
        {
            // given
            segment.write( 0, entry1 );
            segment.flush();

            // when
            IOCursor<EntryRecord> reader = segment.getReader( 0 );

            // then
            assertTrue( reader.next() );
            assertEquals( entry1, reader.get().logEntry() );

            reader.close();
        }
    }

    @Test
    public void shouldBeAbleToReadFromOffset() throws Exception
    {
        try ( SegmentFile segment = create( fsRule.get(), fileNames.getForVersion( 0 ), readerPool, 0, contentMarshal, logProvider, segmentHeader ) )
        {
            // given
            segment.write( 0, entry1 );
            segment.write( 1, entry2 );
            segment.write( 2, entry3 );
            segment.write( 3, entry4 );
            segment.flush();

            // when
            IOCursor<EntryRecord> reader = segment.getReader( 2 );

            // then
            assertTrue( reader.next() );
            assertEquals( entry3, reader.get().logEntry() );

            reader.close();
        }
    }

    @Test
    public void shouldBeAbleToRepeatedlyReadWrittenValues() throws Exception
    {
        try ( SegmentFile segment = create( fsRule.get(), fileNames.getForVersion( 0 ), readerPool, 0, contentMarshal, logProvider, segmentHeader ) )
        {
            // given
            segment.write( 0, entry1 );
            segment.write( 1, entry2 );
            segment.write( 2, entry3 );
            segment.flush();

            for ( int i = 0; i < 3; i++ )
            {
                // when
                IOCursor<EntryRecord> reader = segment.getReader( 0 );

                // then
                assertTrue( reader.next() );
                assertEquals( entry1, reader.get().logEntry() );
                assertTrue( reader.next() );
                assertEquals( entry2, reader.get().logEntry() );
                assertTrue( reader.next() );
                assertEquals( entry3, reader.get().logEntry() );
                assertFalse( reader.next() );

                reader.close();
            }
        }
    }

    @Test
    public void shouldBeAbleToCloseOnlyAfterWriterIsClosed() throws Exception
    {
        try ( SegmentFile segment = create( fsRule.get(), fileNames.getForVersion( 0 ), readerPool, 0, contentMarshal, logProvider, segmentHeader ) )
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
        try ( SegmentFile segment = create( fsRule.get(), fileNames.getForVersion( 0 ), readerPool, 0, contentMarshal, logProvider, segmentHeader ) )
        {
            // given
            IOCursor<EntryRecord> reader0 = segment.getReader( 0 );
            IOCursor<EntryRecord> reader1 = segment.getReader( 1 );

            // when
            segment.closeWriter();
            reader0.close();

            // then
            assertFalse( segment.tryClose() );

            // when
            reader1.close();

            // then
            assertTrue( segment.tryClose() );
        }
    }

    @Test
    public void shouldHaveIdempotentCloseMethods() throws Exception
    {
        // given
        SegmentFile segment = create( fsRule.get(), fileNames.getForVersion( 0 ), readerPool, 0, contentMarshal, logProvider, segmentHeader );
        IOCursor<EntryRecord> reader = segment.getReader( 0 );

        // when
        segment.closeWriter();
        reader.close();

        // then
        assertTrue( segment.tryClose() );
        segment.close();
        assertTrue( segment.tryClose() );
        segment.close();
    }

    @Test
    public void shouldCatchDoubleCloseReaderErrors() throws Exception
    {
        try ( SegmentFile segment = create( fsRule.get(), fileNames.getForVersion( 0 ), readerPool, 0, contentMarshal, logProvider, segmentHeader ) )
        {
            // given
            IOCursor<EntryRecord> reader = segment.getReader( 0 );

            reader.close();
            reader.close();
            fail( "Should have caught double close error" );
        }
        catch ( IllegalStateException e )
        {
            // expected
        }
    }
}
