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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.neo4j.coreedge.raft.ReplicatedString.valueOf;
import static org.neo4j.coreedge.raft.log.segmented.SegmentFile.create;

import java.io.File;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.coreedge.raft.log.DummyRaftableContentSerializer;
import org.neo4j.coreedge.raft.log.EntryRecord;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.cursor.IOCursor;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

public class SegmentFileTest
{
    @Rule
    public final EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    private final File rafLogFile = new File( "raft-log.0" );
    private final DummyRaftableContentSerializer contentMarshal = new DummyRaftableContentSerializer();
    private final NullLogProvider logProvider = NullLogProvider.getInstance();
    private final SegmentHeader segmentHeader = new SegmentHeader( -1, 0, -1, -1 );

    // various constants used throughout tests
    private final RaftLogEntry entry1 = new RaftLogEntry( 30, valueOf( "contentA" ) );
    private final RaftLogEntry entry2 = new RaftLogEntry( 31, valueOf( "contentB" ) );
    private final RaftLogEntry entry3 = new RaftLogEntry( 32, valueOf( "contentC" ) );
    private final RaftLogEntry entry4 = new RaftLogEntry( 33, valueOf( "contentD" ) );

    @Test
    public void shouldReportCorrectInitialValues() throws Exception
    {
        try ( SegmentFile segment = create( fsRule.get(), rafLogFile, contentMarshal, logProvider, segmentHeader ) )
        {
            assertEquals( 0, segment.header().version() );
            assertFalse( segment.isDisposed() );

            IOCursor<EntryRecord> reader = segment.getReader( 0 );
            assertFalse( reader.next() );
        }
    }

    @Test
    public void shouldBeAbleToWriteAndRead() throws Exception
    {
        try ( SegmentFile segment = create( fsRule.get(), rafLogFile, contentMarshal, logProvider, segmentHeader ) )
        {
            // given
            segment.write( 0, entry1 );

            // when
            IOCursor<EntryRecord> reader = segment.getReader( 0 );

            // then
            assertTrue( reader.next() );
            assertEquals( entry1, reader.get().logEntry() );
        }
    }

    @Test
    public void shouldBeAbleToReadFromOffset() throws Exception
    {
        try ( SegmentFile segment = create( fsRule.get(), rafLogFile, contentMarshal, logProvider, segmentHeader ) )
        {
            // given
            segment.write( 0, entry1 );
            segment.write( 1, entry2 );
            segment.write( 2, entry3 );
            segment.write( 3, entry4 );

            // when
            IOCursor<EntryRecord> reader = segment.getReader( 2 );

            // then
            assertTrue( reader.next() );
            assertEquals( entry3, reader.get().logEntry() );
        }
    }

    @Test
    public void shouldBeAbleToRepeatedlyReadWrittenValues() throws Exception
    {
        try ( SegmentFile segment = create( fsRule.get(), rafLogFile, contentMarshal, logProvider, segmentHeader ) )
        {
            // given
            segment.write( 0, entry1 );
            segment.write( 1, entry2 );
            segment.write( 2, entry3 );

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
    public void shouldCallDisposeHandler() throws Exception
    {
        try ( SegmentFile segment = create( fsRule.get(), rafLogFile, contentMarshal, logProvider, segmentHeader ) )
        {
            // given
            Runnable onDisposeHandler = mock( Runnable.class );

            // when
            segment.closeWriter();
            segment.markForDisposal( onDisposeHandler );

            // then
            verify( onDisposeHandler ).run();
        }
    }

    @Test
    public void shouldCallDisposeHandlerAfterWriterIsClosed() throws Exception
    {
        try ( SegmentFile segment = create( fsRule.get(), rafLogFile, contentMarshal, logProvider, segmentHeader ) )
        {
            // given
            Runnable onDisposeHandler = mock( Runnable.class );
            segment.write( 0, entry1 );

            // when
            segment.markForDisposal( onDisposeHandler );

            // then
            verify( onDisposeHandler, never() ).run();

            // when
            segment.closeWriter();

            // then
            assertTrue( segment.isDisposed() );
            verify( onDisposeHandler ).run();
        }
    }

    @Test
    public void shouldCallDisposeHandlerAfterLastReaderIsClosed() throws Exception
    {
        try ( SegmentFile segment = create( fsRule.get(), rafLogFile, contentMarshal, logProvider, segmentHeader ) )
        {
            // given
            Runnable onDisposeHandler = mock( Runnable.class );
            IOCursor<EntryRecord> reader0 = segment.getReader( 0 );
            IOCursor<EntryRecord> reader1 = segment.getReader( 1 );

            // when
            segment.closeWriter();
            segment.markForDisposal( onDisposeHandler );
            reader0.close();

            // then
            verify( onDisposeHandler, never() ).run();

            // when
            reader1.close();

            // then
            assertTrue( segment.isDisposed() );
            verify( onDisposeHandler ).run();
        }
    }

    @Test
    public void shouldCallDisposeHandlerAfterBothReadersAndWriterAreClosed() throws Exception
    {
        try ( SegmentFile segment = create( fsRule.get(), rafLogFile, contentMarshal, logProvider, segmentHeader ) )
        {
            // given
            Runnable onDisposeHandler = mock( Runnable.class );
            IOCursor<EntryRecord> reader0 = segment.getReader( 0 );
            IOCursor<EntryRecord> reader1 = segment.getReader( 1 );
            IOCursor<EntryRecord> reader2 = segment.getReader( 1 );

            segment.write( 0, entry1 );

            // when
            segment.markForDisposal( onDisposeHandler );
            reader0.close();
            reader1.close();
            segment.closeWriter();

            // then
            verify( onDisposeHandler, never() ).run();

            // when
            reader2.close();

            // then
            assertTrue( segment.isDisposed() );
            verify( onDisposeHandler ).run();
        }
    }

    @Test
    public void shouldDisposePoolAndWriterOnClose() throws Exception
    {
        SegmentFile segment = create( fsRule.get(), rafLogFile, contentMarshal, logProvider, segmentHeader );
        segment.close();

        try
        {
            segment.getReader( 0 );
            fail( "should have thrown" );
        }
        catch ( DisposedException ex )
        {
            // good!
        }

        try
        {
            segment.writer();
            fail( "should have thrown" );
        }
        catch ( DisposedException ex )
        {
            // good!
        }
    }
}
