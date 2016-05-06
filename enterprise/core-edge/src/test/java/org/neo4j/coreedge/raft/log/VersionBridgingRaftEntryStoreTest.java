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

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.coreedge.raft.ReplicatedInteger.valueOf;
import static org.neo4j.kernel.impl.util.IOCursors.cursor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;
import org.neo4j.coreedge.raft.log.physical.RaftLogAppendRecord;
import org.neo4j.coreedge.raft.log.physical.SingleVersionReader;
import org.neo4j.coreedge.raft.log.physical.VersionBridgingRaftEntryStore;
import org.neo4j.coreedge.raft.log.physical.VersionIndexRanges;
import org.neo4j.cursor.IOCursor;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;

public class VersionBridgingRaftEntryStoreTest
{
    RaftLogAppendRecord entry0 = new RaftLogAppendRecord( 0, new RaftLogEntry( 10, valueOf( 100 ) ) );
    RaftLogAppendRecord entry1 = new RaftLogAppendRecord( 1, new RaftLogEntry( 10, valueOf( 101 ) ) );
    RaftLogAppendRecord entry2 = new RaftLogAppendRecord( 2, new RaftLogEntry( 10, valueOf( 102 ) ) );
    RaftLogAppendRecord entry3 = new RaftLogAppendRecord( 3, new RaftLogEntry( 10, valueOf( 103 ) ) );
    RaftLogAppendRecord entry4 = new RaftLogAppendRecord( 4, new RaftLogEntry( 11, valueOf( 104 ) ) );
    RaftLogAppendRecord entry5 = new RaftLogAppendRecord( 5, new RaftLogEntry( 11, valueOf( 105 ) ) );
    RaftLogAppendRecord entry6 = new RaftLogAppendRecord( 6, new RaftLogEntry( 12, valueOf( 106 ) ) );
    RaftLogAppendRecord entry7 = new RaftLogAppendRecord( 7, new RaftLogEntry( 12, valueOf( 107 ) ) );
    RaftLogAppendRecord entry8 = new RaftLogAppendRecord( 8, new RaftLogEntry( 12, valueOf( 108 ) ) );

    private static LogPosition positionAtBeginningOfVersion( long version )
    {
        return new LogPosition( version, LogHeader.LOG_HEADER_SIZE );
    }

    @Test
    public void shouldReadEntriesFromSingleVersion() throws Exception
    {
        // given
        SingleVersionReader versionReader = mock( SingleVersionReader.class );

        when( versionReader.readEntriesFrom( positionAtBeginningOfVersion( 0 ) ) ).thenReturn( cursor( entry0, entry1, entry2, entry3 ) );

        VersionIndexRanges ranges = new VersionIndexRanges();
        ranges.add( 0, -1 );

        VersionBridgingRaftEntryStore store = new VersionBridgingRaftEntryStore( ranges, versionReader,
                mock( RaftLogMetadataCache.class ) );

        // when
        try ( IOCursor<RaftLogAppendRecord> cursor = store.getEntriesFrom( 0 ) )
        {
            // then
            assertThat( allItems( cursor ), consistsOf( entry0, entry1, entry2, entry3 ) );
        }
    }

    @Test
    public void shouldReadEntriesFromMiddleOfFile() throws Exception
    {
        // given
        SingleVersionReader versionReader = mock( SingleVersionReader.class );

        when( versionReader.readEntriesFrom( positionAtBeginningOfVersion( 0 ) ) ).thenReturn( cursor( entry0, entry1, entry2, entry3 ) );

        VersionIndexRanges ranges = new VersionIndexRanges();
        ranges.add( 0, -1 );

        VersionBridgingRaftEntryStore store = new VersionBridgingRaftEntryStore( ranges, versionReader, mock( RaftLogMetadataCache.class ) );

        // when
        try ( IOCursor<RaftLogAppendRecord> cursor = store.getEntriesFrom( 2 ) )
        {
            // then
            assertThat( allItems( cursor ), consistsOf( entry2, entry3 ) );
        }
    }

    @Test
    public void shouldReadAcrossVersionBoundary() throws Exception
    {
        // given
        SingleVersionReader versionReader = mock( SingleVersionReader.class );

        RaftLogAppendRecord truncatedEntry4 = new RaftLogAppendRecord( 3, new RaftLogEntry( 10, valueOf( -1 ) ) );
        when( versionReader.readEntriesFrom( positionAtBeginningOfVersion( 0 ) ) ).thenReturn(
                cursor( entry0, entry1, entry2, entry3, truncatedEntry4 ) );
        when( versionReader.readEntriesFrom( positionAtBeginningOfVersion( 2 ) ) ).thenReturn(
                cursor( entry4, entry5 ) );
        when( versionReader.readEntriesFrom( positionAtBeginningOfVersion( 3 ) ) ).thenReturn(
                cursor( entry6, entry7, entry8 ) );

        VersionIndexRanges ranges = new VersionIndexRanges();
        ranges.add( 0, -1 );
        ranges.add( 2, 3 );
        ranges.add( 3, 5 );

        VersionBridgingRaftEntryStore store = new VersionBridgingRaftEntryStore( ranges, versionReader, mock( RaftLogMetadataCache.class ) );

        // when
        try ( IOCursor<RaftLogAppendRecord> cursor = store.getEntriesFrom( 0 ) )
        {
            // then
            assertThat( allItems( cursor ),
                    consistsOf( entry0, entry1, entry2, entry3, entry4, entry5, entry6, entry7, entry8 ) );
        }
    }

    @Test
    public void shouldCloseUnderlyingCursors() throws Exception
    {
        // given
        SingleVersionReader versionReader = mock( SingleVersionReader.class );

        VersionIndexRanges ranges = new VersionIndexRanges();
        List<StubCursor> cursors = new ArrayList<>();
        for ( int i = 0; i < 10; i++ )
        {
            ranges.add( i, i * 2 - 1 );
            StubCursor versionCursor = new StubCursor( cursor(
                    new RaftLogAppendRecord( i * 2, new RaftLogEntry( i, valueOf( i * 10 ) ) ),
                    new RaftLogAppendRecord( i * 2 + 1, new RaftLogEntry( i, valueOf( i * 10 ) ) )
            ) );
            cursors.add( versionCursor );
            when( versionReader.readEntriesFrom( positionAtBeginningOfVersion( i ) ) ).thenReturn( versionCursor );
        }

        VersionBridgingRaftEntryStore store = new VersionBridgingRaftEntryStore( ranges, versionReader, mock( RaftLogMetadataCache.class ) );

        IOCursor<RaftLogAppendRecord> cursor = store.getEntriesFrom( 0 );
        allItems( cursor );

        // when
        cursor.close();

        // then
        cursors.forEach( (versionCursor) -> assertTrue( versionCursor.isClosed() ) );
    }

    @Test
    public void shouldReadNoEntriesFromEmptyLog() throws Exception
    {
        // given
        SingleVersionReader versionReader = mock( SingleVersionReader.class );

        VersionIndexRanges ranges = new VersionIndexRanges(); // empty

        VersionBridgingRaftEntryStore store = new VersionBridgingRaftEntryStore( ranges, versionReader, mock( RaftLogMetadataCache.class ) );

        // when
        try ( IOCursor<RaftLogAppendRecord> cursor = store.getEntriesFrom( 0 ) )
        {
            // then
            assertThat( allItems( cursor ), empty() );
        }
    }

    @Test
    public void shouldUsePositionCacheIfItContainsTargetEntry() throws Exception
    {
        // given
        SingleVersionReader versionReader = mock( SingleVersionReader.class, RETURNS_MOCKS );
        VersionIndexRanges ranges = new VersionIndexRanges(); // empty
        int currentVersion = 4;
        ranges.add( currentVersion, 9 ); // log version 4 contains everything from 9 forward
        RaftLogMetadataCache raftLogMetadataCache = new RaftLogMetadataCache( 3 );
        // and a cache that has metadata for the entry we are after
        LogPosition thePosition = new LogPosition( currentVersion, 128 );
        int cachedIndex = 10;
        raftLogMetadataCache.cacheMetadata( cachedIndex, 30, thePosition );

        VersionBridgingRaftEntryStore store = new VersionBridgingRaftEntryStore( ranges, versionReader, raftLogMetadataCache );

        // when
        // we ask the store for an entry at an index that has its position cached
        IOCursor<RaftLogAppendRecord> entriesFrom = store.getEntriesFrom( cachedIndex );
        // and we ask the cursor to actually read the thing
        entriesFrom.next();

        // then
        verify( versionReader, times( 1 ) ).readEntriesFrom( thePosition );
    }

    @Test
    public void shouldReturnEmtpyCursorIfRequestedIndexIsNotInRange() throws Exception
    {
        // given
        SingleVersionReader versionReader = mock( SingleVersionReader.class, RETURNS_MOCKS );
        VersionIndexRanges ranges = new VersionIndexRanges(); // empty
        ranges.add( 4, 11 ); // log version 4 contains everything from 11 forward
        RaftLogMetadataCache raftLogMetadataCache = new RaftLogMetadataCache( 3 );
        // and a cache that has metadata for the entry we are after
        long requestedIndex = 10;
        VersionBridgingRaftEntryStore store = new VersionBridgingRaftEntryStore( ranges, versionReader, raftLogMetadataCache );

        // when
        // we ask the store for an entry at an index that is not present in the ranges
        IOCursor<RaftLogAppendRecord> entriesFrom = store.getEntriesFrom( requestedIndex );

        // then
        // the returned cursor should be empty
        assertFalse( entriesFrom.next() );
    }

    @Test
    public void shouldCreateCursorPositionedAfterHeaderIfTheEntryHasNoMetadataCached() throws Exception
    {
        // given
        SingleVersionReader versionReader = mock( SingleVersionReader.class, RETURNS_MOCKS );
        VersionIndexRanges ranges = new VersionIndexRanges(); // empty
        int currentVersion = 0;
        ranges.add( currentVersion, 1 ); // log version 0 contains everything from 1 forward
        RaftLogMetadataCache raftLogMetadataCache = new RaftLogMetadataCache( 3 );
        long requestedIndex = 10;

        VersionBridgingRaftEntryStore store = new VersionBridgingRaftEntryStore( ranges, versionReader,
                raftLogMetadataCache );

        // when
        // we ask the store for an entry at an index that has its position not present in the cache
        IOCursor<RaftLogAppendRecord> entriesFrom = store.getEntriesFrom( requestedIndex );
        // and we ask the cursor to actually read the thing
        entriesFrom.next();

        // then
        verify( versionReader, times( 1 ) ).readEntriesFrom( new LogPosition( currentVersion, LogHeader.LOG_HEADER_SIZE ) );
    }

    @SafeVarargs
    public static <T> Matcher<Iterable<T>> consistsOf(T... expected)
    {
        Matcher<Iterable<T>> hasItems = hasItems( expected );
        return new TypeSafeMatcher<Iterable<T>>()
        {
            @Override
            protected boolean matchesSafely( Iterable<T> actual )
            {
                return expected.length == Iterables.count( actual ) && hasItems.matches( actual );
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "A collection of " );
                description.appendValue( expected.length );
                description.appendText( " items, specifically: " );
                description.appendValue( asList( expected ) );
            }
        };
    }

    private <T> List<T> allItems( IOCursor<T> cursor ) throws IOException
    {
        LinkedList<T> list = new LinkedList<>();
        while ( cursor.next() )
        {
            list.add( cursor.get() );
        }
        return list;
    }

    private class StubCursor implements IOCursor<RaftLogAppendRecord>
    {
        private final IOCursor<RaftLogAppendRecord> inner;
        private boolean closed = false;

        private StubCursor( IOCursor<RaftLogAppendRecord> inner )
        {
            this.inner = inner;
        }

        @Override
        public boolean next() throws IOException
        {
            return inner.next();
        }

        @Override
        public void close() throws IOException
        {
            closed = true;
        }

        @Override
        public void forAll( Consumer<RaftLogAppendRecord> consumer ) throws IOException
        {
            inner.forAll( consumer );
        }

        @Override
        public RaftLogAppendRecord get()
        {
            return inner.get();
        }

        public boolean isClosed()
        {
            return closed;
        }
    }
}
