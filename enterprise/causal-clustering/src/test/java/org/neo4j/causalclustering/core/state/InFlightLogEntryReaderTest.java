/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.core.state;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.neo4j.causalclustering.core.consensus.log.RaftLogCursor;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.consensus.log.ReadableRaftLog;
import org.neo4j.causalclustering.core.consensus.log.segmented.InFlightMap;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

@RunWith( Parameterized.class )
public class InFlightLogEntryReaderTest
{
    private final ReadableRaftLog raftLog = mock( ReadableRaftLog.class );
    @SuppressWarnings( "unchecked" )
    private final InFlightMap<RaftLogEntry> inFlightMap = mock( InFlightMap.class );
    private final long logIndex = 42L;
    private final RaftLogEntry entry = mock( RaftLogEntry.class );

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Boolean[]> params()
    {
        return asList( new Boolean[]{true}, new Boolean[]{false} );
    }

    @Parameterized.Parameter( 0 )
    public boolean clearCache;

    @Test
    public void shouldUseTheCacheWhenTheIndexIsPresent() throws Exception
    {
        // given
        InFlightLogEntryReader reader = new InFlightLogEntryReader( raftLog, inFlightMap, clearCache );
        startingFromIndexReturnEntries( inFlightMap, logIndex, entry );
        startingFromIndexReturnEntries( raftLog, -1, null );

        // when
        RaftLogEntry raftLogEntry = reader.get( logIndex );

        // then
        assertEquals( entry, raftLogEntry );
        verify( inFlightMap ).get( logIndex );
        assertCacheIsUpdated( inFlightMap, logIndex );
        verifyNoMoreInteractions( inFlightMap );
        verifyZeroInteractions( raftLog );
    }

    @Test
    public void shouldUseTheRaftLogWhenTheIndexIsNotPresent() throws Exception
    {
        // given
        InFlightLogEntryReader reader = new InFlightLogEntryReader( raftLog, inFlightMap, clearCache );
        startingFromIndexReturnEntries( inFlightMap, logIndex, null );
        startingFromIndexReturnEntries( raftLog, logIndex, entry );

        // when
        RaftLogEntry raftLogEntry = reader.get( logIndex );

        // then
        assertEquals( entry, raftLogEntry );
        verify( inFlightMap ).get( logIndex );
        verify( raftLog ).getEntryCursor( logIndex );
        assertCacheIsUpdated( inFlightMap, logIndex );

        verifyNoMoreInteractions( inFlightMap );
        verifyNoMoreInteractions( raftLog );
    }

    @Test
    public void shouldNeverUseMapAgainAfterHavingFeltBackToTheRaftLog() throws Exception
    {
        // given
        InFlightLogEntryReader reader = new InFlightLogEntryReader( raftLog, inFlightMap, clearCache );
        startingFromIndexReturnEntries( inFlightMap, logIndex, entry, null, mock( RaftLogEntry.class ) );
        RaftLogEntry[] entries = {entry, mock( RaftLogEntry.class ), mock( RaftLogEntry.class )};
        startingFromIndexReturnEntries( raftLog, logIndex + 1, entries[1], entries[2] );

        for ( int offset = 0; offset < 3; offset++ )
        {
            // when
            RaftLogEntry raftLogEntry = reader.get( offset + logIndex );

            // then
            assertEquals( entries[offset], raftLogEntry );

            if ( offset <= 1 )
            {
                verify( inFlightMap ).get( offset + logIndex );
            }

            if ( offset == 1 )
            {
                verify( raftLog ).getEntryCursor( offset + logIndex );
            }

            assertCacheIsUpdated( inFlightMap, offset + logIndex );
        }

        verifyNoMoreInteractions( inFlightMap );
        verifyNoMoreInteractions( raftLog );
    }

    private void startingFromIndexReturnEntries( InFlightMap<RaftLogEntry> inFlightMap, long startIndex,
            RaftLogEntry entry, RaftLogEntry... otherEntries ) throws IOException
    {
        when( inFlightMap.get( startIndex ) ).thenReturn( entry );
        for ( int offset = 0; offset < otherEntries.length; offset++ )
        {
            when( inFlightMap.get( startIndex + offset + 1L ) ).thenReturn( otherEntries[offset] );
        }
    }

    private void startingFromIndexReturnEntries( ReadableRaftLog raftLog, long startIndex,
            RaftLogEntry entry, RaftLogEntry... otherEntries ) throws IOException
    {
        RaftLogCursor cursor = mock( RaftLogCursor.class );
        when( raftLog.getEntryCursor( startIndex ) ).thenReturn( cursor, (RaftLogCursor) null );

        Boolean[] bools = new Boolean[otherEntries.length + 1];
        Arrays.fill( bools, Boolean.TRUE );
        bools[otherEntries.length] = Boolean.FALSE;

        when( cursor.next() ).thenReturn( true, bools );

        Long[] indexes = new Long[otherEntries.length + 1];
        for ( int offset = 0; offset < indexes.length; offset++ )
        {
            indexes[offset] = startIndex + 1 + offset;
        }
        indexes[otherEntries.length] = -1L;

        when( cursor.index() ).thenReturn( startIndex, indexes );

        RaftLogEntry[] raftLogEntries = Arrays.copyOf( otherEntries, otherEntries.length + 1 );
        when( cursor.get() ).thenReturn( entry, raftLogEntries );
    }

    public void assertCacheIsUpdated( InFlightMap<RaftLogEntry> inFlightMap, long key )
    {
        if ( clearCache )
        {
            verify( inFlightMap, times( 1 ) ).remove( key );
        }
        else
        {
            verify( inFlightMap, never() ).remove( key );
        }
    }
}
