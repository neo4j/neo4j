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
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EntryBasedLogPruningStrategyTest
{
    @Test
    public void indexToKeepTest() throws Exception
    {
        //given
        Segments segments = mock( Segments.class );
        List<SegmentFile> testSegmentFiles = testSegmentFiles( 10 );
        when( segments.getSegmentFileIteratorAtEnd() ).thenAnswer(
                (Answer<ListIterator>) invocationOnMock -> testSegmentFiles.listIterator( testSegmentFiles.size() ) );
        EntryBasedLogPruningStrategy strategy = new EntryBasedLogPruningStrategy( 6, mock( LogProvider.class ) );

        //when
        long indexToKeep = strategy.getIndexToKeep( segments );

        //then
        assertEquals( 2, indexToKeep );
    }

    @Test
    public void pruneStrategyExceedsNumberOfEntriesTest() throws Exception
    {
        //given
        Segments segments = mock( Segments.class );
        List<SegmentFile> subList = testSegmentFiles( 10 ).subList( 5, 10 );
        when( segments.getSegmentFileIteratorAtEnd() )
                .thenAnswer( (Answer<ListIterator>) invocationOnMock -> subList.listIterator( subList.size() ) );
        EntryBasedLogPruningStrategy strategy = new EntryBasedLogPruningStrategy( 7, mock( LogProvider.class ) );

        //when
        long indexToKeep = strategy.getIndexToKeep( segments );

        //then
        assertEquals( 4, indexToKeep );
    }

    @Test
    public void onlyFirstActiveLogFileTest() throws Exception
    {
        //given
        Segments segments = mock( Segments.class );
        List<SegmentFile> testSegmentFiles = testSegmentFiles( 1 );
        when( segments.getSegmentFileIteratorAtEnd() ).thenAnswer(
                (Answer<ListIterator>) invocationOnMock -> testSegmentFiles.listIterator( testSegmentFiles.size() ) );
        EntryBasedLogPruningStrategy strategy = new EntryBasedLogPruningStrategy( 6, mock( LogProvider.class ) );

        //when
        long indexToKeep = strategy.getIndexToKeep( segments );

        //then
        assertEquals( -1, indexToKeep );
    }

    @Test
    public void onlyOneActiveLogFileTest() throws Exception
    {
        //given
        Segments segments = mock( Segments.class );
        List<SegmentFile> subList = testSegmentFiles( 6 ).subList( 5, 6 );
        when( segments.getSegmentFileIteratorAtEnd() )
                .thenAnswer( (Answer<ListIterator>) invocationOnMock -> subList.listIterator( subList.size() ) );
        EntryBasedLogPruningStrategy strategy = new EntryBasedLogPruningStrategy( 6, mock( LogProvider.class ) );

        //when
        long indexToKeep = strategy.getIndexToKeep( segments );

        //then
        assertEquals( 4, indexToKeep );
    }

    @Test
    public void noFilesLogsWarningTest() throws Exception
    {
        Segments segments = mock( Segments.class );
        List<SegmentFile> segmentFiles = new ArrayList<>(  );
        when( segments.getSegmentFileIteratorAtEnd() )
                .thenAnswer( (Answer<ListIterator>) invocationOnMock -> segmentFiles.listIterator( segmentFiles.size() ) );
        LogProvider mockLogProvider = mock( LogProvider.class );
        Log mockLog = mock( Log.class );
        when( mockLogProvider.getLog( EntryBasedLogPruningStrategy.class ) ).thenReturn( mockLog );
        EntryBasedLogPruningStrategy strategy = new EntryBasedLogPruningStrategy( 6, mockLogProvider );

        //when
        long indexToKeep = strategy.getIndexToKeep( segments );

        //then
        // a safe index is returned
        assertEquals( -1, indexToKeep );
        // and a warning is issued
        verify( mockLog, times( 1 ) ).warn( anyString() );
    }

    private ArrayList<SegmentFile> testSegmentFiles( int size )
    {
        ArrayList<SegmentFile> list = new ArrayList<>( size );
        for ( int i = 0; i < size; i++ )
        {
            SegmentFile file = mock( SegmentFile.class );
            when( file.header() ).thenReturn( testSegmentHeader( i ) );
            list.add( file );
        }
        return list;
    }

    private SegmentHeader testSegmentHeader( long value )
    {
        return new SegmentHeader( -1, -1, value - 1, -1 );
    }
}
