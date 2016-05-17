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

import java.util.ArrayList;
import java.util.ListIterator;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SizeBasedLogPruningStrategyTest
{
    @Test
    public void indexToKeepTest() throws Exception
    {
        int bytesToKeep = 6;
        int segmentFilesCount = 14;
        int expectedIndex = segmentFilesCount - bytesToKeep;
        Segments segments = mock( Segments.class );
        ListIterator<SegmentFile> testSegmentFiles = testSegmentFiles( segmentFilesCount );
        when( segments.getSegmentFileIteratorAtEnd() ).thenReturn( testSegmentFiles );
        SizeBasedLogPruningStrategy sizeBasedLogPruningStrategy = new SizeBasedLogPruningStrategy( bytesToKeep );
        assertEquals( expectedIndex, sizeBasedLogPruningStrategy.getIndexToKeep( segments ) );
    }

    private ListIterator<SegmentFile> testSegmentFiles( int size )
    {
        ArrayList<SegmentFile> list = new ArrayList<>( size );
        for ( int i = 0; i < size; i++ )
        {
            SegmentFile file = mock( SegmentFile.class );
            when( file.header() ).thenReturn( testSegmentHeader( i ) );
            when( file.size() ).thenReturn( 1L );
            list.add( file );
        }
        return list.listIterator( size );
    }

    private SegmentHeader testSegmentHeader( long value )
    {
        return new SegmentHeader( -1, -1, value, -1 );
    }
}
