/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.index.label;

import org.eclipse.collections.api.iterator.LongIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;

import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Seeker;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.collection.PrimitiveLongCollections.asArray;
import static org.neo4j.collection.PrimitiveLongCollections.closingAsArray;

@Execution( CONCURRENT )
class NativeLabelScanReaderTest
{
    private static final int LABEL_ID = 1;

    @SuppressWarnings( "unchecked" )
    @Test
    void shouldFindMultipleNodesInEachRange() throws Exception
    {
        // GIVEN
        GBPTree<LabelScanKey,LabelScanValue> index = mock( GBPTree.class );
        Seeker<LabelScanKey,LabelScanValue> cursor = mock( Seeker.class );
        when( cursor.next() ).thenReturn( true, true, true, false );
        when( cursor.key() ).thenReturn(
                key( 0 ),
                key( 1 ),
                key( 3 ) );
        when( cursor.value() ).thenReturn(
                value( 0b1000_1000__1100_0010L ),
                value( 0b0000_0010__0000_1000L ),
                value( 0b0010_0000__1010_0001L ),
                null );
        when( index.seek( any( LabelScanKey.class ), any( LabelScanKey.class ) ) )
                .thenReturn( cursor );
        // WHEN
        NativeLabelScanReader reader = new NativeLabelScanReader( index );
        try ( PrimitiveLongResourceIterator iterator = reader.nodesWithLabel( LABEL_ID ) )
        {
            // THEN
            assertArrayEquals( new long[]{
                            // base 0*64 = 0
                            1, 6, 7, 11, 15,
                            // base 1*64 = 64
                            64 + 3, 64 + 9,
                            // base 3*64 = 192
                            192 + 0, 192 + 5, 192 + 7, 192 + 13},

                    closingAsArray( iterator ) );
        }
    }

    @Test
    void shouldSupportMultipleOpenCursorsConcurrently() throws Exception
    {
        // GIVEN
        GBPTree<LabelScanKey,LabelScanValue> index = mock( GBPTree.class );
        Seeker<LabelScanKey,LabelScanValue> cursor1 = mock( Seeker.class );
        when( cursor1.next() ).thenReturn( false );
        Seeker<LabelScanKey,LabelScanValue> cursor2 = mock( Seeker.class );
        when( cursor2.next() ).thenReturn( false );
        when( index.seek( any( LabelScanKey.class ), any( LabelScanKey.class ) ) ).thenReturn( cursor1, cursor2 );

        // WHEN
        NativeLabelScanReader reader = new NativeLabelScanReader( index );
        try ( PrimitiveLongResourceIterator first = reader.nodesWithLabel( LABEL_ID );
              PrimitiveLongResourceIterator second = reader.nodesWithLabel( LABEL_ID ) )
        {
            // first check test invariants
            verify( cursor1, never() ).close();
            verify( cursor2, never() ).close();

            // getting the second iterator should not have closed the first one
            verify( cursor1, never() ).close();
            verify( cursor2, never() ).close();

            // exhausting the first one should have closed only the first one
            exhaust( first );
            verify( cursor1 ).close();
            verify( cursor2, never() ).close();

            // exhausting the second one should close it
            exhaust( second );
            verify( cursor1 ).close();
            verify( cursor2 ).close();
        }
    }

    @Test
    void shouldCloseUnexhaustedCursorsOnReaderClose() throws Exception
    {
        // GIVEN
        GBPTree<LabelScanKey,LabelScanValue> index = mock( GBPTree.class );
        Seeker<LabelScanKey,LabelScanValue> cursor1 = mock( Seeker.class );
        when( cursor1.next() ).thenReturn( false );
        Seeker<LabelScanKey,LabelScanValue> cursor2 = mock( Seeker.class );
        when( cursor2.next() ).thenReturn( false );
        when( index.seek( any( LabelScanKey.class ), any( LabelScanKey.class ) ) ).thenReturn( cursor1, cursor2 );

        // WHEN
        NativeLabelScanReader reader = new NativeLabelScanReader( index );
        try ( PrimitiveLongResourceIterator ignore1 = reader.nodesWithLabel( LABEL_ID );
              PrimitiveLongResourceIterator ignore2 = reader.nodesWithLabel( LABEL_ID )
        )
        {
            // first check test invariants

            verify( cursor1, never() ).close();
            verify( cursor2, never() ).close();
        }

        // THEN
        verify( cursor1 ).close();
        verify( cursor2 ).close();
    }

    @Test
    void shouldStartFromGivenId() throws IOException
    {
        // given
        GBPTree<LabelScanKey,LabelScanValue> index = mock( GBPTree.class );
        Seeker<LabelScanKey,LabelScanValue> cursor = mock( Seeker.class );
        when( cursor.next() ).thenReturn( true, true, false );
        when( cursor.key() ).thenReturn(
                key( 1 ),
                key( 3 ),
                null );
        when( cursor.value() ).thenReturn(
                value( 0b0001_1000__0101_1110L ),
                //                     ^--fromId, i.e. ids after this id should be visible
                value( 0b0010_0000__1010_0001L ),
                null );
        when( index.seek( any( LabelScanKey.class ), any( LabelScanKey.class ) ) )
                .thenReturn( cursor );

        // when
        long fromId = LabelScanValue.RANGE_SIZE + 3;
        NativeLabelScanReader reader = new NativeLabelScanReader( index );
        try ( PrimitiveLongResourceIterator iterator = reader.nodesWithAnyOfLabels( fromId, LABEL_ID ) )
        {
            // then
            assertArrayEquals( new long[] {
                            // base 1*64 = 64
                            64 + 4, 64 + 6, 64 + 11, 64 + 12,
                            // base 3*64 = 192
                            192 + 0, 192 + 5, 192 + 7, 192 + 13 },

                    asArray( iterator ) );
        }
    }

    private static LabelScanValue value( long bits )
    {
        LabelScanValue value = new LabelScanValue();
        value.bits = bits;
        return value;
    }

    private static LabelScanKey key( long idRange )
    {
        return new LabelScanKey( LABEL_ID, idRange );
    }

    private static void exhaust( LongIterator iterator )
    {
        while ( iterator.hasNext() )
        {
            iterator.next();
        }
    }
}
