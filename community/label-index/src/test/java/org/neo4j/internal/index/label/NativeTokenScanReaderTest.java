/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import java.util.Collections;
import java.util.PrimitiveIterator;
import java.util.stream.LongStream;

import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.kernel.api.index.IndexProgressor;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.collection.PrimitiveLongCollections.asArray;
import static org.neo4j.collection.PrimitiveLongCollections.closingAsArray;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

@SuppressWarnings( "StatementWithEmptyBody" )
@Execution( CONCURRENT )
class NativeTokenScanReaderTest
{
    private static final int LABEL_ID = 1;
    private long[] keys = {0, 1, 3};
    private long[] bitsets = {0b1000_1000__1100_0010L,
                              0b0000_0010__0000_1000L,
                              0b0010_0000__1010_0001L};
    private long[] expected = {
            // base 0*64 = 0
            1, 6, 7, 11, 15,
            // base 1*64 = 64
            64 + 3, 64 + 9,
            // base 3*64 = 192
            192 + 0, 192 + 5, 192 + 7, 192 + 13};

    private Seeker<TokenScanKey,TokenScanValue> cursor( boolean ascending ) throws Exception
    {
        Seeker<TokenScanKey,TokenScanValue> cursor = mock( Seeker.class );
        when( cursor.next() ).thenReturn( true, true, true, false );
        when( cursor.key() ).thenReturn(
                key( ascending ? keys[0] : keys[2] ),
                key( keys[1] ),
                key( ascending ? keys[2] : keys[0] ) );
        when( cursor.value() ).thenReturn(
                value( ascending ? bitsets[0] : bitsets[2] ),
                value( bitsets[1] ),
                value( ascending ? bitsets[2] : bitsets[0] ),
                null );
        return cursor;
    }

    private GBPTree<TokenScanKey,TokenScanValue> setupMockIndex( boolean ascending ) throws Exception
    {

        GBPTree<TokenScanKey,TokenScanValue> index = mock( GBPTree.class );

        when( index.seek( any( TokenScanKey.class ), any( TokenScanKey.class ), eq( NULL ) ) )
                .thenAnswer( ignored -> cursor( ascending ) );
        return index;
    }

    @SuppressWarnings( "unchecked" )
    @Test
    void shouldFindMultipleNodesInEachRange() throws Exception
    {
        // GIVEN
        GBPTree<TokenScanKey,TokenScanValue> index = setupMockIndex( true );

        // WHEN
        NativeTokenScanReader reader = new NativeTokenScanReader( index );
        try ( PrimitiveLongResourceIterator iterator = reader.entitiesWithToken( LABEL_ID, NULL ) )
        {
            // THEN
            assertArrayEquals( expected, closingAsArray( iterator ) );
        }
    }

    @Test
    void shouldFindMultipleWithProgressorAscending() throws Exception
    {
        // GIVEN
        GBPTree<TokenScanKey,TokenScanValue> index = setupMockIndex( true );

        // WHEN
        NativeTokenScanReader reader = new NativeTokenScanReader( index );
        TokenScan tokenScan = reader.entityTokenScan( LABEL_ID, NULL );
        TokenScanValueIndexProgressorTest.MyClient client = new TokenScanValueIndexProgressorTest.MyClient( LongStream.of( expected ).iterator() );
        IndexProgressor progressor = tokenScan.initialize( client, IndexOrder.ASCENDING, NULL );

        while ( progressor.next() )
        {
        }
    }

    @Test
    void shouldFindMultipleWithProgressorDescending() throws Exception
    {
        // GIVEN
        GBPTree<TokenScanKey,TokenScanValue> index = setupMockIndex( false );

        // WHEN
        NativeTokenScanReader reader = new NativeTokenScanReader( index );
        TokenScan tokenScan = reader.entityTokenScan( LABEL_ID, NULL );
        PrimitiveIterator.OfLong iterator = LongStream.of( expected )
                                                      .boxed()
                                                      .sorted( Collections.reverseOrder() )
                                                      .mapToLong( l -> l )
                                                      .iterator();
        TokenScanValueIndexProgressorTest.MyClient client = new TokenScanValueIndexProgressorTest.MyClient( iterator );
        IndexProgressor progressor = tokenScan.initialize( client, IndexOrder.DESCENDING, NULL );

        while ( progressor.next() )
        {
        }
    }

    @Test
    void shouldSupportMultipleOpenCursorsConcurrently() throws Exception
    {
        // GIVEN
        GBPTree<TokenScanKey,TokenScanValue> index = mock( GBPTree.class );
        Seeker<TokenScanKey,TokenScanValue> cursor1 = mock( Seeker.class );
        when( cursor1.next() ).thenReturn( false );
        Seeker<TokenScanKey,TokenScanValue> cursor2 = mock( Seeker.class );
        when( cursor2.next() ).thenReturn( false );
        when( index.seek( any( TokenScanKey.class ), any( TokenScanKey.class ), eq( NULL ) ) ).thenReturn( cursor1, cursor2 );

        // WHEN
        NativeTokenScanReader reader = new NativeTokenScanReader( index );
        try ( PrimitiveLongResourceIterator first = reader.entitiesWithToken( LABEL_ID, NULL );
              PrimitiveLongResourceIterator second = reader.entitiesWithToken( LABEL_ID, NULL ) )
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
        GBPTree<TokenScanKey,TokenScanValue> index = mock( GBPTree.class );
        Seeker<TokenScanKey,TokenScanValue> cursor1 = mock( Seeker.class );
        when( cursor1.next() ).thenReturn( false );
        Seeker<TokenScanKey,TokenScanValue> cursor2 = mock( Seeker.class );
        when( cursor2.next() ).thenReturn( false );
        when( index.seek( any( TokenScanKey.class ), any( TokenScanKey.class ), eq( NULL ) ) ).thenReturn( cursor1, cursor2 );

        // WHEN
        NativeTokenScanReader reader = new NativeTokenScanReader( index );
        try ( PrimitiveLongResourceIterator ignore1 = reader.entitiesWithToken( LABEL_ID, NULL );
              PrimitiveLongResourceIterator ignore2 = reader.entitiesWithToken( LABEL_ID, NULL )
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
        GBPTree<TokenScanKey,TokenScanValue> index = mock( GBPTree.class );
        Seeker<TokenScanKey,TokenScanValue> cursor = mock( Seeker.class );
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
        when( index.seek( any( TokenScanKey.class ), any( TokenScanKey.class ), eq( NULL ) ) )
                .thenReturn( cursor );

        // when
        long fromId = TokenScanValue.RANGE_SIZE + 3;
        NativeTokenScanReader reader = new NativeTokenScanReader( index );
        try ( PrimitiveLongResourceIterator iterator = reader.entitiesWithAnyOfTokens( fromId, new int[]{LABEL_ID}, NULL ) )
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

    private static TokenScanValue value( long bits )
    {
        TokenScanValue value = new TokenScanValue();
        value.bits = bits;
        return value;
    }

    private static TokenScanKey key( long idRange )
    {
        return new TokenScanKey( LABEL_ID, idRange );
    }

    private static void exhaust( LongIterator iterator )
    {
        while ( iterator.hasNext() )
        {
            iterator.next();
        }
    }
}
