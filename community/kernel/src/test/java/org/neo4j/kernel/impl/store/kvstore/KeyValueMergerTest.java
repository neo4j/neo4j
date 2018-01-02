/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.store.kvstore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import static java.util.Arrays.asList;

import static org.junit.Assert.assertEquals;

import static org.neo4j.kernel.impl.store.kvstore.KeyValueMergerTest.Pair.pair;

public class KeyValueMergerTest
{
    @Test
    public void shouldMergeEmptyProviders() throws Exception
    {
        // given
        KeyValueMerger merger = new KeyValueMerger( provider(), provider(), 4, 4 );

        // when
        List<Integer> data = extract( merger );

        // then
        assertEquals( Arrays.<Integer>asList(), data );
    }

    @Test
    public void shouldProvideUpdatesWhenNoDataProvided() throws Exception
    {
        // given
        KeyValueMerger merger = new KeyValueMerger( provider(), provider( pair( 14, 1 ),
                                                                          pair( 19, 2 ),
                                                                          pair( 128, 3 ) ), 4, 4 );

        // when
        List<Integer> data = extract( merger );

        // then
        assertEquals( asList( 14, 1,
                              19, 2,
                              128, 3 ), data );
    }

    @Test
    public void shouldProvideUpdatesWhenNoChangesProvided() throws Exception
    {
        // given
        KeyValueMerger merger = new KeyValueMerger( provider( pair( 14, 1 ),
                                                              pair( 19, 2 ),
                                                              pair( 128, 3 ) ), provider(), 4, 4 );

        // when
        List<Integer> data = extract( merger );

        // then
        assertEquals( asList( 14, 1,
                              19, 2,
                              128, 3 ), data );
    }

    @Test
    public void shouldMergeDataStreams() throws Exception
    {
        // given
        KeyValueMerger merger = new KeyValueMerger( provider( pair( 1, 1 ), pair( 3, 1 ), pair( 5, 1 ) ),
                                                    provider( pair( 2, 2 ), pair( 4, 2 ), pair( 6, 2 ) ), 4, 4 );

        // when
        List<Integer> data = extract( merger );

        // then
        assertEquals( asList( 1, 1,
                              2, 2,
                              3, 1,
                              4, 2,
                              5, 1,
                              6, 2 ), data );
    }

    @Test
    public void shouldReplaceValuesOnEqualKey() throws Exception
    {
        // given
        KeyValueMerger merger = new KeyValueMerger( provider( pair( 1, 1 ), pair( 3, 1 ), pair( 5, 1 ) ),
                                                    provider( pair( 2, 2 ), pair( 3, 2 ), pair( 6, 2 ) ), 4, 4 );

        // when
        List<Integer> data = extract( merger );

        // then
        assertEquals( asList( 1, 1,
                              2, 2,
                              3, 2,
                              5, 1,
                              6, 2 ), data );
    }

    private static List<Integer> extract( EntryVisitor<WritableBuffer> producer ) throws IOException
    {
        List<Integer> result = new ArrayList<>();
        BigEndianByteArrayBuffer key = new BigEndianByteArrayBuffer( 4 );
        BigEndianByteArrayBuffer value = new BigEndianByteArrayBuffer( 4 );
        while ( producer.visit( key, value ) )
        {
            result.add( key.getInt( 0 ) );
            result.add( value.getInt( 0 ) );
        }
        return result;
    }

    static DataProvider provider( final Pair... data )
    {
        return new DataProvider()
        {
            int i;

            @Override
            public boolean visit( WritableBuffer key, WritableBuffer value ) throws IOException
            {
                if ( i < data.length )
                {
                    data[i++].visit( key, value );
                    return true;
                }
                return false;
            }

            @Override
            public void close() throws IOException
            {
            }
        };
    }

    static class Pair
    {
        static Pair pair( int key, int value )
        {
            return new Pair( key, value );
        }

        final int key, value;

        Pair( int key, int value )
        {
            this.key = key;
            this.value = value;
        }

        void visit( WritableBuffer key, WritableBuffer value )
        {
            key.putInt( 0, this.key );
            value.putInt( 0, this.value );
        }
    }
}