/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.index.bitmaps;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class BitmapFormatTest
{
    @Test
    public void shouldConvertRangeAndBitmapToArray_32() throws Exception
    {
        // given
        for ( int i = 0; i < 32; i++ )
        {
            // when
            long[] longs = BitmapFormat._32.convertRangeAndBitmapToArray( 16, 1L << i );

            // then
            assertArrayEquals( new long[]{(16 << 5) + i}, longs );
        }

        // given
        for ( int i = 0; i < 32; i++ )
        {
            // when
            long[] longs = BitmapFormat._32.convertRangeAndBitmapToArray( 13, -1L );

            // then
            long[] expected = new long[32];
            for ( int j = 0; j < expected.length; j++ )
            {
                expected[j] = (13 << 5) + j;
            }
            assertArrayEquals( expected, longs );
        }
    }

    @Test
    public void shouldConvertRangeAndBitmapToArray_64() throws Exception
    {
        // given
        for ( int i = 0; i < 64; i++ )
        {
            // when
            long[] longs = BitmapFormat._64.convertRangeAndBitmapToArray( 11, 1L << i );

            // then
            assertArrayEquals( new long[]{(11 << 6) + i}, longs );
        }

        // given
        for ( int i = 0; i < 32; i++ )
        {
            // when
            long[] longs = BitmapFormat._64.convertRangeAndBitmapToArray( 19, -1L );

            // then
            long[] expected = new long[64];
            for ( int j = 0; j < expected.length; j++ )
            {
                expected[j] = (19 << 6) + j;
            }
            assertArrayEquals( expected, longs );
        }
    }

    @Test
    public void shouldSetBitmap_32() throws Exception
    {
        Set<Long> bitmaps = new HashSet<>();
        for ( int i = 0; i < 32; i++ )
        {
            // given
            Bitmap bitmap = new Bitmap();
            // when
            BitmapFormat._32.set( bitmap, i, true );
            // then
            assertEquals( "set i=" + i, 1, Long.bitCount( bitmap.bitmap() ) );
            bitmaps.add( bitmap.bitmap() );
            // when
            BitmapFormat._32.set( bitmap, i, false );
            // then
            assertEquals( "unset i=" + i, 0L, bitmap.bitmap() );
        }
        // then
        assertEquals( "each value is unique", 32, bitmaps.size() );
    }

    @Test
    public void shouldSetBitmap_64() throws Exception
    {
        Set<Long> bitmaps = new HashSet<>();
        for ( int i = 0; i < 64; i++ )
        {
            // given
            Bitmap bitmap = new Bitmap();
            // when
            BitmapFormat._64.set( bitmap, i, true );
            // then
            assertEquals( "set i=" + i, 1, Long.bitCount( bitmap.bitmap() ) );
            bitmaps.add( bitmap.bitmap() );
            // when
            BitmapFormat._64.set( bitmap, i, false );
            // then
            assertEquals( "unset i=" + i, 0L, bitmap.bitmap() );
        }
        // then
        assertEquals( "each value is unique", 64, bitmaps.size() );
    }

    @Test
    public void shouldBeAbleToCheckIfASingleNodeIdIsSet() throws Exception
    {
        // when
        for ( int nodeId = 0; nodeId < 32; nodeId++ )
        {
            Bitmap bitmap = new Bitmap();
            BitmapFormat._32.set( bitmap, nodeId, true );

            for ( int offsetId = 1; offsetId < 32; offsetId++ )
            {
                assertThat( BitmapFormat._32.peek( bitmap.bitmap(), nodeId ), is( true ) );
                assertThat( BitmapFormat._32.peek( bitmap.bitmap(), (nodeId + offsetId) % 32 ), is( false ) );
            }
        }
    }
}
