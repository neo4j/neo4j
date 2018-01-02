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
package org.neo4j.kernel.api.impl.index.bitmaps;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BitmapExtractorTest
{
    @Test
    public void shouldIterateOverAllIdsInPage() throws Exception
    {
        // given
        BitmapExtractor pages = new BitmapExtractor( BitmapFormat._64,
                                                           // first range
                                                           0x00L >> 6, -1L,
                                                           // second range
                                                           0x80L >> 6, -1L );

        // then
        long[] expected = new long[64];
        for ( int i = 0; i < expected.length; i++ )
        {
            expected[i] = i;
        }
        assertTrue( pages.hasNext() );
        assertArrayEquals( expected, pages.next() );

        for ( int i = 0; i < expected.length; i++ )
        {
            expected[i] = 0x80 + i;
        }
        assertTrue( pages.hasNext() );
        assertArrayEquals( expected, pages.next() );

        assertFalse( pages.hasNext() );
    }

    @Test
    public void shouldYieldNothingForEmptyPages() throws Exception
    {
        // given
        BitmapExtractor pages = new BitmapExtractor( BitmapFormat._64,
                                                           0, 0,  // page 1
                                                           1, 0,  // page 2
                                                           2, 0,  // page 3
                                                           3, 0,  // page 4
                                                           4, 0,  // page 5
                                                           5, 0,  // page 6
                                                           6, 0,  // page 7
                                                           7, 0 );// page 8

        // then
        assertFalse( pages.hasNext() );
    }

    @Test
    public void shouldYieldCorrectBitFromEachPage() throws Exception
    {
        // given
        long[] rangeBitmap = new long[64 * 2];
        for ( int i = 0; i < 64; i++ )
        {
            rangeBitmap[i * 2] = i;
            rangeBitmap[i * 2 + 1] = 1L << i;
        }
        BitmapExtractor pages = new BitmapExtractor( BitmapFormat._64, rangeBitmap );
        // then
        for ( int i = 0; i < 64; i++ )
        {
            assertArrayEquals( "page:" + i, new long[]{64 * i + i}, pages.next() );
        }
        assertFalse( pages.hasNext() );
    }

    @Test
    public void shouldHandle32BitBitmaps() throws Exception
    {
        // given
        BitmapExtractor pages = new BitmapExtractor( BitmapFormat._32, 0xFFL, 0x0001_8000_0000L );

        // then
        assertArrayEquals( new long[]{(0xFFL << 5) + 31}, pages.next() );
        assertFalse( pages.hasNext() );
    }
}
