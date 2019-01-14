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
package org.neo4j.io.pagecache.impl;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import org.neo4j.io.pagecache.CursorException;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.StubPageCursor;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.test.matchers.ByteArrayMatcher.byteArray;

public class CompositePageCursorTest
{
    private static final int PAGE_SIZE = 16;
    private StubPageCursor first;
    private StubPageCursor second;
    private byte[] bytes = new byte[4];

    private StubPageCursor generatePage( int initialPageId, int pageSize, int initialValue )
    {
        StubPageCursor cursor = new StubPageCursor( initialPageId, pageSize );
        for ( int i = 0; i < pageSize; i++ )
        {
            cursor.putByte( i, (byte) (initialValue + i) );
        }
        return cursor;
    }

    @Before
    public void setUp()
    {
        first = generatePage( 0, PAGE_SIZE, 0xA0 );
        second = generatePage( 2, PAGE_SIZE + 8, 0xB0 );
    }

    @Test
    public void getByteMustHitFirstCursorBeforeFlip()
    {
        PageCursor c = CompositePageCursor.compose( first, 1, second, 1 );
        assertThat( c.getByte(), is( (byte) 0xA0 ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void getByteMustHitSecondCursorAfterFlip()
    {
        PageCursor c = CompositePageCursor.compose( first, 1, second, 1 );
        assertThat( c.getByte(), is( (byte) 0xA0 ) );
        assertThat( c.getByte(), is( (byte) 0xB0 ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void getByteMustRespectOffsetIntoFirstCursor()
    {
        first.setOffset( 1 );
        PageCursor c = CompositePageCursor.compose( first, 1, second, 1 );
        assertThat( c.getByte(), is( (byte) 0xA1 ) );
        assertThat( c.getByte(), is( (byte) 0xB0 ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void getByteMustRespectOffsetIntoSecondCursor()
    {
        second.setOffset( 1 );
        PageCursor c = CompositePageCursor.compose( first, 1, second, 1 );
        assertThat( c.getByte(), is( (byte) 0xA0 ) );
        assertThat( c.getByte(), is( (byte) 0xB1 ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void putByteMustHitFirstCursorBeforeFlip()
    {
        PageCursor c = CompositePageCursor.compose( first, 1, second, 1 );
        c.putByte( (byte) 1 );
        c.setOffset( 0 );
        assertThat( c.getByte(), is( (byte) 1 ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void putByteMustHitSecondCursorAfterFlip()
    {
        PageCursor c = CompositePageCursor.compose( first, 1, second, 1 );
        c.putByte( (byte) 1 );
        c.putByte( (byte) 2 );
        c.setOffset( 1 );
        assertThat( c.getByte(), is( (byte) 2 ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void putByteMustRespectOffsetIntoFirstCursor()
    {
        first.setOffset( 1 );
        PageCursor c = CompositePageCursor.compose( first, 1, second, 1 );
        c.putByte( (byte) 1 );
        assertThat( first.getByte( 1 ), is( (byte) 1 ) );
        assertThat( c.getByte(), is( (byte) 0xB0 ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void putByteMustRespectOffsetIntoSecondCursor()
    {
        second.setOffset( 1 );
        PageCursor c = CompositePageCursor.compose( first, 1, second, 2 );
        c.putByte( (byte) 1 );
        c.putByte( (byte) 2 );
        assertThat( second.getByte( 1 ), is( (byte) 2 ) );
        assertThat( c.getByte(), is( (byte) 0xB2 ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void getByteWithOffsetMustHitCorrectCursors()
    {
        first.setOffset( 1 );
        second.setOffset( 2 );
        PageCursor c = CompositePageCursor.compose( first, 1 + 1, second, 1 );
        assertThat( c.getByte( 1 ), is( (byte) 0xA2 ) );
        assertThat( c.getByte( 1 + 1 ), is( (byte) 0xB2 ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void putByteWithOffsetMustHitCorrectCursors()
    {
        first.setOffset( 1 );
        second.setOffset( 2 );
        PageCursor c = CompositePageCursor.compose( first, 2 * 1, second, 2 * 1 );
        c.putByte( 1, (byte) 1 );
        c.putByte( 1 + 1, (byte) 2 );
        assertThat( c.getByte(), is( (byte) 0xA1 ) );
        assertThat( c.getByte(), is( (byte) 1 ) );
        assertThat( c.getByte(), is( (byte) 2 ) );
        assertThat( c.getByte(), is( (byte) 0xB3 ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void getShortMustHitFirstCursorBeforeFlip()
    {
        PageCursor c = CompositePageCursor.compose( first, 2, second, 2 );
        assertThat( c.getShort(), is( (short) 0xA0A1 ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void getShortMustHitSecondCursorAfterFlip()
    {
        PageCursor c = CompositePageCursor.compose( first, 2, second, 2 );
        assertThat( c.getShort(), is( (short) 0xA0A1 ) );
        assertThat( c.getShort(), is( (short) 0xB0B1 ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void getShortMustRespectOffsetIntoFirstCursor()
    {
        first.setOffset( 1 );
        PageCursor c = CompositePageCursor.compose( first, 2, second, 2 );
        assertThat( c.getShort(), is( (short) 0xA1A2 ) );
        assertThat( c.getShort(), is( (short) 0xB0B1 ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void getShortMustRespectOffsetIntoSecondCursor()
    {
        second.setOffset( 1 );
        PageCursor c = CompositePageCursor.compose( first, 2, second, 2 );
        assertThat( c.getShort(), is( (short) 0xA0A1 ) );
        assertThat( c.getShort(), is( (short) 0xB1B2 ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void putShortMustHitFirstCursorBeforeFlip()
    {
        PageCursor c = CompositePageCursor.compose( first, 2, second, 2 );
        c.putShort( (short) 1 );
        c.setOffset( 0 );
        assertThat( c.getShort(), is( (short) 1 ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void putShortMustHitSecondCursorAfterFlip()
    {
        PageCursor c = CompositePageCursor.compose( first, 2, second, 2 );
        c.putShort( (short) 1 );
        c.putShort( (short) 2 );
        c.setOffset( 2 );
        assertThat( c.getShort(), is( (short) 2 ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void putShortMustRespectOffsetIntoFirstCursor()
    {
        first.setOffset( 1 );
        PageCursor c = CompositePageCursor.compose( first, 2, second, 2 );
        c.putShort( (short) 1 );
        assertThat( first.getShort( 1 ), is( (short) 1 ) );
        assertThat( c.getShort(), is( (short) 0xB0B1 ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void putShortMustRespectOffsetIntoSecondCursor()
    {
        second.setOffset( 1 );
        PageCursor c = CompositePageCursor.compose( first, 2, second, 4 );
        c.putShort( (short) 1 );
        c.putShort( (short) 2 );
        assertThat( second.getShort( 1 ), is( (short) 2 ) );
        assertThat( c.getShort(), is( (short) 0xB3B4 ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void getShortWithOffsetMustHitCorrectCursors()
    {
        first.setOffset( 1 );
        second.setOffset( 2 );
        PageCursor c = CompositePageCursor.compose( first, 1 + 2, second, 2 );
        assertThat( c.getShort( 1 ), is( (short) 0xA2A3 ) );
        assertThat( c.getShort( 1 + 2 ), is( (short) 0xB2B3 ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void putShortWithOffsetMustHitCorrectCursors()
    {
        first.setOffset( 1 );
        second.setOffset( 2 );
        PageCursor c = CompositePageCursor.compose( first, 2 * 2, second, 2 * 2 );
        c.putShort( 2, (short) 1 );
        c.putShort( 2 + 2, (short) 2 );
        assertThat( c.getShort(), is( (short) 0xA1A2 ) );
        assertThat( c.getShort(), is( (short) 1 ) );
        assertThat( c.getShort(), is( (short) 2 ) );
        assertThat( c.getShort(), is( (short) 0xB4B5 ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void getIntMustHitFirstCursorBeforeFlip()
    {
        PageCursor c = CompositePageCursor.compose( first, 4, second, 4 );
        assertThat( c.getInt(), is( 0xA0A1A2A3 ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void getIntMustHitSecondCursorAfterFlip()
    {
        PageCursor c = CompositePageCursor.compose( first, 4, second, 4 );
        assertThat( c.getInt(), is( 0xA0A1A2A3 ) );
        assertThat( c.getInt(), is( 0xB0B1B2B3 ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void getIntMustRespectOffsetIntoFirstCursor()
    {
        first.setOffset( 1 );
        PageCursor c = CompositePageCursor.compose( first, 4, second, 4 );
        assertThat( c.getInt(), is( 0xA1A2A3A4 ) );
        assertThat( c.getInt(), is( 0xB0B1B2B3 ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void getIntMustRespectOffsetIntoSecondCursor()
    {
        second.setOffset( 1 );
        PageCursor c = CompositePageCursor.compose( first, 4, second, 4 );
        assertThat( c.getInt(), is( 0xA0A1A2A3 ) );
        assertThat( c.getInt(), is( 0xB1B2B3B4 ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void putIntMustHitFirstCursorBeforeFlip()
    {
        PageCursor c = CompositePageCursor.compose( first, 4, second, 4 );
        c.putInt( 1 );
        c.setOffset( 0 );
        assertThat( c.getInt(), is( 1 ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void putIntMustHitSecondCursorAfterFlip()
    {
        PageCursor c = CompositePageCursor.compose( first, 4, second, 4 );
        c.putInt( 1 );
        c.putInt( 2 );
        c.setOffset( 4 );
        assertThat( c.getInt(), is( 2 ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void putIntMustRespectOffsetIntoFirstCursor()
    {
        first.setOffset( 1 );
        PageCursor c = CompositePageCursor.compose( first, 4, second, 4 );
        c.putInt( 1 );
        assertThat( first.getInt( 1 ), is( 1 ) );
        assertThat( c.getInt(), is( 0xB0B1B2B3 ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void putIntMustRespectOffsetIntoSecondCursor()
    {
        second.setOffset( 1 );
        PageCursor c = CompositePageCursor.compose( first, 4, second, 8 );
        c.putInt( 1 );
        c.putInt( 2 );
        assertThat( second.getInt( 1 ), is( 2 ) );
        assertThat( c.getInt(), is( 0xB5B6B7B8 ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void getIntWithOffsetMustHitCorrectCursors()
    {
        first.setOffset( 1 );
        second.setOffset( 2 );
        PageCursor c = CompositePageCursor.compose( first, 1 + 4, second, 4 );
        assertThat( c.getInt( 1 ), is( 0xA2A3A4A5 ) );
        assertThat( c.getInt( 1 + 4 ), is( 0xB2B3B4B5 ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void putIntWithOffsetMustHitCorrectCursors()
    {
        first.setOffset( 1 );
        second.setOffset( 2 );
        PageCursor c = CompositePageCursor.compose( first, 2 * 4, second, 2 * 4 );
        c.putInt( 4, 1 );
        c.putInt( 4 + 4, 2 );
        assertThat( c.getInt(), is( 0xA1A2A3A4 ) );
        assertThat( c.getInt(), is( 1 ) );
        assertThat( c.getInt(), is( 2 ) );
        assertThat( c.getInt(), is( 0xB6B7B8B9 ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void getLongMustHitFirstCursorBeforeFlip()
    {
        PageCursor c = CompositePageCursor.compose( first, 8, second, 8 );
        assertThat( c.getLong(), is( 0xA0A1A2A3A4A5A6A7L ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void getLongMustHitSecondCursorAfterFlip()
    {
        PageCursor c = CompositePageCursor.compose( first, 8, second, 8 );
        assertThat( c.getLong(), is( 0xA0A1A2A3A4A5A6A7L ) );
        assertThat( c.getLong(), is( 0xB0B1B2B3B4B5B6B7L ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void getLongMustRespectOffsetIntoFirstCursor()
    {
        first.setOffset( 1 );
        PageCursor c = CompositePageCursor.compose( first, 8, second, 8 );
        assertThat( c.getLong(), is( 0xA1A2A3A4A5A6A7A8L ) );
        assertThat( c.getLong(), is( 0xB0B1B2B3B4B5B6B7L ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void getLongMustRespectOffsetIntoSecondCursor()
    {
        second.setOffset( 1 );
        PageCursor c = CompositePageCursor.compose( first, 8, second, 8 );
        assertThat( c.getLong(), is( 0xA0A1A2A3A4A5A6A7L ) );
        assertThat( c.getLong(), is( 0xB1B2B3B4B5B6B7B8L ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void putLongMustHitFirstCursorBeforeFlip()
    {
        PageCursor c = CompositePageCursor.compose( first, 8, second, 8 );
        c.putLong( (long) 1 );
        c.setOffset( 0 );
        assertThat( c.getLong(), is( (long) 1 ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void putLongMustHitSecondCursorAfterFlip()
    {
        PageCursor c = CompositePageCursor.compose( first, 8, second, 8 );
        c.putLong( (long) 1 );
        c.putLong( (long) 2 );
        c.setOffset( 8 );
        assertThat( c.getLong(), is( (long) 2 ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void putLongMustRespectOffsetIntoFirstCursor()
    {
        first.setOffset( 1 );
        PageCursor c = CompositePageCursor.compose( first, 8, second, 8 );
        c.putLong( (long) 1 );
        assertThat( first.getLong( 1 ), is( (long) 1 ) );
        assertThat( c.getLong(), is( 0xB0B1B2B3B4B5B6B7L ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void putLongMustRespectOffsetIntoSecondCursor()
    {
        second.setOffset( 1 );
        PageCursor c = CompositePageCursor.compose( first, 8, second, PAGE_SIZE );
        c.putLong( (long) 1 );
        c.putLong( (long) 2 );
        assertThat( second.getLong( 1 ), is( (long) 2 ) );
        assertThat( c.getLong(), is( 0xB9BABBBCBDBEBFC0L ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void getLongWithOffsetMustHitCorrectCursors()
    {
        first.setOffset( 1 );
        second.setOffset( 2 );
        PageCursor c = CompositePageCursor.compose( first, 1 + 8, second, 8 );
        assertThat( c.getLong( 1 ), is( 0xA2A3A4A5A6A7A8A9L ) );
        assertThat( c.getLong( 1 + 8 ), is( 0xB2B3B4B5B6B7B8B9L ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void putLongWithOffsetMustHitCorrectCursors()
    {
        first = generatePage( 0, PAGE_SIZE + 8, 0xA0 );
        second = generatePage( 0, PAGE_SIZE + 8, 0xC0 );
        first.setOffset( 1 );
        second.setOffset( 2 );
        PageCursor c = CompositePageCursor.compose( first, 2 * 8, second, 2 * 8 );
        c.putLong( 8, (long) 1 );
        c.putLong( 8 + 8, (long) 2 );
        assertThat( c.getLong(), is( 0xA1A2A3A4A5A6A7A8L ) );
        assertThat( c.getLong(), is( (long) 1 ) );
        assertThat( c.getLong(), is( (long) 2 ) );
        assertThat( c.getLong(), is( 0xCACBCCCDCECFD0D1L ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void getBytesMustHitFirstCursorBeforeFlip()
    {
        PageCursor c = CompositePageCursor.compose( first, 4, second, 4 );
        c.getBytes( bytes );
        assertThat( bytes, byteArray( 0xA0, 0xA1, 0xA2, 0xA3 ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void getBytesMustHitSecondCursorAfterFlip()
    {
        PageCursor c = CompositePageCursor.compose( first, 4, second, 4 );
        c.getBytes( bytes );
        assertThat( bytes, byteArray( 0xA0, 0xA1, 0xA2, 0xA3 ) );
        c.getBytes( bytes );
        assertThat( bytes, byteArray( 0xB0, 0xB1, 0xB2, 0xB3 ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void getBytesMustRespectOffsetIntoFirstCursor()
    {
        first.setOffset( 1 );
        PageCursor c = CompositePageCursor.compose( first, 4, second, 4 );
        c.getBytes( bytes );
        assertThat( bytes, byteArray( 0xA1, 0xA2, 0xA3, 0xA4 ) );
        c.getBytes( bytes );
        assertThat( bytes, byteArray( 0xB0, 0xB1, 0xB2, 0xB3 ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void getBytesMustRespectOffsetIntoSecondCursor()
    {
        second.setOffset( 1 );
        PageCursor c = CompositePageCursor.compose( first, 4, second, 4 );
        c.getBytes( bytes );
        assertThat( bytes, byteArray( 0xA0, 0xA1, 0xA2, 0xA3 ) );
        c.getBytes( bytes );
        assertThat( bytes, byteArray( 0xB1, 0xB2, 0xB3, 0xB4 ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void putBytesMustHitFirstCursorBeforeFlip()
    {
        PageCursor c = CompositePageCursor.compose( first, 4, second, 4 );
        c.putBytes( new byte[]{1, 2, 3, 4} );
        c.setOffset( 0 );
        c.getBytes( bytes );
        assertThat( bytes, byteArray( 1, 2, 3, 4 ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void putBytesMustHitSecondCursorAfterFlip()
    {
        PageCursor c = CompositePageCursor.compose( first, 1, second, 4 );
        c.putBytes( new byte[]{1} );
        c.putBytes( new byte[]{2} );
        c.setOffset( 1 );
        c.getBytes( bytes );
        assertThat( Arrays.copyOfRange( bytes, 0, 1 ), byteArray( 2 ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void putBytesMustRespectOffsetIntoFirstCursor()
    {
        first.setOffset( 1 );
        PageCursor c = CompositePageCursor.compose( first, 1, second, 4 );
        c.putBytes( new byte[]{1} );
        first.setOffset( 1 );
        first.getBytes( bytes );
        assertThat( Arrays.copyOfRange( bytes, 0, 1 ), byteArray( 1 ) );
        c.getBytes( bytes );
        assertThat( bytes, byteArray( 0xB0, 0xB1, 0xB2, 0xB3 ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void putBytesMustRespectOffsetIntoSecondCursor()
    {
        second.setOffset( 1 );
        PageCursor c = CompositePageCursor.compose( first, 1, second, 8 );
        c.putBytes( new byte[]{1} );
        c.putBytes( new byte[]{2} );
        second.setOffset( 1 );
        second.getBytes( bytes );
        assertThat( Arrays.copyOfRange( bytes, 0, 1 ), byteArray( 2 ) );
        c.getBytes( bytes );
        assertThat( bytes, byteArray( 0xB5, 0xB6, 0xB7, 0xB8 ) );
        assertFalse( c.checkAndClearBoundsFlag() );
    }

    @Test
    public void overlappingGetAccess()
    {
        PageCursor c = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        c.setOffset( PAGE_SIZE - 2 );
        assertThat( c.getInt(), is( 0xAEAFB0B1 ) );
        c.setOffset( PAGE_SIZE - 1 );
        assertThat( c.getShort(), is( (short) 0xAFB0 ) );
        c.setOffset( PAGE_SIZE - 4 );
        assertThat( c.getLong(), is( 0xACADAEAFB0B1B2B3L ) );
        c.setOffset( PAGE_SIZE - 2 );
        c.getBytes( bytes );
        assertThat( bytes, byteArray( 0xAE, 0xAF, 0xB0, 0xB1 ) );
    }

    @Test
    public void overlappingOffsettedGetAccess()
    {
        PageCursor c = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        assertThat( c.getInt( PAGE_SIZE - 2 ), is( 0xAEAFB0B1 ) );
        assertThat( c.getShort( PAGE_SIZE - 1 ), is( (short) 0xAFB0 ) );
        assertThat( c.getLong( PAGE_SIZE - 4 ), is( 0xACADAEAFB0B1B2B3L ) );
    }

    @Test
    public void overlappingPutAccess()
    {
        PageCursor c = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        c.setOffset( PAGE_SIZE - 2 );
        c.putInt( 0x01020304 );
        c.setOffset( PAGE_SIZE - 2 );
        assertThat( c.getInt(), is( 0x01020304 ) );

        c.setOffset( PAGE_SIZE - 1 );
        c.putShort( (short) 0x0102 );
        c.setOffset( PAGE_SIZE - 1 );
        assertThat( c.getShort(), is( (short) 0x0102 ) );

        c.setOffset( PAGE_SIZE - 4 );
        c.putLong( 0x0102030405060708L );
        c.setOffset( PAGE_SIZE - 4 );
        assertThat( c.getLong(), is( 0x0102030405060708L ) );

        c.setOffset( PAGE_SIZE - 2 );
        for ( int i = 0; i < bytes.length; i++ )
        {
            bytes[i] = (byte) (i + 1);
        }
        c.putBytes( bytes );
        c.setOffset( PAGE_SIZE - 2 );
        c.getBytes( bytes );
        assertThat( bytes, byteArray( 0x01, 0x02, 0x03, 0x04 ) );
    }

    @Test
    public void overlappingOffsettedPutAccess()
    {
        PageCursor c = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        c.putInt( PAGE_SIZE - 2, 0x01020304 );
        assertThat( c.getInt( PAGE_SIZE - 2 ), is( 0x01020304 ) );

        c.putShort( PAGE_SIZE - 1, (short) 0x0102 );
        assertThat( c.getShort( PAGE_SIZE - 1 ), is( (short) 0x0102 ) );

        c.putLong( PAGE_SIZE - 4, 0x0102030405060708L );
        assertThat( c.getLong( PAGE_SIZE - 4 ), is( 0x0102030405060708L ) );
    }

    @Test
    public void closeBothCursorsOnClose()
    {
        PageCursor pageCursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        pageCursor.close();

        assertTrue( first.isClosed() );
        assertTrue( second.isClosed() );
    }

    @Test( expected = UnsupportedOperationException.class )
    public void nextIsNotSupportedOperation() throws Exception
    {
        PageCursor pageCursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        pageCursor.next();
    }

    @Test( expected = UnsupportedOperationException.class )
    public void nextWithPageIdIsNotSupportedOperation() throws Exception
    {
        PageCursor pageCursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        pageCursor.next( 12 );
    }

    @Test
    public void rewindCompositeCursor()
    {
        first.setOffset( 1 );
        second.setOffset( 2 );
        PageCursor pageCursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );

        pageCursor.getLong();
        pageCursor.getLong();
        pageCursor.getLong();

        pageCursor.rewind();

        assertEquals( 0, pageCursor.getOffset() );
        assertEquals( 1, first.getOffset() );
        assertEquals( 2, second.getOffset() );
    }

    @Test
    public void getOffsetMustReturnOffsetIntoView()
    {
        PageCursor pageCursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        pageCursor.getLong();
        assertThat( pageCursor.getOffset(), is( 8 ) );
        pageCursor.getLong();
        pageCursor.getLong();
        assertThat( pageCursor.getOffset(), is( 24 ) );
    }

    @Test
    public void setOffsetMustSetOffsetIntoView()
    {
        PageCursor pageCursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        pageCursor.setOffset( 13 );
        assertThat( first.getOffset(), is( 13 ) );
        assertThat( second.getOffset(), is( 0 ) );
        pageCursor.setOffset( 18 ); // beyond first page cursor
        assertThat( first.getOffset(), is( PAGE_SIZE ) );
        assertThat( second.getOffset(), is( 18 - PAGE_SIZE ) );
    }

    @Test
    public void raisingOutOfBoundsFlagMustRaiseOutOfBoundsFlag()
    {
        PageCursor pageCursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        pageCursor.raiseOutOfBounds();
        assertTrue( pageCursor.checkAndClearBoundsFlag() );
        assertFalse( pageCursor.checkAndClearBoundsFlag() );
    }

    @Test( expected = UnsupportedOperationException.class )
    public void currentPageSizeIsUnsupported()
    {
        PageCursor pageCursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        pageCursor.getCurrentPageSize();
    }

    @Test
    public void pageIdEqualFirstCursorPageIdBeforeFlip()
    {
        PageCursor pageCursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        assertEquals( first.getCurrentPageId(), pageCursor.getCurrentPageId() );

        pageCursor.getLong();
        assertEquals( first.getCurrentPageId(), pageCursor.getCurrentPageId() );

        pageCursor.getLong();
        assertNotEquals( first.getCurrentPageId(), pageCursor.getCurrentPageId() );
    }

    @Test
    public void pageIdEqualSecondCursorPageIdAfterFlip()
    {
        PageCursor pageCursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        assertNotEquals( second.getCurrentPageId(), pageCursor.getCurrentPageId() );

        pageCursor.getLong();
        assertNotEquals( second.getCurrentPageId(), pageCursor.getCurrentPageId() );

        pageCursor.getLong();
        assertEquals( second.getCurrentPageId(), pageCursor.getCurrentPageId() );
    }

    @Test
    public void retryShouldCheckAndResetBothCursors() throws Exception
    {
        PageCursor pageCursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );

        assertFalse( pageCursor.shouldRetry() );

        first.setNeedsRetry( true );
        assertTrue( pageCursor.shouldRetry() );

        first.setNeedsRetry( false );
        assertFalse( pageCursor.shouldRetry() );

        second.setNeedsRetry( true );
        assertTrue( pageCursor.shouldRetry() );
    }

    @Test
    public void retryMustResetOffsetsInBothCursors() throws Exception
    {
        first.setOffset( 1 );
        second.setOffset( 2 );
        PageCursor pageCursor = CompositePageCursor.compose( first, 8, second, 8 );

        pageCursor.setOffset( 5 );
        first.setOffset( 3 );
        second.setOffset( 4 );
        first.setNeedsRetry( true );
        pageCursor.shouldRetry();
        assertThat( first.getOffset(), is( 1 ) );
        assertThat( second.getOffset(), is( 2 ) );
        assertThat( pageCursor.getOffset(), is( 0 ) );

        pageCursor.setOffset( 5 );
        first.setOffset( 3 );
        second.setOffset( 4 );
        first.setNeedsRetry( false );
        second.setNeedsRetry( true );
        pageCursor.shouldRetry();
        assertThat( first.getOffset(), is( 1 ) );
        assertThat( second.getOffset(), is( 2 ) );
        assertThat( pageCursor.getOffset(), is( 0 ) );
    }

    @Test
    public void retryMustClearTheOutOfBoundsFlags() throws Exception
    {
        PageCursor pageCursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        first.raiseOutOfBounds();
        second.raiseOutOfBounds();
        pageCursor.raiseOutOfBounds();
        first.setNeedsRetry( true );
        pageCursor.shouldRetry();
        assertFalse( first.checkAndClearBoundsFlag() );
        assertFalse( second.checkAndClearBoundsFlag() );
        assertFalse( pageCursor.checkAndClearBoundsFlag() );
    }

    @Test
    public void checkAndClearCompositeBoundsFlagMustClearFirstBoundsFlag()
    {
        PageCursor pageCursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        first.raiseOutOfBounds();
        assertTrue( pageCursor.checkAndClearBoundsFlag() );
        assertFalse( pageCursor.checkAndClearBoundsFlag() );
        assertFalse( first.checkAndClearBoundsFlag() );
    }

    @Test
    public void checkAndClearCompositeBoundsFlagMustClearSecondBoundsFlag()
    {
        PageCursor pageCursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        second.raiseOutOfBounds();
        assertTrue( pageCursor.checkAndClearBoundsFlag() );
        assertFalse( pageCursor.checkAndClearBoundsFlag() );
        assertFalse( second.checkAndClearBoundsFlag() );
    }

    @Test
    public void composeMustNotThrowIfFirstLengthExpandsBeyondFirstPage()
    {
        CompositePageCursor.compose( first, Integer.MAX_VALUE, second, PAGE_SIZE );
    }

    @Test
    public void composeMustNotThrowIfSecondLengthExpandsBeyondSecondPage()
    {
        CompositePageCursor.compose( first, PAGE_SIZE, second, Integer.MAX_VALUE );
    }

    @Test( expected = UnsupportedOperationException.class )
    public void compositeCursorDoesNotSupportCopyTo()
    {
        PageCursor pageCursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        pageCursor.copyTo( 0, new StubPageCursor( 0, 7 ), 89, 6 );
    }

    @Test
    public void getByteBeyondEndOfViewMustRaiseBoundsFlag()
    {
        PageCursor pageCursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        for ( int i = 0; i < 3 * PAGE_SIZE; i++ )
        {
            pageCursor.getByte();
        }
        assertTrue( pageCursor.checkAndClearBoundsFlag() );
        assertFalse( pageCursor.checkAndClearBoundsFlag() );
    }

    @Test
    public void getByteOffsettedBeyondEndOfViewMustRaiseBoundsFlag()
    {
        PageCursor pageCursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        for ( int i = 0; i < 3 * PAGE_SIZE; i++ )
        {
            pageCursor.getByte( i );
        }
        assertTrue( pageCursor.checkAndClearBoundsFlag() );
        assertFalse( pageCursor.checkAndClearBoundsFlag() );
    }

    @Test
    public void putByteBeyondEndOfViewMustRaiseBoundsFlag()
    {
        PageCursor pageCursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        for ( int i = 0; i < 3 * PAGE_SIZE; i++ )
        {
            pageCursor.putByte( (byte) 1 );
        }
        assertTrue( pageCursor.checkAndClearBoundsFlag() );
        assertFalse( pageCursor.checkAndClearBoundsFlag() );
    }

    @Test
    public void putByteOffsettedBeyondEndOfViewMustRaiseBoundsFlag()
    {
        PageCursor pageCursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        for ( int i = 0; i < 3 * PAGE_SIZE; i++ )
        {
            pageCursor.putByte( i, (byte) 1 );
        }
        assertTrue( pageCursor.checkAndClearBoundsFlag() );
        assertFalse( pageCursor.checkAndClearBoundsFlag() );
    }

    @Test
    public void getByteOffsettedBeforeFirstPageMustRaiseBoundsFlag()
    {
        PageCursor pageCursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        pageCursor.getByte( -1 );
        assertTrue( pageCursor.checkAndClearBoundsFlag() );
        assertFalse( pageCursor.checkAndClearBoundsFlag() );
    }

    @Test
    public void putByteOffsettedBeforeFirstPageMustRaiseBoundsFlag()
    {
        PageCursor pageCursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        pageCursor.putByte( -1, (byte) 1 );
        assertTrue( pageCursor.checkAndClearBoundsFlag() );
        assertFalse( pageCursor.checkAndClearBoundsFlag() );
    }

    @Test
    public void getShortBeyondEndOfViewMustRaiseBoundsFlag()
    {
        PageCursor pageCursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        for ( int i = 0; i < 3 * PAGE_SIZE; i++ )
        {
            pageCursor.getShort();
        }
        assertTrue( pageCursor.checkAndClearBoundsFlag() );
        assertFalse( pageCursor.checkAndClearBoundsFlag() );
    }

    @Test
    public void getShortOffsettedBeyondEndOfViewMustRaiseBoundsFlag()
    {
        PageCursor pageCursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        for ( int i = 0; i < 3 * PAGE_SIZE; i++ )
        {
            pageCursor.getShort( i );
        }
        assertTrue( pageCursor.checkAndClearBoundsFlag() );
        assertFalse( pageCursor.checkAndClearBoundsFlag() );
    }

    @Test
    public void putShortBeyondEndOfViewMustRaiseBoundsFlag()
    {
        PageCursor pageCursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        for ( int i = 0; i < 3 * PAGE_SIZE; i++ )
        {
            pageCursor.putShort( (short) 1 );
        }
        assertTrue( pageCursor.checkAndClearBoundsFlag() );
        assertFalse( pageCursor.checkAndClearBoundsFlag() );
    }

    @Test
    public void putShortOffsettedBeyondEndOfViewMustRaiseBoundsFlag()
    {
        PageCursor pageCursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        for ( int i = 0; i < 3 * PAGE_SIZE; i++ )
        {
            pageCursor.putShort( i, (short) 1 );
        }
        assertTrue( pageCursor.checkAndClearBoundsFlag() );
        assertFalse( pageCursor.checkAndClearBoundsFlag() );
    }

    @Test
    public void getShortOffsettedBeforeFirstPageMustRaiseBoundsFlag()
    {
        PageCursor pageCursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        pageCursor.getShort( -1 );
        assertTrue( pageCursor.checkAndClearBoundsFlag() );
        assertFalse( pageCursor.checkAndClearBoundsFlag() );
    }

    @Test
    public void putShortOffsettedBeforeFirstPageMustRaiseBoundsFlag()
    {
        PageCursor pageCursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        pageCursor.putShort( -1, (short) 1 );
        assertTrue( pageCursor.checkAndClearBoundsFlag() );
        assertFalse( pageCursor.checkAndClearBoundsFlag() );
    }

    @Test
    public void getIntBeyondEndOfViewMustRaiseBoundsFlag()
    {
        PageCursor pageCursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        for ( int i = 0; i < 3 * PAGE_SIZE; i++ )
        {
            pageCursor.getInt();
        }
        assertTrue( pageCursor.checkAndClearBoundsFlag() );
        assertFalse( pageCursor.checkAndClearBoundsFlag() );
    }

    @Test
    public void getIntOffsettedBeyondEndOfViewMustRaiseBoundsFlag()
    {
        PageCursor pageCursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        for ( int i = 0; i < 3 * PAGE_SIZE; i++ )
        {
            pageCursor.getInt( i );
        }
        assertTrue( pageCursor.checkAndClearBoundsFlag() );
        assertFalse( pageCursor.checkAndClearBoundsFlag() );
    }

    @Test
    public void putIntBeyondEndOfViewMustRaiseBoundsFlag()
    {
        PageCursor pageCursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        for ( int i = 0; i < 3 * PAGE_SIZE; i++ )
        {
            pageCursor.putInt( 1 );
        }
        assertTrue( pageCursor.checkAndClearBoundsFlag() );
        assertFalse( pageCursor.checkAndClearBoundsFlag() );
    }

    @Test
    public void putIntOffsettedBeyondEndOfViewMustRaiseBoundsFlag()
    {
        PageCursor pageCursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        for ( int i = 0; i < 3 * PAGE_SIZE; i++ )
        {
            pageCursor.putInt( i, 1 );
        }
        assertTrue( pageCursor.checkAndClearBoundsFlag() );
        assertFalse( pageCursor.checkAndClearBoundsFlag() );
    }

    @Test
    public void getIntOffsettedBeforeFirstPageMustRaiseBoundsFlag()
    {
        PageCursor pageCursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        pageCursor.getInt( -1 );
        assertTrue( pageCursor.checkAndClearBoundsFlag() );
        assertFalse( pageCursor.checkAndClearBoundsFlag() );
    }

    @Test
    public void putIntOffsettedBeforeFirstPageMustRaiseBoundsFlag()
    {
        PageCursor pageCursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        pageCursor.putInt( -1, 1 );
        assertTrue( pageCursor.checkAndClearBoundsFlag() );
        assertFalse( pageCursor.checkAndClearBoundsFlag() );
    }

    @Test
    public void getLongBeyondEndOfViewMustRaiseBoundsFlag()
    {
        PageCursor pageCursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        for ( int i = 0; i < 3 * PAGE_SIZE; i++ )
        {
            pageCursor.getLong();
        }
        assertTrue( pageCursor.checkAndClearBoundsFlag() );
        assertFalse( pageCursor.checkAndClearBoundsFlag() );
    }

    @Test
    public void getLongOffsettedBeyondEndOfViewMustRaiseBoundsFlag()
    {
        PageCursor pageCursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        for ( int i = 0; i < 3 * PAGE_SIZE; i++ )
        {
            pageCursor.getLong( i );
        }
        assertTrue( pageCursor.checkAndClearBoundsFlag() );
        assertFalse( pageCursor.checkAndClearBoundsFlag() );
    }

    @Test
    public void putLongBeyondEndOfViewMustRaiseBoundsFlag()
    {
        PageCursor pageCursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        for ( int i = 0; i < 3 * PAGE_SIZE; i++ )
        {
            pageCursor.putLong( (long) 1 );
        }
        assertTrue( pageCursor.checkAndClearBoundsFlag() );
        assertFalse( pageCursor.checkAndClearBoundsFlag() );
    }

    @Test
    public void putLongOffsettedBeyondEndOfViewMustRaiseBoundsFlag()
    {
        PageCursor pageCursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        for ( int i = 0; i < 3 * PAGE_SIZE; i++ )
        {
            pageCursor.putLong( i, (long) 1 );
        }
        assertTrue( pageCursor.checkAndClearBoundsFlag() );
        assertFalse( pageCursor.checkAndClearBoundsFlag() );
    }

    @Test
    public void getLongOffsettedBeforeFirstPageMustRaiseBoundsFlag()
    {
        PageCursor pageCursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        pageCursor.getLong( -1 );
        assertTrue( pageCursor.checkAndClearBoundsFlag() );
        assertFalse( pageCursor.checkAndClearBoundsFlag() );
    }

    @Test
    public void putLongOffsettedBeforeFirstPageMustRaiseBoundsFlag()
    {
        PageCursor pageCursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        pageCursor.putLong( -1, (long) 1 );
        assertTrue( pageCursor.checkAndClearBoundsFlag() );
        assertFalse( pageCursor.checkAndClearBoundsFlag() );
    }

    @Test
    public void getByteArrayBeyondEndOfViewMustRaiseBoundsFlag()
    {
        PageCursor pageCursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        for ( int i = 0; i < 3 * PAGE_SIZE; i++ )
        {
            pageCursor.getBytes( bytes );
        }
        assertTrue( pageCursor.checkAndClearBoundsFlag() );
        assertFalse( pageCursor.checkAndClearBoundsFlag() );
    }

    @Test
    public void putByteArrayBeyondEndOfViewMustRaiseBoundsFlag()
    {
        PageCursor pageCursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        for ( int i = 0; i < 3 * PAGE_SIZE; i++ )
        {
            pageCursor.putBytes( bytes );
        }
        assertTrue( pageCursor.checkAndClearBoundsFlag() );
        assertFalse( pageCursor.checkAndClearBoundsFlag() );
    }

    @Test
    public void setCursorErrorMustApplyToCursorAtCurrentOffset()
    {
        PageCursor cursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        String firstMsg = "first boo";
        String secondMsg = "second boo";

        cursor.setCursorException( firstMsg );
        assertFalse( cursor.checkAndClearBoundsFlag() );
        try
        {
            first.checkAndClearCursorException();
            fail( "first checkAndClearCursorError should have thrown" );
        }
        catch ( CursorException e )
        {
            assertThat( e.getMessage(), is( firstMsg ) );
        }

        cursor.setOffset( PAGE_SIZE );
        cursor.setCursorException( secondMsg );
        assertFalse( cursor.checkAndClearBoundsFlag() );
        try
        {
            second.checkAndClearCursorException();
            fail( "second checkAndClearCursorError should have thrown" );
        }
        catch ( CursorException e )
        {
            assertThat( e.getMessage(), is( secondMsg ) );
        }
    }

    @Test
    public void checkAndClearCursorErrorMustNotThrowIfNoErrorsAreSet() throws Exception
    {
        PageCursor cursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        cursor.checkAndClearCursorException();
    }

    @Test
    public void checkAndClearCursorErrorMustThrowIfFirstCursorHasError()
    {
        PageCursor cursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        first.setCursorException( "boo" );
        try
        {
            cursor.checkAndClearCursorException();
            fail( "composite cursor checkAndClearCursorError should have thrown" );
        }
        catch ( CursorException e )
        {
            assertThat( e.getMessage(), is( "boo" ) );
        }
    }

    @Test
    public void checkAndClearCursorErrorMustThrowIfSecondCursorHasError()
    {
        PageCursor cursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        second.setCursorException( "boo" );
        try
        {
            cursor.checkAndClearCursorException();
            fail( "composite cursor checkAndClearCursorError should have thrown" );
        }
        catch ( CursorException e )
        {
            assertThat( e.getMessage(), is( "boo" ) );
        }
    }

    @Test
    public void checkAndClearCursorErrorWillOnlyCheckFirstCursorIfBothHaveErrorsSet()
    {
        PageCursor cursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        first.setCursorException( "first boo" );
        second.setCursorException( "second boo" );
        try
        {
            cursor.checkAndClearCursorException();
            fail( "composite cursor checkAndClearCursorError should have thrown" );
        }
        catch ( CursorException e )
        {
            assertThat( e.getMessage(), is( "first boo" ) );
        }
        try
        {
            second.checkAndClearCursorException();
            fail( "second cursor checkAndClearCursorError should have thrown" );
        }
        catch ( CursorException e )
        {
            assertThat( e.getMessage(), is( "second boo" ) );
        }
    }

    @Test
    public void clearCursorErrorMustClearBothCursors() throws Exception
    {
        PageCursor cursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        first.setCursorException( "first boo" );
        second.setCursorException( "second boo" );
        cursor.clearCursorException();

        // Now these must not throw
        first.checkAndClearCursorException();
        second.checkAndClearCursorException();
        cursor.checkAndClearCursorException();
    }

    @Test
    public void isWriteLockedMustBeTrueIfBothCursorsAreWriteLocked()
    {
        PageCursor cursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        first.setWriteLocked( true );
        second.setWriteLocked( true );
        assertTrue( cursor.isWriteLocked() );
    }

    @Test
    public void isWriteLockedMustBeFalseIfBothCursorsAreNotWriteLocked()
    {
        PageCursor cursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        first.setWriteLocked( false );
        second.setWriteLocked( false );
        assertFalse( cursor.isWriteLocked() );
    }

    @Test
    public void isWriteLockedMustBeFalseIfFirstCursorIsNotWriteLocked()
    {
        PageCursor cursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        first.setWriteLocked( false );
        second.setWriteLocked( true );
        assertFalse( cursor.isWriteLocked() );
    }

    @Test
    public void isWriteLockedMustBeFalseIfSecondCursorIsNotWriteLocked()
    {
        PageCursor cursor = CompositePageCursor.compose( first, PAGE_SIZE, second, PAGE_SIZE );
        first.setWriteLocked( true );
        second.setWriteLocked( false );
        assertFalse( cursor.isWriteLocked() );
    }
}
