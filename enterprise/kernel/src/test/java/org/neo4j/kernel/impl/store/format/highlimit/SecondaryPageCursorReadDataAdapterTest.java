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
package org.neo4j.kernel.impl.store.format.highlimit;

import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.StubPageCursor;
import org.neo4j.test.RandomRule;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SecondaryPageCursorReadDataAdapterTest
{
    @Rule
    public final RandomRule random = new RandomRule();

    @Test
    public void getShortEntirelyFromPrimaryCursor() throws Exception
    {
        int secondaryPageId = 42;
        int secondaryCursorOffset = 0;
        int primaryCursorEndOffset = Short.BYTES + 1; // full short can fit into the primary cursor
        short value = (short) random.nextInt();

        PageCursor primaryCursor = newPageCursor();
        PageCursor secondaryCursor = newPageCursor();

        writeShortToPrimary( primaryCursor, value );

        SecondaryPageCursorReadDataAdapter adapter = new SecondaryPageCursorReadDataAdapter( primaryCursor,
                newPagedFile( secondaryCursor ), secondaryPageId, secondaryCursorOffset, primaryCursorEndOffset,
                PagedFile.PF_SHARED_READ_LOCK );

        short read = adapter.getShort( primaryCursor );

        assertEquals( value, read );
    }

    @Test
    public void getShortEntirelyFromSecondaryCursor() throws Exception
    {
        int secondaryPageId = 42;
        int secondaryCursorOffset = 0;
        int primaryCursorEndOffset = 0; // can't read anything else from the primary cursor
        short value = (short) random.nextInt();

        PageCursor primaryCursor = newPageCursor();
        PageCursor secondaryCursor = newPageCursor();

        writeShortToSecondary( secondaryCursor, value );

        SecondaryPageCursorReadDataAdapter adapter = new SecondaryPageCursorReadDataAdapter( primaryCursor,
                newPagedFile( secondaryCursor ), secondaryPageId, secondaryCursorOffset, primaryCursorEndOffset,
                PagedFile.PF_SHARED_READ_LOCK );

        short read = adapter.getShort( primaryCursor );

        assertEquals( value, read );
    }

    @Test
    public void getShortFromPrimaryAndSecondaryCursor() throws Exception
    {
        int secondaryPageId = 42;
        int secondaryCursorOffset = 0;
        int primaryCursorEndOffset = Short.BYTES / 2; // only one byte can be read from the primary cursor

        short value = (short) random.nextInt();
        ByteBuffer buffer = newByteBuffer( Short.BYTES );
        buffer.putShort( value );
        buffer.flip();

        PageCursor primaryCursor = newPageCursor();
        PageCursor secondaryCursor = newPageCursor();

        writeByteToPrimary( primaryCursor, buffer.get() );
        writeByteToSecondary( secondaryCursor, buffer.get() );

        SecondaryPageCursorReadDataAdapter adapter = new SecondaryPageCursorReadDataAdapter( primaryCursor,
                newPagedFile( secondaryCursor ), secondaryPageId, secondaryCursorOffset, primaryCursorEndOffset,
                PagedFile.PF_SHARED_READ_LOCK );

        short read = adapter.getShort( primaryCursor );

        assertEquals( value, read );
    }

    private static void writeShortToPrimary( PageCursor cursor, short value )
    {
        int offset = cursor.getOffset();
        cursor.putShort( value );
        cursor.setOffset( offset );
    }

    private static void writeByteToPrimary( PageCursor cursor, byte value )
    {
        int offset = cursor.getOffset();
        cursor.putByte( value );
        cursor.setOffset( offset );
    }

    private static void writeShortToSecondary( PageCursor cursor, short value )
    {
        int offset = cursor.getOffset();
        cursor.putByte( (byte) 0 ); // put dummy header byte for the secondary record unit
        cursor.putShort( value );
        cursor.setOffset( offset );
    }

    private static void writeByteToSecondary( PageCursor cursor, byte value )
    {
        int offset = cursor.getOffset();
        cursor.putByte( (byte) 0 ); // put dummy header byte for the secondary record unit
        cursor.putByte( value );
        cursor.setOffset( offset );
    }

    private static PagedFile newPagedFile( PageCursor cursor ) throws IOException
    {
        PagedFile pagedFile = mock( PagedFile.class );
        when( pagedFile.io( anyLong(), anyInt() ) ).thenReturn( cursor );
        return pagedFile;
    }

    private static PageCursor newPageCursor()
    {
        return new StubPageCursor( 0, newByteBuffer( 100 ) );
    }

    private static ByteBuffer newByteBuffer( int capacity )
    {
        return ByteBuffer.allocateDirect( capacity );
    }
}
