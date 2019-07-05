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
package org.neo4j.kernel.impl.store.kvstore;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KeyValueWriterTest
{
    private static final int ENTRIES_PER_PAGE = 4 * 1024 / 16;
    @SuppressWarnings( "unchecked" )
    private StubCollector collector = new StubCollector( ENTRIES_PER_PAGE );
    private final StubWriter stub = new StubWriter();
    private KeyValueWriter writer = new KeyValueWriter( collector, stub );
    private final BigEndianByteArrayBuffer key = new BigEndianByteArrayBuffer( new byte[8] );
    private final BigEndianByteArrayBuffer value = new BigEndianByteArrayBuffer( new byte[8] );

    @AfterEach
    void closeWriter() throws IOException
    {
        writer.close();
    }

    @Test
    void shouldAcceptNoHeadersAndNoData() throws Exception
    {
        // given
        value.putByte( 0, (byte) 0x7F );
        value.putByte( 7, (byte) 0x7F );

        // when
        assertTrue( writer.writeHeader( key, value ), "format specifier" );
        assertTrue( writer.writeHeader( key, value ), "end-of-header marker" );
        assertTrue( writer.writeHeader( key, value ), "end marker + number of data items" );

        // then

        stub.assertData( new byte[]{0x00, 0, 0, 0, 0, 0, 0, 0x00, // width specifier
                                    0x7F, 0, 0, 0, 0, 0, 0, 0x7F, // format specifier
                                    0x00, 0, 0, 0, 0, 0, 0, 0x00, // end-of-header marker
                                    0x00, 0, 0, 0, 0, 0, 0, 0x00, // zero padding
                                    0x00, 0, 0, 0, 0, 0, 0, 0x00, // end marker
                                    0x00, 0, 0, 0, 0, 0, 0, 0x00, // number of data items
                                    } );
    }

    @Test
    void shouldRequireNonZeroFormatSpecifier() throws Exception
    {
        assertFalse( writer.writeHeader( key, value ), "format-specifier" );
    }

    @Test
    void shouldRejectInvalidHeaderKeyWhenAssertionsAreEnabled() throws Exception
    {
        // given
        key.putByte( 3, (byte) 1 );
        value.putByte( 0, (byte) 0x7F );
        value.putByte( 7, (byte) 0x7F );

        // when
        var e = assertThrows( AssertionError.class, () -> writer.writeHeader( key, value ) );
        assertEquals( "key should have been cleared by previous call", e.getMessage() );
    }

    @Test
    void shouldRejectInvalidDataKey() throws Exception
    {
        // given
        value.putByte( 0, (byte) 0x7F );
        value.putByte( 7, (byte) 0x7F );
        writer.writeHeader( key, value );
        writer.writeHeader( key, value );

        var e = assertThrows( IllegalArgumentException.class, () -> writer.writeData( key, value ) );
        assertEquals( "All-zero keys are not allowed.", e.getMessage() );
    }

    @Test
    void shouldRejectDataBeforeHeaders()
    {
        // given
        key.putByte( 2, (byte) 0x77 );

        var e = assertThrows( IllegalStateException.class, () -> writer.writeData( key, value ) );
        assertEquals( "Cannot write data when expecting format specifier.", e.getMessage() );
    }

    @Test
    void shouldRejectDataAfterInsufficientHeaders() throws Exception
    {
        // given
        value.fill( (byte) 0xFF );
        assertTrue( writer.writeHeader( key, value ) );
        key.putByte( 2, (byte) 0x77 );

        var e = assertThrows( IllegalStateException.class, () -> writer.writeData( key, value ) );
        assertEquals( "Cannot write data when expecting header.", e.getMessage() );
    }

    @Test
    void shouldNotOpenStoreFileIfWritingHasNotCompleted() throws Exception
    {
        for ( int i = 0; i <= 10; i++ )
        {
            // given
            String[] headers;
            switch ( i )
            {
            case 0:
            case 1:
            case 8:
            case 9:
            case 10:
                headers = new String[0];
                break;
            case 2:
                headers = new String[]{"foo"};
                break;
            default:
                headers = new String[]{"foo", "bar"};
                break;
            }
            resetWriter( headers );
            for ( int field = 1; field <= i; field++ )
            {
                switch ( field )
                {
                // header fields
                case 3:
                    if ( i >= 8 ) // no headers
                    {
                        writer.writeHeader( key, value ); // padding
                    }
                case 2:
                    if ( i >= 8 ) // no headers
                    {
                        break;
                    }
                case 1:
                    value.putByte( 0, (byte) 0x7F );
                    value.putByte( 7, (byte) 0x7F );
                    writer.writeHeader( key, value );
                    break;
                default: // data fields
                    if ( (i < 8) || (field > 8) )
                    {
                        key.putByte( key.size() - 1, (byte) field );
                        writer.writeData( key, value );
                    }
                }
            }

            var e = assertThrows( IllegalStateException.class, () -> writer.openStoreFile() );
            assertThat( e.getMessage(), startsWith( "Cannot open store file when " ) );
        }
    }

    private void resetWriter( String... header )
    {
        collector = new StubCollector( ENTRIES_PER_PAGE, header );
        writer = new KeyValueWriter( collector, stub );
    }

    private static class StubWriter extends KeyValueWriter.Writer
    {
        IOException next;
        ByteArrayOutputStream data = new ByteArrayOutputStream();

        @Override
        void write( byte[] data ) throws IOException
        {
            io();
            this.data.write( data );
        }

        @Override
        KeyValueStoreFile open( Metadata metadata, int keySize, int valueSize )
        {
            return null;
        }

        @Override
        void close() throws IOException
        {
            io();
        }

        void assertData( byte... expected )
        {
            assertArrayEquals( expected, this.data.toByteArray() );
        }

        private void io() throws IOException
        {
            try
            {
                if ( next != null )
                {
                    throw next;
                }
            }
            finally
            {
                next = null;
            }
        }
    }
}
