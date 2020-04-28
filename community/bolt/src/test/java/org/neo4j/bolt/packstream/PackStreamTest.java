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
package org.neo4j.bolt.packstream;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.bolt.packstream.PackType.BYTES;
import static org.neo4j.bolt.packstream.PackType.LIST;
import static org.neo4j.bolt.packstream.PackType.MAP;
import static org.neo4j.bolt.packstream.PackType.STRING;

class PackStreamTest
{

    private static Map<String,Object> asMap( Object... keysAndValues )
    {
        Map<String,Object> map = new LinkedHashMap<>( keysAndValues.length / 2 );
        String key = null;
        for ( Object keyOrValue : keysAndValues )
        {
            if ( key == null )
            {
                key = keyOrValue.toString();
            }
            else
            {
                map.put( key, keyOrValue );
                key = null;
            }
        }
        return map;
    }

    private static class Machine
    {
        private final ByteArrayOutputStream output;
        private final WritableByteChannel writable;
        private final PackStream.Packer packer;

        Machine()
        {
            this.output = new ByteArrayOutputStream();
            this.writable = Channels.newChannel( this.output );
            this.packer = new PackStream.Packer( new BufferedChannelOutput( this.writable ) );
        }

        Machine( int bufferSize )
        {
            this.output = new ByteArrayOutputStream();
            this.writable = Channels.newChannel( this.output );
            this.packer = new PackStream.Packer( new BufferedChannelOutput( this.writable, bufferSize ) );
        }

        public void reset()
        {
            output.reset();
        }

        public byte[] output()
        {
            return output.toByteArray();
        }

        public PackStream.Packer packer()
        {
            return packer;
        }
    }

    private static class MachineClient
    {
        private final PackStream.Unpacker unpacker;
        private final ResetableReadableByteChannel readable;

        MachineClient( int capacity )
        {
            readable = new ResetableReadableByteChannel();
            BufferedChannelInput input = new BufferedChannelInput( capacity ).reset( readable );
            unpacker = new PackStream.Unpacker( input );
        }

        public void reset( byte[] input )
        {
            readable.reset( input );
        }

        public PackStream.Unpacker unpacker()
        {
            return this.unpacker;
        }
    }

    private static class ResetableReadableByteChannel implements ReadableByteChannel
    {
        private byte[] bytes;
        private int pos;

        public void reset( byte[] input )
        {
            bytes = input;
            pos = 0;
        }

        @Override
        public int read( ByteBuffer dst )
        {
            dst.put( bytes );
            int read = bytes.length;
            pos += read;
            return read;
        }

        @Override
        public boolean isOpen()
        {
            return pos < bytes.length;
        }

        @Override
        public void close()
        {
        }
    }

    private PackStream.Unpacker newUnpacker( byte[] bytes )
    {
        ByteArrayInputStream input = new ByteArrayInputStream( bytes );
        return new PackStream.Unpacker( new BufferedChannelInput( 16 ).reset( Channels.newChannel( input ) ) );
    }

    @Test
    void testCanPackAndUnpackNull() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        // When
        machine.packer().packNull();
        machine.packer().flush();

        // Then
        byte[] bytes = machine.output();
        assertThat( bytes ).isEqualTo( new byte[]{(byte) 0xC0} );

        // When
        PackStream.Unpacker unpacker = newUnpacker( bytes );
        unpacker.unpackNull();

        // Then
        // it does not blow up
    }

    @Test
    void testCanPackAndUnpackTrue() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        // When
        machine.packer().pack( true );
        machine.packer().flush();

        // Then
        byte[] bytes = machine.output();
        assertThat( bytes ).isEqualTo( new byte[]{(byte) 0xC3} );

        // When
        PackStream.Unpacker unpacker = newUnpacker( bytes );

        // Then
        assertThat( unpacker.unpackBoolean() ).isEqualTo( true );

    }

    @Test
    void testCanPackAndUnpackFalse() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        // When
        machine.packer().pack( false );
        machine.packer().flush();

        // Then
        byte[] bytes = machine.output();
        assertThat( bytes ).isEqualTo( new byte[]{(byte) 0xC2} );

        // When
        PackStream.Unpacker unpacker = newUnpacker( bytes );

        // Then
        assertThat( unpacker.unpackBoolean() ).isEqualTo( false );

    }

    @Test
    void testCanPackAndUnpackTinyIntegers() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        for ( long i = -16; i < 128; i++ )
        {
            // When
            machine.reset();
            machine.packer().pack( i );
            machine.packer().flush();

            // Then
            byte[] bytes = machine.output();
            assertThat( bytes.length ).isEqualTo( 1 );

            // When
            PackStream.Unpacker unpacker = newUnpacker( bytes );

            // Then
            assertThat( unpacker.unpackLong() ).isEqualTo( i );
        }
    }

    @Test
    void testCanPackAndUnpackShortIntegers() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        for ( long i = -32768; i < 32768; i++ )
        {
            // When
            machine.reset();
            machine.packer().pack( i );
            machine.packer().flush();

            // Then
            byte[] bytes = machine.output();
            assertThat( bytes.length ).isLessThanOrEqualTo( 3 );

            // When
            PackStream.Unpacker unpacker = newUnpacker( bytes );

            // Then
            assertThat( unpacker.unpackLong() ).isEqualTo( i );
        }
    }

    @Test
    void testCanPackAndUnpackPowersOfTwoAsIntegers() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        for ( int i = 0; i < 32; i++ )
        {
            long n = (long) Math.pow( 2, i );

            // When
            machine.reset();
            machine.packer().pack( n );
            machine.packer().flush();

            // Then
            long value = newUnpacker( machine.output() ).unpackLong();
            assertThat( value ).isEqualTo( n );
        }
    }

    @Test
    void testCanPackAndUnpackPowersOfTwoPlusABitAsDoubles() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        for ( int i = 0; i < 32; i++ )
        {
            double n = Math.pow( 2, i ) + 0.5;

            // When
            machine.reset();
            machine.packer().pack( n );
            machine.packer().flush();

            double value = newUnpacker( machine.output() ).unpackDouble();

            // Then
            assertThat( value ).isEqualTo( n );
        }
    }

    @Test
    void testCanPackAndUnpackPowersOfTwoMinusABitAsDoubles() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        for ( int i = 0; i < 32; i++ )
        {
            double n = Math.pow( 2, i ) - 0.5;

            // When
            machine.reset();
            machine.packer().pack( n );
            machine.packer().flush();

            // Then
            double value = newUnpacker( machine.output() ).unpackDouble();
            assertThat( value ).isEqualTo( n );
        }
    }

    @Test
    void testCanPackCommonlyUsedCharAndUnpackAsString() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        for ( int i = 32; i < 127; i++ )
        {
            char c = (char) i;

            // When
            machine.reset();
            machine.packer().pack( c );
            machine.packer().flush();

            // Then
            String value = newUnpacker( machine.output() ).unpackString();
            assertThat( value ).isEqualTo( String.valueOf( c ) );
        }
    }

    @Test
    void testCanPackRandomCharAndUnpackAsString() throws Throwable
    {
        // Given
        Machine machine = new Machine();
        char[] chars = {'ø', 'å', '´', '†', 'œ', '≈'};

        for ( char c : chars )
        {
            // When
            machine.reset();
            machine.packer().pack( c );
            machine.packer().flush();

            // Then
            String value = newUnpacker( machine.output() ).unpackString();
            assertThat( value ).isEqualTo( String.valueOf( c ) );
        }
    }

    @Test
    void testCanPackAndUnpackStrings() throws Throwable
    {
        // Given
        Machine machine = new Machine( 17000000 );

        for ( int i = 0; i < 24; i++ )
        {
            String string = new String( new byte[(int) Math.pow( 2, i )] );

            // When
            machine.reset();
            machine.packer().pack( string );
            machine.packer().flush();

            // Then
            String value = newUnpacker( machine.output() ).unpackString();
            assertThat( value ).isEqualTo( string );
        }
    }

    @Test
    void testCanPackAndUnpackBytes() throws Throwable
    {
        // Given
        Machine machine = new Machine();
        byte[] abcdefghij = "ABCDEFGHIJ".getBytes();

        // When
        PackStream.Packer packer = machine.packer();
        packer.pack( abcdefghij );
        packer.flush();

        // Then
        byte[] value = newUnpacker( machine.output() ).unpackBytes();
        assertThat( value ).isEqualTo( abcdefghij );
    }

    @Test
    void testCanPackAndUnpackString() throws Throwable
    {
        // Given
        Machine machine = new Machine();
        String abcdefghij = "ABCDEFGHIJ";

        // When
        PackStream.Packer packer = machine.packer();
        packer.pack( abcdefghij );
        packer.flush();

        // Then
        String value = newUnpacker( machine.output() ).unpackString();
        assertThat( value ).isEqualTo( abcdefghij );
    }

    @Test
    void testCanPackAndUnpackListInOneCall() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        // When
        PackStream.Packer packer = machine.packer();
        packer.packListHeader( 3 );
        packer.pack( 12 );
        packer.pack( 13 );
        packer.pack( 14 );
        packer.flush();
        PackStream.Unpacker unpacker = newUnpacker( machine.output() );

        // Then
        assertThat( unpacker.unpackListHeader() ).isEqualTo( 3L );

        assertThat( unpacker.unpackLong() ).isEqualTo( 12L );
        assertThat( unpacker.unpackLong() ).isEqualTo( 13L );
        assertThat( unpacker.unpackLong() ).isEqualTo( 14L );
    }

    @Test
    void testCanPackAndUnpackListOneItemAtATime() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        // When
        PackStream.Packer packer = machine.packer();
        packer.packListHeader( 3 );
        packer.pack( 12 );
        packer.pack( 13 );
        packer.pack( 14 );
        packer.flush();
        PackStream.Unpacker unpacker = newUnpacker( machine.output() );

        // Then
        assertThat( unpacker.unpackListHeader() ).isEqualTo( 3L );

        assertThat( unpacker.unpackLong() ).isEqualTo( 12L );
        assertThat( unpacker.unpackLong() ).isEqualTo( 13L );
        assertThat( unpacker.unpackLong() ).isEqualTo( 14L );
    }

    @Test
    void testCanPackAndUnpackListOfString() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        // When
        PackStream.Packer packer = machine.packer();
        packer.packListHeader( 3 );
        packer.flush();
        packer.pack( "eins" );
        packer.flush();
        packer.pack( "zwei" );
        packer.flush();
        packer.pack( "drei" );
        packer.flush();
        PackStream.Unpacker unpacker = newUnpacker( machine.output() );

        // Then
        assertThat( unpacker.unpackListHeader() ).isEqualTo( 3L );

        assertThat( unpacker.unpackString() ).isEqualTo( "eins" );
        assertThat( unpacker.unpackString() ).isEqualTo( "zwei" );
        assertThat( unpacker.unpackString() ).isEqualTo( "drei" );
    }

    @Test
    void testCanPackAndUnpackListStream() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        // When
        PackStream.Packer packer = machine.packer();
        packer.packListStreamHeader();
        packer.pack( "eins" );
        packer.pack( "zwei" );
        packer.pack( "drei" );
        packer.packEndOfStream();
        packer.flush();
        PackStream.Unpacker unpacker = newUnpacker( machine.output() );

        // Then

        assertThat( unpacker.unpackListHeader() ).isEqualTo( PackStream.UNKNOWN_SIZE );

        assertThat( unpacker.unpackString() ).isEqualTo( "eins" );
        assertThat( unpacker.unpackString() ).isEqualTo( "zwei" );
        assertThat( unpacker.unpackString() ).isEqualTo( "drei" );

        unpacker.unpackEndOfStream();
    }

    @Test
    void testCanPackAndUnpackMap() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        // When
        PackStream.Packer packer = machine.packer();
        packer.packMapHeader( 2 );
        packer.pack( "one" );
        packer.pack( 1 );
        packer.pack( "two" );
        packer.pack( 2 );
        packer.flush();
        PackStream.Unpacker unpacker = newUnpacker( machine.output() );

        // Then

        assertThat( unpacker.unpackMapHeader() ).isEqualTo( 2L );

        assertThat( unpacker.unpackString() ).isEqualTo( "one" );
        assertThat( unpacker.unpackLong() ).isEqualTo( 1L );
        assertThat( unpacker.unpackString() ).isEqualTo( "two" );
        assertThat( unpacker.unpackLong() ).isEqualTo( 2L );
    }

    @Test
    void testCanPackAndUnpackMapStream() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        // When
        PackStream.Packer packer = machine.packer();
        packer.packMapStreamHeader();
        packer.pack( "one" );
        packer.pack( 1 );
        packer.pack( "two" );
        packer.pack( 2 );
        packer.packEndOfStream();
        packer.flush();
        PackStream.Unpacker unpacker = newUnpacker( machine.output() );

        // Then

        assertThat( unpacker.unpackMapHeader() ).isEqualTo( PackStream.UNKNOWN_SIZE );

        assertThat( unpacker.unpackString() ).isEqualTo( "one" );
        assertThat( unpacker.unpackLong() ).isEqualTo( 1L );
        assertThat( unpacker.unpackString() ).isEqualTo( "two" );
        assertThat( unpacker.unpackLong() ).isEqualTo( 2L );

        unpacker.unpackEndOfStream();
    }

    @Test
    void testCanPackAndUnpackStruct() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        // When
        PackStream.Packer packer = machine.packer();
        packer.packStructHeader( 3, (byte)'N' );
        packer.pack( 12 );
        packer.packListHeader( 2 );
        packer.pack( "Person" );
        packer.pack( "Employee" );
        packer.packMapHeader( 2 );
        packer.pack( "name" );
        packer.pack( "Alice" );
        packer.pack( "age" );
        packer.pack( 33 );
        packer.flush();
        PackStream.Unpacker unpacker = newUnpacker( machine.output() );

        // Then
        assertThat( unpacker.unpackStructHeader() ).isEqualTo( 3L );
        assertThat( unpacker.unpackStructSignature() ).isEqualTo( 'N' );

        assertThat( unpacker.unpackLong() ).isEqualTo( 12L );

        assertThat( unpacker.unpackListHeader() ).isEqualTo( 2L );
        assertThat( unpacker.unpackString() ).isEqualTo( "Person" );
        assertThat( unpacker.unpackString() ).isEqualTo( "Employee" );

        assertThat( unpacker.unpackMapHeader() ).isEqualTo( 2L );
        assertThat( unpacker.unpackString() ).isEqualTo( "name" );
        assertThat( unpacker.unpackString() ).isEqualTo( "Alice" );
        assertThat( unpacker.unpackString() ).isEqualTo( "age" );
        assertThat( unpacker.unpackLong() ).isEqualTo( 33L );
    }

    @Test
    void testCanPackStructIncludingSignature() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        // When
        PackStream.Packer packer = machine.packer();
        packer.packStructHeader( 3,  (byte)'N' );
        packer.pack( 12 );
        packer.packListHeader( 2 );
        packer.pack( "Person" );
        packer.pack( "Employee" );
        packer.packMapHeader( 2 );
        packer.pack( "name" );
        packer.pack( "Alice" );
        packer.pack( "age" );
        packer.pack( 33 );
        packer.flush();

        // Then
        byte[] bytes = machine.output();
        byte[] expected = new byte[]{
                PackStream.TINY_STRUCT | 3,
                'N',
                12,
                PackStream.TINY_LIST | 2,
                PackStream.TINY_STRING | 6, 'P', 'e', 'r', 's', 'o', 'n',
                PackStream.TINY_STRING | 8, 'E', 'm', 'p', 'l', 'o', 'y', 'e', 'e',
                PackStream.TINY_MAP | 2,
                PackStream.TINY_STRING | 4, 'n', 'a', 'm', 'e',
                PackStream.TINY_STRING | 5, 'A', 'l', 'i', 'c', 'e',
                PackStream.TINY_STRING | 3, 'a', 'g', 'e',
                33};
        assertThat( bytes ).isEqualTo( expected );
    }

    @Test
    void testCanDoStreamingListUnpacking() throws Throwable
    {
        // Given
        Machine machine = new Machine();
        PackStream.Packer packer = machine.packer();
        packer.packListHeader( 4 );
        packer.pack( 1 );
        packer.pack( 2 );
        packer.pack( 3 );
        packer.packListHeader( 2 );
        packer.pack( 4 );
        packer.pack( 5 );
        packer.flush();

        // When I unpack this value
        PackStream.Unpacker unpacker = newUnpacker( machine.output() );

        // Then I can do streaming unpacking
        long size = unpacker.unpackListHeader();
        long a = unpacker.unpackLong();
        long b = unpacker.unpackLong();
        long c = unpacker.unpackLong();

        long innerSize = unpacker.unpackListHeader();
        long d = unpacker.unpackLong();
        long e = unpacker.unpackLong();

        // And all the values should be sane
        assertEquals( 4, size );
        assertEquals( 2, innerSize );
        assertEquals( 1, a );
        assertEquals( 2, b );
        assertEquals( 3, c );
        assertEquals( 4, d );
        assertEquals( 5, e );
    }

    @Test
    void testCanDoStreamingStructUnpacking() throws Throwable
    {
        // Given
        Machine machine = new Machine();
        PackStream.Packer packer = machine.packer();
        packer.packStructHeader( 4,  (byte)'~' );
        packer.pack( 1 );
        packer.pack( 2 );
        packer.pack( 3 );
        packer.packListHeader( 2 );
        packer.pack( 4 );
        packer.pack( 5 );
        packer.flush();

        // When I unpack this value
        PackStream.Unpacker unpacker = newUnpacker( machine.output() );

        // Then I can do streaming unpacking
        long size = unpacker.unpackStructHeader();
        char signature = unpacker.unpackStructSignature();
        long a = unpacker.unpackLong();
        long b = unpacker.unpackLong();
        long c = unpacker.unpackLong();

        long innerSize = unpacker.unpackListHeader();
        long d = unpacker.unpackLong();
        long e = unpacker.unpackLong();

        // And all the values should be sane
        assertEquals( 4, size );
        assertEquals( '~', signature );
        assertEquals( 2, innerSize );
        assertEquals( 1, a );
        assertEquals( 2, b );
        assertEquals( 3, c );
        assertEquals( 4, d );
        assertEquals( 5, e );
    }

    @Test
    void testCanDoStreamingMapUnpacking() throws Throwable
    {
        // Given
        Machine machine = new Machine();
        PackStream.Packer packer = machine.packer();
        packer.packMapHeader( 2 );
        packer.pack( "name" );
        packer.pack( "Bob" );
        packer.pack( "cat_ages" );
        packer.packListHeader( 2 );
        packer.pack( 4.3 );
        packer.pack( true );
        packer.flush();

        // When I unpack this value
        PackStream.Unpacker unpacker = newUnpacker( machine.output() );

        // Then I can do streaming unpacking
        long size = unpacker.unpackMapHeader();
        String k1 = unpacker.unpackString();
        String v1 = unpacker.unpackString();
        String k2 = unpacker.unpackString();

        long innerSize = unpacker.unpackListHeader();
        double d = unpacker.unpackDouble();
        boolean e = unpacker.unpackBoolean();

        // And all the values should be sane
        assertEquals( 2, size );
        assertEquals( 2, innerSize );
        assertEquals( "name", k1 );
        assertEquals( "Bob", v1 );
        assertEquals( "cat_ages", k2 );
        assertEquals( 4.3, d, 0.0001 );
        assertTrue( e );
    }

    @Test
    void handlesDataCrossingBufferBoundaries() throws Throwable
    {
        // Given
        Machine machine = new Machine();
        PackStream.Packer packer = machine.packer();
        packer.pack( Long.MAX_VALUE );
        packer.pack( Long.MAX_VALUE );
        packer.flush();

        ReadableByteChannel ch = Channels.newChannel( new ByteArrayInputStream( machine.output() ) );
        PackStream.Unpacker unpacker = new PackStream.Unpacker( new BufferedChannelInput( 11 ).reset( ch ) );

        // Serialized ch will look like, and misalign with the 11-byte unpack buffer:

        // [XX][XX][XX][XX][XX][XX][XX][XX][XX][XX][XX][XX][XX][XX][XX][XX][XX][XX]
        //  mkr \___________data______________/ mkr \___________data______________/
        // \____________unpack buffer_________________/

        // When
        long first = unpacker.unpackLong();
        long second = unpacker.unpackLong();

        // Then
        assertEquals( Long.MAX_VALUE, first );
        assertEquals( Long.MAX_VALUE, second );
    }

    @Test
    void testCanPeekOnNextType() throws Throwable
    {
        // When & Then
        assertPeekType( PackType.STRING, "a string" );
        assertPeekType( PackType.INTEGER, 123L );
        assertPeekType( PackType.FLOAT, 123.123d );
        assertPeekType( PackType.BOOLEAN, true );
        assertPeekType( PackType.LIST, asList( 1, 2, 3 ) );
        assertPeekType( PackType.MAP, asMap( "l", 3 ) );
    }

    @Test
    void shouldPackUnpackBytesHeaderWithCorrectBufferSize() throws Throwable
    {
        Machine machine = new Machine();
        PackStream.Packer packer = machine.packer();

        MachineClient client = new MachineClient( 8 );
        PackStream.Unpacker unpacker = client.unpacker();

        for ( int size = 0; size <= 65536; size++ )
        {
            machine.reset();
            packer.packBytesHeader( size );
            packer.flush();

            // Then
            int bufferSize = computeOutputBufferSize( size, false );
            byte[] output = machine.output();
            assertThat( output.length ).isEqualTo( bufferSize );

            client.reset( output );
            int value = unpacker.unpackBytesHeader();
            assertThat( value ).isEqualTo( size );
        }
    }

    @Test
    void shouldPackUnpackStringHeaderWithCorrectBufferSize() throws Throwable
    {
        shouldPackUnpackHeaderWithCorrectBufferSize( PackType.STRING );
    }

    @Test
    void shouldPackUnpackMapHeaderWithCorrectBufferSize() throws Throwable
    {
        shouldPackUnpackHeaderWithCorrectBufferSize( MAP );
    }

    @Test
    void shouldPackUnpackListHeaderWithCorrectBufferSize() throws Throwable
    {
        shouldPackUnpackHeaderWithCorrectBufferSize( PackType.LIST );
    }

    @Test
    void shouldThrowErrorWhenUnPackHeaderSizeGreaterThanIntMaxValue()
    {
        assertThrows( PackStream.Overflow.class, () -> unpackHeaderSizeGreaterThanIntMaxValue( MAP ) );
        assertThrows( PackStream.Overflow.class, () -> unpackHeaderSizeGreaterThanIntMaxValue( LIST ) );
        assertThrows( PackStream.Overflow.class, () -> unpackHeaderSizeGreaterThanIntMaxValue( STRING ) );
        assertThrows( PackStream.Overflow.class, () -> unpackHeaderSizeGreaterThanIntMaxValue( BYTES ) );
    }

    private void unpackHeaderSizeGreaterThanIntMaxValue( PackType type ) throws Throwable
    {
        byte marker32;
        switch ( type )
        {
        case MAP:
            marker32 = PackStream.MAP_32;
            break;
        case LIST:
            marker32 = PackStream.LIST_32;
            break;
        case STRING:
            marker32 = PackStream.STRING_32;
            break;
        case BYTES:
            marker32 = PackStream.BYTES_32;
            break;
        default:
            throw new IllegalArgumentException( "Unsupported type: " + type + '.' );
        }

        byte[] input = new byte[]{marker32, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff};
        MachineClient client = new MachineClient( 8 );

        client.reset( input );
        PackStream.Unpacker unpacker = client.unpacker();

        switch ( type )
        {
        case MAP:
            unpacker.unpackMapHeader();
            break;
        case LIST:
            unpacker.unpackListHeader();
            break;
        case STRING:
            unpacker.unpackStringHeader();
            break;
        case BYTES:
            unpacker.unpackBytesHeader();
            break;
        default:
            throw new IllegalArgumentException( "Unsupported type: " + type + '.' );
        }
    }

    private void shouldPackUnpackHeaderWithCorrectBufferSize( PackType type ) throws Throwable
    {
        Machine machine = new Machine();
        PackStream.Packer packer = machine.packer();

        MachineClient client = new MachineClient( 8 );
        PackStream.Unpacker unpacker = client.unpacker();

        for ( int size = 0; size <= 65536; size++ )
        {
            machine.reset();
            switch ( type )
            {
            case MAP:
                packer.packMapHeader( size );
                break;
            case LIST:
                packer.packListHeader( size );
                break;
            case STRING:
                packer.packStringHeader( size );
                break;
            default:
                throw new IllegalArgumentException( "Unsupported type: " + type + '.' );
            }
            packer.flush();

            int bufferSize = computeOutputBufferSize( size, true );
            byte[] output = machine.output();
            assertThat( output.length ).isEqualTo( bufferSize );

            client.reset( output );
            int value;
            switch ( type )
            {
            case MAP:
                value = (int) unpacker.unpackMapHeader();
                break;
            case LIST:
                value = (int) unpacker.unpackListHeader();
                break;
            case STRING:
                value = unpacker.unpackStringHeader();
                break;
            default:
                throw new IllegalArgumentException( "Unsupported type: " + type + '.' );
            }

            assertThat( value ).isEqualTo( size );
        }
    }

    private int computeOutputBufferSize( int size, boolean withMarker8 )
    {
        int bufferSize;
        if ( withMarker8 && size < 16 )
        {
            bufferSize = 1;
        }
        else if ( size < 128 )
        {
            bufferSize = 2;
        }
        else if ( size < 32768 )
        {
            bufferSize = 1 + 2;
        }
        else
        {
            bufferSize = 1 + 4;
        }
        return bufferSize;
    }

    private void assertPeekType( PackType type, Object value ) throws IOException
    {
        // Given
        Machine machine = new Machine();
        PackStream.Packer packer = machine.packer();
        doTheThing( packer, value );
        packer.flush();

        PackStream.Unpacker unpacker = newUnpacker( machine.output() );

        // When & Then
        assertEquals( type, unpacker.peekNextType() );
    }

    private void doTheThing( PackStream.Packer packer, Object value ) throws IOException
    {
        if ( value instanceof String )
        {
            packer.pack( (String) value );
        }
        else if ( value instanceof Long || value instanceof Integer )
        {
            packer.pack( ((Number) value).longValue() );
        }
        else if ( value instanceof Double || value instanceof Float )
        {
            packer.pack( ((Number) value).doubleValue() );
        }
        else if ( value instanceof Boolean )
        {
            packer.pack( (Boolean) value );
        }
        else if ( value instanceof List )
        {
            List list = (List) value;
            packer.packListHeader( list.size() );
            for ( Object o : list )
            {
                doTheThing( packer, o );
            }
        }
        else if ( value instanceof Map )
        {
            Map<?,?> map = (Map<?,?>) value;
            packer.packMapHeader( map.size() );
            for ( Map.Entry<?,?> o : map.entrySet() )
            {
                doTheThing( packer, o.getKey() );
                doTheThing( packer, o.getValue() );
            }
        }
    }
}
