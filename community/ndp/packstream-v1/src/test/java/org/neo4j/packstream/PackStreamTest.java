/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.packstream;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static junit.framework.TestCase.assertFalse;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PackStreamTest
{

    public static Map<String, Object> asMap( Object... keysAndValues )
    {
        Map<String, Object> map = new LinkedHashMap<>( keysAndValues.length / 2 );
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

        public Machine()
        {
            this.output = new ByteArrayOutputStream();
            this.writable = Channels.newChannel( this.output );
            this.packer = new PackStream.Packer( new BufferedChannelOutput( this.writable ) );
        }

        public Machine( int bufferSize )
        {
            this.output = new ByteArrayOutputStream();
            this.writable = Channels.newChannel( this.output );
            this.packer = new PackStream.Packer( new BufferedChannelOutput( this.writable, bufferSize) );
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

    private PackStream.Unpacker newUnpacker( byte[] bytes )
    {
        ByteArrayInputStream input = new ByteArrayInputStream( bytes );
        return new PackStream.Unpacker( Channels.newChannel( input ) );
    }

    @Test
    public void testCanPackAndUnpackNull() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        // When
        machine.packer().packNull();
        machine.packer().flush();

        // Then
        byte[] bytes = machine.output();
        assertThat( bytes, equalTo( new byte[]{(byte) 0xC0} ) );

        // When
        PackStream.Unpacker unpacker = newUnpacker( bytes );
        PackValue value = unpacker.unpack();

        // Then
        assertThat( value.isNull(), equalTo( true ) );

    }

    @Test
    public void testCanPackAndUnpackTrue() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        // When
        machine.packer().pack( true );
        machine.packer().flush();

        // Then
        byte[] bytes = machine.output();
        assertThat( bytes, equalTo( new byte[]{(byte) 0xC3} ) );

        // When
        PackStream.Unpacker unpacker = newUnpacker( bytes );
        PackValue value = unpacker.unpack();

        // Then
        assertThat( value.isBoolean(), equalTo( true ) );
        assertThat( value.booleanValue(), equalTo( true ) );

    }

    @Test
    public void testCanPackAndUnpackFalse() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        // When
        machine.packer().pack( false );
        machine.packer().flush();

        // Then
        byte[] bytes = machine.output();
        assertThat( bytes, equalTo( new byte[]{(byte) 0xC2} ) );

        // When
        PackStream.Unpacker unpacker = newUnpacker( bytes );
        PackValue value = unpacker.unpack();

        // Then
        assertThat( value.isBoolean(), equalTo( true ) );
        assertThat( value.booleanValue(), equalTo( false ) );

    }

    @Test
    public void testCanPackAndUnpackTinyIntegers() throws Throwable
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
            assertThat( bytes.length, equalTo( 1 ) );

            // When
            PackStream.Unpacker unpacker = newUnpacker( bytes );
            PackValue value = unpacker.unpack();

            // Then
            assertThat( value.isInteger(), equalTo( true ) );
            assertThat( value.longValue(), equalTo( i ) );

        }

    }

    @Test
    public void testZeroIntegerAlwaysUnpacksToSameInstance() throws Throwable
    {
        // Given
        byte[] bytes = new byte[]{0};

        // When
        PackStream.Unpacker unpacker = newUnpacker( bytes );
        PackValue firstValue = unpacker.unpack();
        unpacker = newUnpacker( bytes );
        PackValue secondValue = unpacker.unpack();

        // Then
        assertThat( firstValue == secondValue, equalTo( true ) );

    }

    @Test
    public void testCanPackAndUnpackShortIntegers() throws Throwable
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
            assertThat( bytes.length, lessThanOrEqualTo( 3 ) );

            // When
            PackStream.Unpacker unpacker = newUnpacker( bytes );
            PackValue value = unpacker.unpack();

            // Then
            assertThat( value.isInteger(), equalTo( true ) );
            assertThat( value.longValue(), equalTo( i ) );

        }

    }

    @Test
    public void testCanPackAndUnpackPowersOfTwoAsIntegers() throws Throwable
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
            PackValue value = newUnpacker( machine.output() ).unpack();
            assertThat( value.isInteger(), equalTo( true ) );
            assertThat( value.longValue(), equalTo( n ) );

        }

    }

    @Test
    public void testCanPackAndUnpackPowersOfTwoPlusABitAsDoubles() throws Throwable
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

            // Then
            PackValue value = newUnpacker( machine.output() ).unpack();
            assertThat( value.isFloat(), equalTo( true ) );
            assertThat( value.doubleValue(), equalTo( n ) );

        }

    }

    @Test
    public void testCanPackAndUnpackPowersOfTwoMinusABitAsDoubles() throws Throwable
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
            PackValue value = newUnpacker( machine.output() ).unpack();
            assertThat( value.isFloat(), equalTo( true ) );
            assertThat( value.doubleValue(), equalTo( n ) );

        }

    }

    @Test
    public void testCanPackAndUnpackByteArrays() throws Throwable
    {
        // Given
        Machine machine = new Machine( 17000000 );

        for ( int i = 0; i < 24; i++ )
        {
            byte[] array = new byte[(int) Math.pow( 2, i )];

            // When
            machine.reset();
            machine.packer().pack( array );
            machine.packer().flush();

            // Then
            PackValue value = newUnpacker( machine.output() ).unpack();
            assertThat( value.isBytes(), equalTo( true ) );
            assertThat( value.byteArrayValue(), equalTo( array ) );
        }
    }

    @Test
    public void testCanPackAndUnpackStrings() throws Throwable
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
            PackValue value = newUnpacker( machine.output() ).unpack();
            assertThat( value.isText(), equalTo( true ) );
            assertThat( value.stringValue(), equalTo( string ) );
        }

    }

    @Test
    public void testCanPackAndUnpackBytes() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        // When
        PackStream.Packer packer = machine.packer();
        packer.pack( "ABCDEFGHIJ".getBytes() );
        packer.flush();

        // Then
        PackValue value = newUnpacker( machine.output() ).unpack();
        assertThat( value.isBytes(), equalTo( true ) );
        assertThat( value.byteArrayValue(), equalTo( "ABCDEFGHIJ".getBytes() ) );

    }

    @Test
    public void testCanPackAndUnpackText() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        // When
        PackStream.Packer packer = machine.packer();
        packer.pack( "ABCDEFGHIJ" );
        packer.flush();

        // Then
        PackValue value = newUnpacker( machine.output() ).unpack();
        assertThat( value.isText(), equalTo( true ) );
        assertThat( value.stringValue(), equalTo( "ABCDEFGHIJ" ) );

    }

    @Test
    public void testCanPackAndUnpackTextFromBytes() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        // When
        PackStream.Packer packer = machine.packer();
        packer.packText( "ABCDEFGHIJ".getBytes() );
        packer.flush();

        // Then
        PackValue value = newUnpacker( machine.output() ).unpack();
        assertThat( value.isText(), equalTo( true ) );
        assertThat( value.stringValue(), equalTo( "ABCDEFGHIJ" ) );

    }

    @Test
    public void testCanPackAndUnpackListInOneCall() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        // When
        PackStream.Packer packer = machine.packer();
        packer.pack( asList( 12, 13, 14 ) );
        packer.flush();

        // Then
        PackValue value = newUnpacker( machine.output() ).unpack();
        assertThat( value.isList(), equalTo( true ) );
        List<PackValue> list = value.listValue();
        assertThat( list.size(), equalTo( 3 ) );
        assertThat( list.get( 0 ).isInteger(), equalTo( true ) );
        assertThat( list.get( 1 ).isInteger(), equalTo( true ) );
        assertThat( list.get( 2 ).isInteger(), equalTo( true ) );
        assertThat( list.get( 0 ).longValue(), equalTo( 12L ) );
        assertThat( list.get( 1 ).longValue(), equalTo( 13L ) );
        assertThat( list.get( 2 ).longValue(), equalTo( 14L ) );

    }

    @Test
    public void testCanPackAndUnpackListOneItemAtATime() throws Throwable
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

        // Then
        PackValue value = newUnpacker( machine.output() ).unpack();
        assertThat( value.isList(), equalTo( true ) );
        List<PackValue> list = value.listValue();
        assertThat( list.size(), equalTo( 3 ) );
        assertThat( list.get( 0 ).isInteger(), equalTo( true ) );
        assertThat( list.get( 1 ).isInteger(), equalTo( true ) );
        assertThat( list.get( 2 ).isInteger(), equalTo( true ) );
        assertThat( list.get( 0 ).intValue(), equalTo( 12 ) );
        assertThat( list.get( 1 ).intValue(), equalTo( 13 ) );
        assertThat( list.get( 2 ).intValue(), equalTo( 14 ) );

    }

    @Test
    public void testCanPackAndUnpackListOfText() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        // When
        PackStream.Packer packer = machine.packer();
        packer.pack( asList( "eins", "zwei", "drei" ) );
        packer.flush();

        // Then
        PackValue value = newUnpacker( machine.output() ).unpack();
        assertThat( value.isList(), equalTo( true ) );
        List<PackValue> list = value.listValue();
        assertThat( list.size(), equalTo( 3 ) );
        assertThat( list.get( 0 ).isText(), equalTo( true ) );
        assertThat( list.get( 1 ).isText(), equalTo( true ) );
        assertThat( list.get( 2 ).isText(), equalTo( true ) );
        assertThat( list.get( 0 ).stringValue(), equalTo( "eins" ) );
        assertThat( list.get( 1 ).stringValue(), equalTo( "zwei" ) );
        assertThat( list.get( 2 ).stringValue(), equalTo( "drei" ) );

    }

    @Test
    public void testCanPackAndUnpackListOfTextOneByOne() throws Throwable
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

        // Then
        PackValue value = newUnpacker( machine.output() ).unpack();
        assertThat( value.isList(), equalTo( true ) );
        List<PackValue> list = value.listValue();
        assertThat( list.size(), equalTo( 3 ) );
        assertThat( list.get( 0 ).isText(), equalTo( true ) );
        assertThat( list.get( 1 ).isText(), equalTo( true ) );
        assertThat( list.get( 2 ).isText(), equalTo( true ) );
        assertThat( list.get( 0 ).stringValue(), equalTo( "eins" ) );
        assertThat( list.get( 1 ).stringValue(), equalTo( "zwei" ) );
        assertThat( list.get( 2 ).stringValue(), equalTo( "drei" ) );

    }

    @Test
    public void testCanPackAndUnpackMap() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        // When
        PackStream.Packer packer = machine.packer();
        packer.pack( asMap( "one", 1, "two", 2 ) );
        packer.flush();

        // Then
        PackValue value = newUnpacker( machine.output() ).unpack();
        assertThat( value.isMap(), equalTo( true ) );
        Map<String,PackValue> map = value.mapValue();
        assertThat( map.size(), equalTo( 2 ) );
        assertThat( map.containsKey( "one" ), equalTo( true ) );
        assertThat( map.containsKey( "two" ), equalTo( true ) );
        assertThat( map.get( "one" ).isInteger(), equalTo( true ) );
        assertThat( map.get( "two" ).isInteger(), equalTo( true ) );
        assertThat( map.get( "one" ).longValue(), equalTo( 1L ) );
        assertThat( map.get( "two" ).longValue(), equalTo( 2L ) );

    }

    @Test
    public void testCanPackAndUnpackStruct() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        // When
        PackStream.Packer packer = machine.packer();
        packer.packStructHeader( 3, 'N' );
        packer.pack( 12 );
        packer.pack( asList( "Person", "Employee" ) );
        packer.pack( asMap( "name", "Alice", "age", 33 ) );
        packer.flush();

        // Then
        PackValue value = newUnpacker( machine.output() ).unpack();
        assertThat( value.isStruct(), equalTo( true ) );
        List<PackValue> struct = value.listValue();
        assertThat( struct.size(), equalTo( 3 ) );
        assertThat( struct.get( 0 ).isInteger(), equalTo( true ) );
        assertThat( struct.get( 1 ).isList(), equalTo( true ) );
        assertThat( struct.get( 2 ).isMap(), equalTo( true ) );
        assertThat( struct.get( 0 ).longValue(), equalTo( 12L ) );
        assertThat( struct.get( 1 ).listValue().size(), equalTo( 2 ) );
        assertThat( struct.get( 1 ).listValue().get( 0 ).stringValue(), equalTo( "Person" ) );
        assertThat( struct.get( 1 ).listValue().get( 1 ).stringValue(), equalTo( "Employee" ) );
        assertThat( struct.get( 2 ).mapValue().size(), equalTo( 2 ) );
        assertThat( struct.get( 2 ).mapValue().get( "name" ).stringValue(), equalTo( "Alice" ) );
        assertThat( struct.get( 2 ).mapValue().get( "age" ).intValue(), equalTo( 33 ) );

    }

    @Test
    public void testCanPackAndUnpackStructIncludingSignature() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        // When
        PackStream.Packer packer = machine.packer();
        packer.packStructHeader( 3, 'N' );
        packer.pack( 12 );
        packer.pack( asList( "Person", "Employee" ) );
        packer.pack( asMap( "name", "Alice", "age", 33 ) );
        packer.flush();

        // Then
        byte[] bytes = machine.output();
        byte[] expected = new byte[]{
                PackStream.TINY_STRUCT | 3,
                'N',
                12,
                PackStream.TINY_LIST | 2,
                PackStream.TINY_TEXT | 6, 'P', 'e', 'r', 's', 'o', 'n',
                PackStream.TINY_TEXT | 8, 'E', 'm', 'p', 'l', 'o', 'y', 'e', 'e',
                PackStream.TINY_MAP | 2,
                PackStream.TINY_TEXT | 4, 'n', 'a', 'm', 'e',
                PackStream.TINY_TEXT | 5, 'A', 'l', 'i', 'c', 'e',
                PackStream.TINY_TEXT | 3, 'a', 'g', 'e',
                33};
        assertThat( bytes, equalTo( expected ) );

        // When
        PackValue value = newUnpacker( bytes ).unpack();

        // Then
        assertThat( value.isStruct(), equalTo( true ) );
        assertThat( value.size(), equalTo( 3 ) );
        assertThat( value.signature(), equalTo( 'N' ) );

        PackValue identity = value.get( 0 );
        assertThat( identity.isInteger(), equalTo( true ) );
        assertThat( identity.intValue(), equalTo( 12 ) );

        PackValue labels = value.get( 1 );
        assertThat( labels.isList(), equalTo( true ) );
        List<PackValue> labelList = labels.listValue();
        assertThat( labelList.size(), equalTo( 2 ) );
        assertThat( labelList.get( 0 ).stringValue(), equalTo( "Person" ) );
        assertThat( labelList.get( 1 ).stringValue(), equalTo( "Employee" ) );

        PackValue properties = value.get( 2 );
        assertThat( properties.isMap(), equalTo( true ) );
        Map<String,PackValue> propertyMap = properties.mapValue();
        assertThat( propertyMap.size(), equalTo( 2 ) );
        assertThat( propertyMap.get( "name" ).stringValue(), equalTo( "Alice" ) );
        assertThat( propertyMap.get( "age" ).intValue(), equalTo( 33 ) );
    }

    @Test
    public void testCanDoStreamingListUnpacking() throws Throwable
    {
        // Given
        Machine machine = new Machine();
        PackStream.Packer packer = machine.packer();
        packer.pack( asList(1,2,3,asList(4,5)) );
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
    public void testCanDoStreamingStructUnpacking() throws Throwable
    {
        // Given
        Machine machine = new Machine();
        PackStream.Packer packer = machine.packer();
        packer.packStructHeader( 4, '~' );
        packer.pack( 1 );
        packer.pack( 2 );
        packer.pack( 3 );
        packer.pack( asList( 4,5 ) );
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
    public void testCanDoStreamingMapUnpacking() throws Throwable
    {
        // Given
        Machine machine = new Machine();
        PackStream.Packer packer = machine.packer();
        packer.packMapHeader( 2 );
        packer.pack( "name" );
        packer.pack( "Bob" );
        packer.pack( "cat_ages" );
        packer.pack( asList( 4.3, true ) );
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
        assertEquals( true, e );
    }

    @Test
    public void testHasNext() throws Throwable
    {
        // Given
        Machine machine = new Machine();
        PackStream.Packer packer = machine.packer();
        packer.pack( "name" );
        packer.pack( asList( 1, 2 ) );
        packer.flush();

        // When I start unpacking
        PackStream.Unpacker unpacker = newUnpacker( machine.output() );

        // Then
        assertTrue( unpacker.hasNext() );

        // When I unpack the first string
        unpacker.unpack();

        // Then
        assertTrue( unpacker.hasNext() );

        // When I unpack the list
        unpacker.unpack();

        // Then
        assertFalse( unpacker.hasNext() );
    }

    @Test
    public void handlesDataCrossingBufferBoundaries() throws Throwable
    {
        // Given
        Machine machine = new Machine();
        PackStream.Packer packer = machine.packer();
        packer.pack( Long.MAX_VALUE );
        packer.pack( Long.MAX_VALUE );
        packer.flush();

        ReadableByteChannel ch = Channels.newChannel( new ByteArrayInputStream( machine.output() ) );
        PackStream.Unpacker unpacker = new PackStream.Unpacker( 11 );
        unpacker.reset( ch );

        // Serialized ch will look like, and misalign with the 11-byte unpack buffer:

        // [XX][XX][XX][XX][XX][XX][XX][XX][XX][XX][XX][XX][XX][XX][XX][XX][XX][XX]
        //  mkr \___________data______________/ mkr \___________data______________/
        // \____________unpack buffer_________________/

        // When
        long first = unpacker.unpackLong();
        long second = unpacker.unpackLong();

        // Then
        assertEquals(Long.MAX_VALUE, first);
        assertEquals(Long.MAX_VALUE, second);
    }

    @Test
    public void testCanPeekOnNextType() throws Throwable
    {
        // When & Then
        assertPeekType( PackType.TEXT, "a string" );
        assertPeekType( PackType.INTEGER, 123 );
        assertPeekType( PackType.FLOAT, 123.123 );
        assertPeekType( PackType.BOOLEAN, true );
        assertPeekType( PackType.LIST, asList( 1,2,3 ) );
        assertPeekType( PackType.MAP, asMap( "l",3 ) );
    }

    void assertPeekType( PackType type, Object value ) throws IOException
    {
        // Given
        Machine machine = new Machine();
        PackStream.Packer packer = machine.packer();
        packer.pack( value );
        packer.flush();

        PackStream.Unpacker unpacker = newUnpacker( machine.output() );

        // When & Then
        assertEquals( type, unpacker.peekNextType() );
    }
}