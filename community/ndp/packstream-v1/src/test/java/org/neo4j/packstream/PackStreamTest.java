/*
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import static java.util.Arrays.asList;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;

import static org.neo4j.packstream.PackStream.UTF_8;

public class PackStreamTest
{

    public static Map<String,Object> asMap( Object... keysAndValues )
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

    private PackStream.Unpacker newUnpacker( byte[] bytes )
    {
        ByteArrayInputStream input = new ByteArrayInputStream( bytes );
        return new PackStream.Unpacker( new BufferedChannelInput( 16 ).reset( Channels.newChannel( input ) ) );
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
        unpacker.unpackNull();

        // Then
        // it does not blow up
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

        // Then
        assertThat( unpacker.unpackBoolean(), equalTo( true ) );

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

        // Then
        assertThat( unpacker.unpackBoolean(), equalTo( false ) );

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

            // Then
            assertThat( unpacker.unpackLong(), equalTo( i ) );
        }
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

            // Then
            assertThat( unpacker.unpackLong(), equalTo( i ) );
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
            long value = newUnpacker( machine.output() ).unpackLong();
            assertThat( value, equalTo( n ) );
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

            double value = newUnpacker( machine.output() ).unpackDouble();

            // Then
            assertThat( value, equalTo( n ) );
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
            double value = newUnpacker( machine.output() ).unpackDouble();
            assertThat( value, equalTo( n ) );
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
            byte[] value = newUnpacker( machine.output() ).unpackBytes();
            assertThat( value, equalTo( array ) );
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
            String value = newUnpacker( machine.output() ).unpackText();
            assertThat( value, equalTo( string ) );
        }
    }

    @Test
    public void testCanPackAndUnpackBytes() throws Throwable
    {
        // Given
        Machine machine = new Machine();
        byte[] bytes = "ABCDEFGHIJ".getBytes();

        // When
        PackStream.Packer packer = machine.packer();
        packer.pack( bytes );
        packer.flush();

        // Then
        byte[] value = newUnpacker( machine.output() ).unpackBytes();
        assertThat( value, equalTo( bytes ) );

    }

    @Test
    public void testCanPackAndUnpackText() throws Throwable
    {
        // Given
        Machine machine = new Machine();
        String abcdefghij = "ABCDEFGHIJ";

        // When
        PackStream.Packer packer = machine.packer();
        packer.pack( abcdefghij );
        packer.flush();

        // Then
        String value = newUnpacker( machine.output() ).unpackText();
        assertThat( value, equalTo( abcdefghij ) );
    }

    @Test
    public void testCanPackAndUnpackTextFromBytes() throws Throwable
    {
        // Given
        Machine machine = new Machine();
        String abcdefghij = "ABCDEFGHIJ";

        // When
        PackStream.Packer packer = machine.packer();
        packer.packText( abcdefghij.getBytes() );
        packer.flush();

        // Then
        String value = newUnpacker( machine.output() ).unpackText();
        assertThat( value, equalTo( abcdefghij ) );
    }

    @Test
    public void testCanPackAndUnpackTextInParts() throws Throwable
    {
        // Given
        Machine machine = new Machine();
        byte[] part1 = "node/".getBytes( UTF_8 );
        long part2 = 12345;

        // When
        PackStream.Packer packer = machine.packer();
        packer.packText( part1, part2 );
        packer.flush();

        // Then
        String value = newUnpacker( machine.output() ).unpackText();
        assertThat( value, equalTo( "node/12345" ) );
    }

    @Test
    public void testCanPackAndUnpackListInOneCall() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        // When
        PackStream.Packer packer = machine.packer();
        packer.packListHeader( 3, PackListType.INTEGER );
        packer.pack( 12 );
        packer.pack( 13 );
        packer.pack( 14 );
        packer.flush();
        PackStream.Unpacker unpacker = newUnpacker( machine.output() );

        // Then
        assertThat( unpacker.unpackListHeader(), equalTo( 3L ) );
        assertThat( unpacker.unpackListType(), equalTo( PackListType.INTEGER ) );

        assertThat( unpacker.unpackLong(), equalTo( 12L ) );
        assertThat( unpacker.unpackLong(), equalTo( 13L ) );
        assertThat( unpacker.unpackLong(), equalTo( 14L ) );
    }

    @Test
    public void testCanPackAndUnpackListOneItemAtATime() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        // When
        PackStream.Packer packer = machine.packer();
        packer.packListHeader( 3, PackListType.INTEGER );
        packer.pack( 12 );
        packer.pack( 13 );
        packer.pack( 14 );
        packer.flush();
        PackStream.Unpacker unpacker = newUnpacker( machine.output() );

        // Then
        assertThat( unpacker.unpackListHeader(), equalTo( 3L ) );
        assertThat( unpacker.unpackListType(), equalTo( PackListType.INTEGER ) );

        assertThat( unpacker.unpackLong(), equalTo( 12L ) );
        assertThat( unpacker.unpackLong(), equalTo( 13L ) );
        assertThat( unpacker.unpackLong(), equalTo( 14L ) );
    }

    private <T> void testCanPackAndUnpackList( Machine machine,
                                               PackStream.Packer packer,
                                               PackStream.Unpacker unpacker,
                                               List<T> list,
                                               ObjectPacker itemPacker,
                                               ObjectUnpacker itemUnpacker ) throws Throwable
    {
        // Given
        int listSize = list.size();
        assertThat( listSize, greaterThan( 0 ) );  // otherwise we can't ascertain type information
        PackListType listType = null;

        // When
        for ( T item : list )
        {
            if ( listType == null )  // have we packed the list type yet?
            {
                listType = PackListType.fromClass( item.getClass() );
                packer.packListHeader( listSize, listType );
                packer.flush();
            }
            itemPacker.pack( item );
            packer.flush();
        }

        ByteArrayInputStream input = new ByteArrayInputStream( machine.output() );
        unpacker.reset( new BufferedChannelInput( 16 ).reset( Channels.newChannel( input ) ) );

        // Then
        assertThat( unpacker.unpackListHeader(), equalTo( (long) listSize ) );
        assertThat( unpacker.unpackListType(), equalTo( listType ) );

        for ( Object item : list )
        {
            assertThat( itemUnpacker.unpack(), equalTo( item ) );
        }

    }

    @Test
    public void testCanPackAndUnpackListOfAny() throws Throwable
    {
        final Machine machine = new Machine();
        final PackStream.Packer packer = machine.packer();
        final PackStream.Unpacker unpacker = new PackStream.Unpacker();
        testCanPackAndUnpackList( machine, packer, unpacker,
                asList( "one", 2, 3.0 ),
                new ObjectPacker()
                {
                    @Override
                    public void pack( Object obj ) throws IOException
                    {
                        if ( obj instanceof String )
                        {
                            packer.pack( (String) obj );
                        }
                        else if ( obj instanceof Integer )
                        {
                            packer.pack( (int) obj );
                        }
                        else if ( obj instanceof Double )
                        {
                            packer.pack( (double) obj );
                        }
                        else
                        {
                            assert false;
                        }
                    }

                },
                new ObjectUnpacker()
                {
                    int sequence = 0;

                    @Override
                    public Object unpack() throws IOException
                    {
                        try
                        {
                            switch ( sequence )
                            {
                                case 0:
                                    return unpacker.unpackText();
                                case 1:
                                    return unpacker.unpackInteger();
                                case 2:
                                    return unpacker.unpackDouble();
                                default:
                                    throw new RuntimeException( "This should never happen" );
                            }
                        }
                        finally
                        {
                            sequence += 1;
                        }
                    }

                } );
    }

    @Test
    public void testCanPackAndUnpackListOfBoolean() throws Throwable
    {
        final Machine machine = new Machine();
        final PackStream.Packer packer = machine.packer();
        final PackStream.Unpacker unpacker = new PackStream.Unpacker();
        testCanPackAndUnpackList( machine, packer, unpacker,
                asList( true, false, true, true, false ),
                new ObjectPacker()
                {
                    @Override
                    public void pack( Object obj ) throws IOException
                    {
                        packer.pack( (boolean) obj );
                    }

                },
                new ObjectUnpacker()
                {
                    @Override
                    public Object unpack() throws IOException
                    {
                        return unpacker.unpackBoolean();
                    }

                } );
    }

    @Test
    public void testCanPackAndUnpackListOfInteger() throws Throwable
    {
        final Machine machine = new Machine();
        final PackStream.Packer packer = machine.packer();
        final PackStream.Unpacker unpacker = new PackStream.Unpacker();
        testCanPackAndUnpackList( machine, packer, unpacker,
                asList( 1, 2, 3 ),
                new ObjectPacker()
                {
                    @Override
                    public void pack( Object obj ) throws IOException
                    {
                        packer.pack( (int) obj );
                    }

                },
                new ObjectUnpacker()
                {
                    @Override
                    public Object unpack() throws IOException
                    {
                        return unpacker.unpackInteger();
                    }

                } );
    }

    @Test
    public void testCanPackAndUnpackListOfFloat() throws Throwable
    {
        final Machine machine = new Machine();
        final PackStream.Packer packer = machine.packer();
        final PackStream.Unpacker unpacker = new PackStream.Unpacker();
        testCanPackAndUnpackList( machine, packer, unpacker,
                asList( 1.1, 2.2, 3.141592653589 ),
                new ObjectPacker()
                {
                    @Override
                    public void pack( Object obj ) throws IOException
                    {
                        packer.pack( (double) obj );
                    }

                },
                new ObjectUnpacker()
                {
                    @Override
                    public Object unpack() throws IOException
                    {
                        return unpacker.unpackDouble();
                    }

                } );
    }

    @Test
    public void testCanPackAndUnpackListOfText() throws Throwable
    {
        final Machine machine = new Machine();
        final PackStream.Packer packer = machine.packer();
        final PackStream.Unpacker unpacker = new PackStream.Unpacker();
        testCanPackAndUnpackList( machine, packer, unpacker,
                asList( "eins", "zwei", "drei", "vier", "fünf" ),
                new ObjectPacker()
                {
                    @Override
                    public void pack( Object obj ) throws IOException
                    {
                        packer.pack( (String) obj );
                    }

                },
                new ObjectUnpacker()
                {
                    @Override
                    public Object unpack() throws IOException
                    {
                        return unpacker.unpackText();
                    }

                } );
    }

    @Test
    public void testCanPackAndUnpackListOfListOfInteger() throws Throwable
    {
        final Machine machine = new Machine();
        final PackStream.Packer packer = machine.packer();
        final PackStream.Unpacker unpacker = new PackStream.Unpacker();
        testCanPackAndUnpackList( machine, packer, unpacker,
                asList( asList( 1, 2, 3 ), asList( 4, 5, 6 ), asList( 7, 8, 9 ) ),
                new ObjectPacker()
                {
                    @Override
                    public void pack( Object obj ) throws IOException
                    {
                        List list = (List) obj;
                        packer.packListHeader( list.size(), PackListType.INTEGER );
                        for ( Object item : list )
                        {
                            packer.pack( (int) item );
                        }
                    }

                },
                new ObjectUnpacker()
                {
                    @Override
                    public Object unpack() throws IOException
                    {
                        int listSize = (int) unpacker.unpackListHeader();
                        PackListType listType = unpacker.unpackListType();
                        assertThat( listType, equalTo( PackListType.INTEGER ) );
                        List<Integer> list = new ArrayList<>( listSize );
                        for ( int i = 0; i < listSize; i++ )
                        {
                            list.add( unpacker.unpackInteger() );
                        }
                        return list;
                    }

                } );
    }

    @Test
    public void testCanPackAndUnpackListOfListOfFloat() throws Throwable
    {
        final Machine machine = new Machine();
        final PackStream.Packer packer = machine.packer();
        final PackStream.Unpacker unpacker = new PackStream.Unpacker();
        testCanPackAndUnpackList( machine, packer, unpacker,
                asList( asList( 1.1, 2.2, 3.3 ), asList( 4.4, 5.5, 6.6 ), asList( 7.7, 8.8, 9.9 ) ),
                new ObjectPacker()
                {
                    @Override
                    public void pack( Object obj ) throws IOException
                    {
                        List list = (List) obj;
                        packer.packListHeader( list.size(), PackListType.FLOAT );
                        for ( Object item : list )
                        {
                            packer.pack( (double) item );
                        }
                    }

                },
                new ObjectUnpacker()
                {
                    @Override
                    public Object unpack() throws IOException
                    {
                        int listSize = (int) unpacker.unpackListHeader();
                        PackListType listType = unpacker.unpackListType();
                        assertThat( listType, equalTo( PackListType.FLOAT ) );
                        List<Double> list = new ArrayList<>( listSize );
                        for ( int i = 0; i < listSize; i++ )
                        {
                            list.add( unpacker.unpackDouble() );
                        }
                        return list;
                    }

                } );
    }

    @Test
    public void testCanPackAndUnpackListOfListOfText() throws Throwable
    {
        final Machine machine = new Machine();
        final PackStream.Packer packer = machine.packer();
        final PackStream.Unpacker unpacker = new PackStream.Unpacker();
        testCanPackAndUnpackList( machine, packer, unpacker,
                asList( asList( "eins", "zwei", "drei" ),
                        asList( "vier", "fünf", "sechs" ),
                        asList( "sieben", "acht", "neun" ) ),
                new ObjectPacker()
                {
                    @Override
                    public void pack( Object obj ) throws IOException
                    {
                        List list = (List) obj;
                        packer.packListHeader( list.size(), PackListType.TEXT );
                        for ( Object item : list )
                        {
                            packer.pack( (String) item );
                        }
                    }

                },
                new ObjectUnpacker()
                {
                    @Override
                    public Object unpack() throws IOException
                    {
                        int listSize = (int) unpacker.unpackListHeader();
                        PackListType listType = unpacker.unpackListType();
                        assertThat( listType, equalTo( PackListType.TEXT ) );
                        List<String> list = new ArrayList<>( listSize );
                        for ( int i = 0; i < listSize; i++ )
                        {
                            list.add( unpacker.unpackText() );
                        }
                        return list;
                    }

                } );
    }

    @Test
    public void testCanPackAndUnpackListOfMap() throws Throwable
    {
        final Machine machine = new Machine();
        final PackStream.Packer packer = machine.packer();
        final PackStream.Unpacker unpacker = new PackStream.Unpacker();
        testCanPackAndUnpackList( machine, packer, unpacker,
                asList( asMap( "one", 1, "two", 2 ), asMap( "three", 3, "four", 4 ) ),
                new ObjectPacker()
                {
                    @Override
                    public void pack( Object obj ) throws IOException
                    {
                        Map map = (Map) obj;
                        packer.packMapHeader( map.size() );
                        for ( Object key : map.keySet() )
                        {
                            packer.pack( (String) key );
                            packer.pack( (int) map.get(key) );
                        }
                    }

                },
                new ObjectUnpacker()
                {
                    @Override
                    public Object unpack() throws IOException
                    {
                        long size = unpacker.unpackMapHeader();
                        Map<String, Object> map = new LinkedHashMap<>( (int) size );
                        for ( long i = 0; i < size; i++ )
                        {
                            String key = unpacker.unpackText();
                            Integer value = unpacker.unpackInteger();
                            map.put( key, value );
                        }
                        return map;
                    }

                } );
    }

    @Test
    public void testCanPackAndUnpackMap() throws Throwable
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

        assertThat( unpacker.unpackMapHeader(), equalTo( 2L ) );

        assertThat( unpacker.unpackText(), equalTo( "one" ) );
        assertThat( unpacker.unpackLong(), equalTo( 1L ) );
        assertThat( unpacker.unpackText(), equalTo( "two" ) );
        assertThat( unpacker.unpackLong(), equalTo( 2L ) );
    }

    @Test
    public void testCanPackAndUnpackStruct() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        // When
        PackStream.Packer packer = machine.packer();
        packer.packStructHeader( 3, (byte)'N' );
        packer.pack( 12 );
        packer.packListHeader( 2, PackListType.TEXT );
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
        assertThat( unpacker.unpackStructHeader(), equalTo( 3L ) );
        assertThat( unpacker.unpackStructSignature(), equalTo( (byte) 'N' ) );

        assertThat( unpacker.unpackLong(), equalTo( 12L ) );

        assertThat( unpacker.unpackListHeader(), equalTo( 2L ) );
        assertThat( unpacker.unpackListType(), equalTo( PackListType.TEXT ) );
        assertThat( unpacker.unpackText(), equalTo( "Person" ) );
        assertThat( unpacker.unpackText(), equalTo( "Employee" ) );

        assertThat( unpacker.unpackMapHeader(), equalTo( 2L ) );
        assertThat( unpacker.unpackText(), equalTo( "name" ) );
        assertThat( unpacker.unpackText(), equalTo( "Alice" ) );
        assertThat( unpacker.unpackText(), equalTo( "age" ) );
        assertThat( unpacker.unpackLong(), equalTo( 33L ) );
    }

    @Test
    public void testCanPackStructIncludingSignature() throws Throwable
    {
        // Given
        Machine machine = new Machine();

        // When
        PackStream.Packer packer = machine.packer();
        packer.packStructHeader( 3,  (byte)'N' );
        packer.pack( 12 );
        packer.packListHeader( 2, PackListType.TEXT );
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
                PackStream.TINY_LIST | 2, PackListType.TEXT.marker(),
                PackStream.TINY_TEXT | 6, 'P', 'e', 'r', 's', 'o', 'n',
                PackStream.TINY_TEXT | 8, 'E', 'm', 'p', 'l', 'o', 'y', 'e', 'e',
                PackStream.TINY_MAP | 2,
                PackStream.TINY_TEXT | 4, 'n', 'a', 'm', 'e',
                PackStream.TINY_TEXT | 5, 'A', 'l', 'i', 'c', 'e',
                PackStream.TINY_TEXT | 3, 'a', 'g', 'e',
                33};
        assertThat( bytes, equalTo( expected ) );
    }

    @Test
    public void testCanDoStreamingListUnpacking() throws Throwable
    {
        // Given
        Machine machine = new Machine();
        PackStream.Packer packer = machine.packer();
        packer.packListHeader( 4, PackListType.INTEGER );
        packer.pack( 1 );
        packer.pack( 2 );
        packer.pack( 3 );
        packer.packListHeader( 2, PackListType.INTEGER );
        packer.pack( 4 );
        packer.pack( 5 );
        packer.flush();

        // When I unpack this value
        PackStream.Unpacker unpacker = newUnpacker( machine.output() );

        // Then I can do streaming unpacking
        long size = unpacker.unpackListHeader();
        PackListType type = unpacker.unpackListType();
        long a = unpacker.unpackLong();
        long b = unpacker.unpackLong();
        long c = unpacker.unpackLong();

        long innerSize = unpacker.unpackListHeader();
        PackListType innerType = unpacker.unpackListType();
        long d = unpacker.unpackLong();
        long e = unpacker.unpackLong();

        // And all the values should be sane
        assertEquals( 4, size );
        assertEquals( PackListType.INTEGER, type );
        assertEquals( 2, innerSize );
        assertEquals( PackListType.INTEGER, innerType );
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
        packer.packStructHeader( 4,  (byte) '~' );
        packer.pack( 1 );
        packer.pack( 2 );
        packer.pack( 3 );
        packer.packListHeader( 2, PackListType.INTEGER );
        packer.pack( 4 );
        packer.pack( 5 );
        packer.flush();

        // When I unpack this value
        PackStream.Unpacker unpacker = newUnpacker( machine.output() );

        // Then I can do streaming unpacking
        long size = unpacker.unpackStructHeader();
        byte signature = unpacker.unpackStructSignature();
        long a = unpacker.unpackLong();
        long b = unpacker.unpackLong();
        long c = unpacker.unpackLong();

        long innerSize = unpacker.unpackListHeader();
        PackListType type = unpacker.unpackListType();
        long d = unpacker.unpackLong();
        long e = unpacker.unpackLong();

        // And all the values should be sane
        assertEquals( 4, size );
        assertEquals( (byte) '~', signature );
        assertEquals( 2, innerSize );
        assertEquals( PackListType.INTEGER, type );
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
        packer.packListHeader( 2, PackListType.ANY );
        packer.pack( 4.3 );
        packer.pack( true );
        packer.flush();

        // When I unpack this value
        PackStream.Unpacker unpacker = newUnpacker( machine.output() );

        // Then I can do streaming unpacking
        long size = unpacker.unpackMapHeader();
        String k1 = unpacker.unpackText();
        String v1 = unpacker.unpackText();
        String k2 = unpacker.unpackText();

        long innerSize = unpacker.unpackListHeader();
        PackListType type = unpacker.unpackListType();
        double d = unpacker.unpackDouble();
        boolean e = unpacker.unpackBoolean();

        // And all the values should be sane
        assertEquals( 2, size );
        assertEquals( 2, innerSize );
        assertEquals( PackListType.ANY, type );
        assertEquals( "name", k1 );
        assertEquals( "Bob", v1 );
        assertEquals( "cat_ages", k2 );
        assertEquals( 4.3, d, 0.0001 );
        assertEquals( true, e );
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
    public void testCanPeekOnNextType() throws Throwable
    {
        // When & Then
        assertPeekType( PackType.TEXT, "a string" );
        assertPeekType( PackType.INTEGER, 123L );
        assertPeekType( PackType.FLOAT, 123.123d );
        assertPeekType( PackType.BOOLEAN, true );
        assertPeekType( PackType.LIST, asList( 1, 2, 3 ) );
        assertPeekType( PackType.MAP, asMap( "l", 3 ) );
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
        else if ( value instanceof Long || value instanceof Integer)
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
            packer.packListHeader( list.size(), PackListType.ANY );
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
