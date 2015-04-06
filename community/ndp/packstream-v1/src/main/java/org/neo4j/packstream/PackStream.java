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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.lang.Integer.toHexString;
import static java.lang.String.format;
import static java.util.Arrays.asList;

import static org.neo4j.packstream.PackValue.EMPTY_BYTE_ARRAY;
import static org.neo4j.packstream.PackValue.EMPTY_LIST_OF_VALUES;
import static org.neo4j.packstream.PackValue.EMPTY_MAP_OF_VALUES;
import static org.neo4j.packstream.PackValue.NullValue.NULL_VALUE;

/**
 * PackStream is a messaging serialisation format heavily inspired by MessagePack.
 * The key differences are in the type system itself which (among other things) replaces extensions with structures.
 * The Packer and Unpacker implementations are also faster than their MessagePack counterparts.
 * 
 * Note that several marker byte values are RESERVED for future use.
 * Extra markers should <em>not</em> be added casually and such additions must be follow a strict process involving both client and server software.
 * 
 * The table below shows all allocated marker byte values.
 *
 * <table> 
 * <tr><th>Marker</th><th>Binary</th><th>Type</th><th>Description</th></tr>
 * <tr><td><code>00..7F</code></td><td><code>0xxxxxxx</code></td><td>+TINY_INT</td><td>Integer 0 to 127</td></tr>
 * <tr><td><code>80..8F</code></td><td><code>1000xxxx</code></td><td>TINY_TEXT</td><td></td></tr>
 * <tr><td><code>90..9F</code></td><td><code>1001xxxx</code></td><td>TINY_LIST</td><td></td></tr>
 * <tr><td><code>A0..AF</code></td><td><code>1010xxxx</code></td><td>TINY_MAP</td><td></td></tr>
 * <tr><td><code>B0..BF</code></td><td><code>1011xxxx</code></td><td>TINY_STRUCT</td><td></td></tr>
 * <tr><td><code>C0</code></td><td><code>11000000</code></td><td>NULL</td><td></td></tr>
 * <tr><td><code>C1</code></td><td><code>11000001</code></td><td>FLOAT_64</td><td>64-bit floating point number (double)</td></tr>
 * <tr><td><code>C2</code></td><td><code>11000010</code></td><td>FALSE</td><td>Boolean false</td></tr>
 * <tr><td><code>C3</code></td><td><code>11000011</code></td><td>TRUE</td><td>Boolean true</td></tr>
 * <tr><td><code>C4..C7</code></td><td><code>110001xx</code></td><td><em>RESERVED</em></td><td></td></tr>
 * <tr><td><code>C8</code></td><td><code>11001000</code></td><td>INT_8</td><td>8-bit signed integer</td></tr>
 * <tr><td><code>C9</code></td><td><code>11001001</code></td><td>INT_8</td><td>16-bit signed integer</td></tr>
 * <tr><td><code>CA</code></td><td><code>11001010</code></td><td>INT_8</td><td>32-bit signed integer</td></tr>
 * <tr><td><code>CB</code></td><td><code>11001011</code></td><td>INT_8</td><td>64-bit signed integer</td></tr>
 * <tr><td><code>CC</code></td><td><code>11001100</code></td><td>BYTES_8</td><td>Byte string (fewer than 2<sup>8</sup> bytes)</td></tr>
 * <tr><td><code>CD</code></td><td><code>11001101</code></td><td>BYTES_16</td><td>Byte string (fewer than 2<sup>16</sup> bytes)</td></tr>
 * <tr><td><code>CE</code></td><td><code>11001110</code></td><td>BYTES_32</td><td>Byte string (fewer than 2<sup>32</sup> bytes)</td></tr>
 * <tr><td><code>CF</code></td><td><code>11001111</code></td><td><em>RESERVED</em></td><td></td></tr>
 * <tr><td><code>D0</code></td><td><code>11010000</code></td><td>TEXT_8</td><td>UTF-8 encoded text string (fewer than 2<sup>8</sup> bytes)</td></tr>
 * <tr><td><code>D1</code></td><td><code>11010001</code></td><td>TEXT_16</td><td>UTF-8 encoded text string (fewer than 2<sup>16</sup> bytes)</td></tr>
 * <tr><td><code>D2</code></td><td><code>11010010</code></td><td>TEXT_32</td><td>UTF-8 encoded text string (fewer than 2<sup>32</sup> bytes)</td></tr>
 * <tr><td><code>D3</code></td><td><code>11010011</code></td><td><em>RESERVED</em></td><td></td></tr>
 * <tr><td><code>D4</code></td><td><code>11010100</code></td><td>LIST_8</td><td>List (fewer than 2<sup>8</sup> items)</td></tr>
 * <tr><td><code>D5</code></td><td><code>11010101</code></td><td>LIST_16</td><td>List (fewer than 2<sup>16</sup> items)</td></tr>
 * <tr><td><code>D6</code></td><td><code>11010110</code></td><td>LIST_32</td><td>List (fewer than 2<sup>32</sup> items)</td></tr>
 * <tr><td><code>D7</code></td><td><code>11010111</code></td><td><em>RESERVED</em></td><td></td></tr>
 * <tr><td><code>D8</code></td><td><code>11011000</code></td><td>MAP_8</td><td>Map (fewer than 2<sup>8</sup> key:value pairs)</td></tr>
 * <tr><td><code>D9</code></td><td><code>11011001</code></td><td>MAP_16</td><td>Map (fewer than 2<sup>16</sup> key:value pairs)</td></tr>
 * <tr><td><code>DA</code></td><td><code>11011010</code></td><td>MAP_32</td><td>Map (fewer than 2<sup>32</sup> key:value pairs)</td></tr>
 * <tr><td><code>DB</code></td><td><code>11011011</code></td><td><em>RESERVED</em></td><td></td></tr>
 * <tr><td><code>DC</code></td><td><code>11011100</code></td><td>STRUCT_8</td><td>Structure (fewer than 2<sup>8</sup> fields)</td></tr>
 * <tr><td><code>DD</code></td><td><code>11011101</code></td><td>STRUCT_16</td><td>Structure (fewer than 2<sup>16</sup> fields)</td></tr>
 * <tr><td><code>DE</code></td><td><code>11011110</code></td><td>STRUCT_32</td><td>Structure (fewer than 2<sup>32</sup> fields)</td></tr>
 * <tr><td><code>DF</code></td><td><code>11011111</code></td><td><em>RESERVED</em></td><td></td></tr>
 * <tr><td><code>E0..EF</code></td><td><code>1110xxxx</code></td><td><em>RESERVED</em></td><td></td></tr>
 * <tr><td><code>F0..FF</code></td><td><code>1111xxxx</code></td><td>-TINY_INT</td><td>Integer -1 to -16</td></tr>
 * </table>
 *
 */
public class PackStream
{
    
    public static final byte TINY_TEXT = (byte) 0x80;
    public static final byte TINY_LIST = (byte) 0x90;
    public static final byte TINY_MAP = (byte) 0xA0;
    public static final byte TINY_STRUCT = (byte) 0xB0;
    public static final byte NULL = (byte) 0xC0;
    public static final byte FLOAT_64 = (byte) 0xC1;
    public static final byte FALSE = (byte) 0xC2;
    public static final byte TRUE = (byte) 0xC3;
    public static final byte RESERVED_C4 = (byte) 0xC4;
    public static final byte RESERVED_C5 = (byte) 0xC5;
    public static final byte RESERVED_C6 = (byte) 0xC6;
    public static final byte RESERVED_C7 = (byte) 0xC7;
    public static final byte INT_8 = (byte) 0xC8;
    public static final byte INT_16 = (byte) 0xC9;
    public static final byte INT_32 = (byte) 0xCA;
    public static final byte INT_64 = (byte) 0xCB;
    public static final byte BYTES_8 = (byte) 0xCC;
    public static final byte BYTES_16 = (byte) 0xCD;
    public static final byte BYTES_32 = (byte) 0xCE;
    public static final byte RESERVED_CF = (byte) 0xCF;
    public static final byte TEXT_8 = (byte) 0xD0;
    public static final byte TEXT_16 = (byte) 0xD1;
    public static final byte TEXT_32 = (byte) 0xD2;
    public static final byte RESERVED_D3 = (byte) 0xD3;
    public static final byte LIST_8 = (byte) 0xD4;
    public static final byte LIST_16 = (byte) 0xD5;
    public static final byte LIST_32 = (byte) 0xD6;
    public static final byte RESERVED_D7 = (byte) 0xD7;
    public static final byte MAP_8 = (byte) 0xD8;
    public static final byte MAP_16 = (byte) 0xD9;
    public static final byte MAP_32 = (byte) 0xDA;
    public static final byte RESERVED_DB = (byte) 0xDB;
    public static final byte STRUCT_8 = (byte) 0xDC;
    public static final byte STRUCT_16 = (byte) 0xDD;
    public static final byte RESERVED_DE = (byte) 0xDE;
    public static final byte RESERVED_DF = (byte) 0xDF;
    public static final byte RESERVED_E0 = (byte) 0xE0;
    public static final byte RESERVED_E1 = (byte) 0xE1;
    public static final byte RESERVED_E2 = (byte) 0xE2;
    public static final byte RESERVED_E3 = (byte) 0xE3;
    public static final byte RESERVED_E4 = (byte) 0xE4;
    public static final byte RESERVED_E5 = (byte) 0xE5;
    public static final byte RESERVED_E6 = (byte) 0xE6;
    public static final byte RESERVED_E7 = (byte) 0xE7;
    public static final byte RESERVED_E8 = (byte) 0xE8;
    public static final byte RESERVED_E9 = (byte) 0xE9;
    public static final byte RESERVED_EA = (byte) 0xEA;
    public static final byte RESERVED_EB = (byte) 0xEB;
    public static final byte RESERVED_EC = (byte) 0xEC;
    public static final byte RESERVED_ED = (byte) 0xED;
    public static final byte RESERVED_EE = (byte) 0xEE;
    public static final byte RESERVED_EF = (byte) 0xEF;

    private static final long PLUS_2_TO_THE_31  = 2147483648L;
    private static final long PLUS_2_TO_THE_15  = 32768L;
    private static final long PLUS_2_TO_THE_7   = 128L;
    private static final long MINUS_2_TO_THE_4  = -16L;
    private static final long MINUS_2_TO_THE_7  = -128L;
    private static final long MINUS_2_TO_THE_15 = -32768L;
    private static final long MINUS_2_TO_THE_31 = -2147483648L;

    private static final Charset UTF_8 = Charset.forName( "UTF-8" );

    private static final int DEFAULT_BUFFER_CAPACITY = 8192;

    private PackStream() {}

    public static class Packer
    {
        private final ByteBuffer buffer;
        private WritableByteChannel channel;

        public Packer( WritableByteChannel channel )
        {
            this( channel, DEFAULT_BUFFER_CAPACITY );
            this.reset(channel);
        }

        public Packer( WritableByteChannel channel, int bufferCapacity )
        {
            this(bufferCapacity);
            this.reset(channel);
        }

        public Packer( int bufferCapacity )
        {
            this.buffer = ByteBuffer.allocateDirect( bufferCapacity ).order( ByteOrder.BIG_ENDIAN );
        }

        public void reset( WritableByteChannel channel )
        {
            this.channel = channel;
            this.buffer.clear();
        }

        private void ensure( int size ) throws IOException
        {
            if ( buffer.remaining() < size )
            {
                flush();
            }
        }

        public void flush() throws IOException
        {
            buffer.flip();
            do { channel.write( buffer ); } while( buffer.remaining() > 0 );
            buffer.clear();
        }

        public void packRaw( byte[] data ) throws IOException
        {
            int index = 0;
            while(index < data.length)
            {
                int amountToWrite = Math.min( buffer.remaining(), data.length - index );

                buffer.put( data, index, amountToWrite );
                index += amountToWrite;

                if(buffer.remaining() == 0)
                {
                    flush();
                }
            }
        }

        public ByteBuffer packNull() throws IOException
        {
            ensure( 1 );
            return buffer.put( NULL );
        }

        public void pack( boolean value ) throws IOException
        {
            ensure( 1 );
            buffer.put( value ? TRUE : FALSE );
        }

        public void pack( long value ) throws IOException
        {
            if ( value >= MINUS_2_TO_THE_4 && value < PLUS_2_TO_THE_7)
            {
                ensure( 1 );
                buffer.put( (byte) value );
            }
            else if ( value >= MINUS_2_TO_THE_7 && value < MINUS_2_TO_THE_4 )
            {
                ensure( 2 );
                buffer.put( INT_8 );
                buffer.put( (byte) value );
            }
            else if ( value >= MINUS_2_TO_THE_15 && value < PLUS_2_TO_THE_15 )
            {
                ensure( 3 );
                buffer.put( INT_16 );
                buffer.putShort( (short) value );
            }
            else if ( value >= MINUS_2_TO_THE_31 && value < PLUS_2_TO_THE_31 )
            {
                ensure( 5 );
                buffer.put( INT_32 );
                buffer.putInt( (int) value );
            }
            else
            {
                ensure( 9 );
                buffer.put( INT_64 );
                buffer.putLong( value );
            }
        }

        public void pack( double value ) throws IOException
        {
            ensure( 9 );
            buffer.put( FLOAT_64 );
            buffer.putDouble( value );
        }

        public void pack( byte[] values ) throws IOException
        {
            if ( values == null ) { packNull(); }
            else
            {
                packBytesHeader( values.length );
                packRaw( values );
            }
        }

        public void pack( String value ) throws IOException
        {
            if ( value == null ) { packNull(); }
            else
            {
                byte[] utf8 = value.getBytes( UTF_8 );
                packTextHeader( utf8.length );
                packRaw( utf8 );
            }
        }

        public void packText( byte[] utf8 ) throws IOException
        {
            if ( utf8 == null ) { packNull(); }
            else
            {
                packTextHeader( utf8.length );
                packRaw( utf8 );
            }
        }

        public void pack( List values ) throws IOException
        {
            if ( values == null ) { packNull(); }
            else
            {
                packListHeader( values.size() );
                for ( Object value : values )
                {
                    pack( value );
                }
            }
        }

        public void pack( Map values ) throws IOException
        {
            if ( values == null ) { packNull(); }
            else
            {
                packMapHeader( values.size() );
                for ( Object key : values.keySet() )
                {
                    pack( key );
                    pack( values.get( key ) );
                }
            }
        }

        public void pack( Object value ) throws IOException
        {
            if ( value == null ) { packNull(); }
            else if ( value instanceof Boolean ) { pack( (boolean) value ); }
            else if ( value instanceof boolean[] ) { pack( asList( value ) ); }
            else if ( value instanceof Byte ) { pack( (byte) value ); }
            else if ( value instanceof byte[] ) { pack( (byte[]) value ); }
            else if ( value instanceof Short ) { pack( (short) value ); }
            else if ( value instanceof short[] ) { pack( asList( value ) ); }
            else if ( value instanceof Integer ) { pack( (int) value ); }
            else if ( value instanceof int[] ) { pack( asList( value ) ); }
            else if ( value instanceof Long ) { pack( (long) value ); }
            else if ( value instanceof long[] ) { pack( asList( value ) ); }
            else if ( value instanceof Float ) { pack( (float) value ); }
            else if ( value instanceof float[] ) { pack( asList( value ) ); }
            else if ( value instanceof Double ) { pack( (double) value ); }
            else if ( value instanceof double[] ) { pack( asList( value ) ); }
            else if ( value instanceof Character ) { pack( Character.toString( (char) value ) ); }
            else if ( value instanceof char[] ) { pack( new String( (char[]) value ) ); }
            else if ( value instanceof String ) { pack( (String) value ); }
            else if ( value instanceof String[] ) { pack( asList( value ) ); }
            else if ( value instanceof List ) { pack( (List) value ); }
            else if ( value instanceof Map ) { pack( (Map) value ); }
            else { throw new Unpackable( format( "Cannot pack object %s", value ) );}
        }

        public void packBytesHeader( int size ) throws IOException
        {
            if ( size <= Byte.MAX_VALUE )
            {
                ensure( 2 );
                buffer.put( BYTES_8 );
                buffer.put( (byte) size );
            }
            else if ( size <= Short.MAX_VALUE )
            {
                ensure( 3 );
                buffer.put( BYTES_16 );
                buffer.putShort( (short) size );
            }
            else
            {
                ensure( 5 );
                buffer.put( BYTES_32 );
                buffer.putInt( size );
            }
        }

        public void packTextHeader( int size ) throws IOException
        {
            if ( size < 0x10 )
            {
                ensure( 1 );
                buffer.put( (byte) (TINY_TEXT | size) );
            }
            else if ( size <= Byte.MAX_VALUE )
            {
                ensure( 2 );
                buffer.put( TEXT_8 );
                buffer.put( (byte) size );
            }
            else if ( size <= Short.MAX_VALUE )
            {
                ensure( 3 );
                buffer.put( TEXT_16 );
                buffer.putShort( (short) size );
            }
            else
            {
                ensure( 5 );
                buffer.put( TEXT_32 );
                buffer.putInt( size );
            }
        }

        public void packListHeader( int size ) throws IOException
        {
            if ( size < 0x10 )
            {
                ensure( 1 );
                buffer.put( (byte) (TINY_LIST | size) );
            }
            else if ( size <= Byte.MAX_VALUE )
            {
                ensure( 2 );
                buffer.put( LIST_8 );
                buffer.put( (byte) size );
            }
            else if ( size <= Short.MAX_VALUE )
            {
                ensure( 3 );
                buffer.put( LIST_16 );
                buffer.putShort( (short) size );
            }
            else
            {
                ensure( 5 );
                buffer.put( LIST_32 );
                buffer.putInt( size );
            }
        }

        public void packMapHeader( int size ) throws IOException
        {
            if ( size < 0x10 )
            {
                ensure( 1 );
                buffer.put( (byte) (TINY_MAP | size) );
            }
            else if ( size <= Byte.MAX_VALUE )
            {
                ensure( 2 );
                buffer.put( MAP_8 );
                buffer.put( (byte) size );
            }
            else if ( size <= Short.MAX_VALUE )
            {
                ensure( 3 );
                buffer.put( MAP_16 );
                buffer.putShort( (short) size );
            }
            else
            {
                ensure( 5 );
                buffer.put( MAP_32 );
                buffer.putInt( size );
            }
        }

        public void packStructHeader( int size, char signature ) throws IOException
        {
            if ( size < 0x10 )
            {
                ensure( 2 );
                buffer.put( (byte) (TINY_STRUCT | size) );
                buffer.put( (byte) signature );
            }
            else if ( size <= Byte.MAX_VALUE )
            {
                ensure( 3 );
                buffer.put( STRUCT_8 );
                buffer.put( (byte) size );
                buffer.put( (byte) signature );
            }
            else if ( size <= Short.MAX_VALUE )
            {
                ensure( 4 );
                buffer.put( STRUCT_16 );
                buffer.putShort( (short) size );
                buffer.put( (byte) signature );
            }
            else
            {
                throw new Overflow(
                        "Structures cannot have more than " + Short.MAX_VALUE + " fields" );
            }
        }

    }

    public static class Unpacker
    {

        private final ByteBuffer buffer;
        private ReadableByteChannel channel;

        public Unpacker( ReadableByteChannel channel )
        {
            this( DEFAULT_BUFFER_CAPACITY );
            reset( channel );
        }

        public Unpacker( int bufferCapacity )
        {
            assert bufferCapacity >= 8 : "Buffer must be at least 8 bytes.";
            this.buffer = ByteBuffer.allocateDirect( bufferCapacity ).order( ByteOrder.BIG_ENDIAN );
        }

        public Unpacker reset( ReadableByteChannel ch )
        {
            this.channel = ch;
            this.buffer.position(0);
            this.buffer.limit(0);
            return this;
        }

        public boolean hasNext() throws IOException
        {
            return buffer.remaining() > 0 || attempt( 1 );
        }

        private void ensure( int numBytes ) throws IOException
        {
            if(!attempt( numBytes ))
            {
                throw new EndOfStream( "Unexpected end of stream while trying to read " + numBytes + " bytes." );
            }
        }

        private boolean attempt( int numBytes ) throws IOException
        {
            if(buffer.remaining() >= numBytes)
            {
                return true;
            }

            if(buffer.remaining() > 0)
            {
                // If there is data remaining in the buffer, shift that remaining data to the beginning of the buffer.
                buffer.compact();
            }
            else
            {
                buffer.clear();
            }

            int count;
            do
            {
                count = channel.read( buffer );
            } while( count >= 0 && (buffer.position() < numBytes && buffer.remaining() != 0));

            buffer.flip();
            return buffer.remaining() >= numBytes;
        }

        public long unpackStructHeader() throws IOException
        {
            ensure( 1 );
            final byte markerByte = buffer.get();
            final byte markerHighNibble = (byte) (markerByte & 0xF0);
            final byte markerLowNibble = (byte) (markerByte & 0x0F);

            if ( markerHighNibble == TINY_STRUCT ) { return markerLowNibble; }
            switch(markerByte)
            {
            case STRUCT_8: return unpackUINT8();
            case STRUCT_16: return unpackUINT16();
            default: throw new Unexpected( "Expected a struct, but got: " + toHexString( markerByte ));
            }
        }

        public char unpackStructSignature() throws IOException
        {
            ensure( 1 );
            return (char) buffer.get();
        }

        public long unpackListHeader() throws IOException
        {
            ensure( 1 );
            final byte markerByte = buffer.get();
            final byte markerHighNibble = (byte) (markerByte & 0xF0);
            final byte markerLowNibble  = (byte) (markerByte & 0x0F);

            if ( markerHighNibble == TINY_LIST ) { return markerLowNibble; }
            switch(markerByte)
            {
            case LIST_8: return unpackUINT8();
            case LIST_16: return unpackUINT16();
            case LIST_32: return unpackUINT32();
            default: throw new Unexpected( "Expected a list, but got: " + toHexString( markerByte ));
            }
        }

        public long unpackMapHeader() throws IOException
        {
            ensure( 1 );
            final byte markerByte = buffer.get();
            final byte markerHighNibble = (byte) (markerByte & 0xF0);
            final byte markerLowNibble = (byte) (markerByte & 0x0F);

            if ( markerHighNibble == TINY_MAP ) { return markerLowNibble; }
            switch(markerByte)
            {
            case MAP_8: return unpackUINT8();
            case MAP_16: return unpackUINT16();
            case MAP_32: return unpackUINT32();
            default: throw new Unexpected( "Expected a map, but got: " + toHexString( markerByte ));
            }
        }

        public long unpackLong() throws IOException
        {
            ensure( 1 );
            final byte markerByte = buffer.get();
            if ( markerByte >= MINUS_2_TO_THE_4) { return markerByte; }
            switch(markerByte)
            {
            case INT_8:   ensure(1); return buffer.get();
            case INT_16:  ensure(2); return buffer.getShort();
            case INT_32:  ensure(4); return buffer.getInt();
            case INT_64:  ensure(8); return buffer.getLong();
            default: throw new Unexpected( "Expected an integer, but got: " + toHexString( markerByte ));
            }
        }

        public double unpackDouble() throws IOException
        {
            ensure( 1 );
            final byte markerByte = buffer.get();
            if(markerByte == FLOAT_64)
            {
                ensure( 8 );
                return buffer.getDouble();
            }
            throw new Unexpected( "Expected a double, but got: " + toHexString( markerByte ));
        }

        public String unpackString() throws IOException
        {
            return new String(unpackUtf8(), UTF_8);
        }

        public byte[] unpackUtf8() throws IOException
        {
            ensure( 1 );
            final byte markerByte = buffer.get();
            final byte markerHighNibble = (byte) (markerByte & 0xF0);
            final byte markerLowNibble = (byte) (markerByte & 0x0F);

            if ( markerHighNibble == TINY_TEXT ) { return unpackBytes( markerLowNibble ); }
            switch(markerByte)
            {
            case TEXT_8: return unpackBytes( unpackUINT8() );
            case TEXT_16: return unpackBytes( unpackUINT16() );
            case TEXT_32:
            {
                long size = unpackUINT32();
                if ( size <= Integer.MAX_VALUE )
                {
                    return unpackBytes( (int) size );
                }
                else
                {
                    throw new Overflow( "TEXT_32 too long for Java" );
                }
            }
            default: throw new Unexpected( "Expected a string, but got: " + toHexString( markerByte ));
            }
        }

        public boolean unpackBoolean() throws IOException
        {
            ensure( 1 );
            final byte markerByte = buffer.get();
            switch(markerByte)
            {
            case TRUE: return true;
            case FALSE: return false;
            default: throw new Unexpected( "Expected a boolean, but got: " + toHexString( markerByte ));
            }
        }

        public PackValue unpack() throws IOException
        {
            ensure( 1 );
            final byte markerByte = buffer.get();
            final byte markerHighNibble = (byte) (markerByte & 0xF0);
            final byte markerLowNibble = (byte) (markerByte & 0x0F);

            if ( markerByte == NULL )
            {
                return NULL_VALUE;
            }
            else if ( markerByte == TRUE )
            {
                return PackValue.BooleanValue.TRUE;
            }
            else if ( markerByte == FALSE )
            {
                return PackValue.BooleanValue.FALSE;
            }
            else if ( markerByte == FLOAT_64 )
            {
                ensure( 8 );
                return new PackValue.FloatValue( buffer.getDouble() );
            }
            else if ( markerByte == INT_8 )
            {
                ensure( 1 );
                return PackValue.IntegerValue.getInstance( buffer.get() );
            }
            else if ( markerByte == INT_16 )
            {
                ensure( 2 );
                return PackValue.IntegerValue.getInstance( buffer.getShort() );
            }
            else if ( markerByte == INT_32 )
            {
                ensure( 4 );
                return PackValue.IntegerValue.getInstance( buffer.getInt() );
            }
            else if ( markerByte == INT_64 )
            {
                ensure( 8 );
                return PackValue.IntegerValue.getInstance( buffer.getLong() );
            }
            else if ( markerByte == BYTES_8 )
            {
                return PackValue.BytesValue.getInstance( unpackBytes( unpackUINT8() ) );
            }
            else if ( markerByte == BYTES_16 )
            {
                return PackValue.BytesValue.getInstance( unpackBytes( unpackUINT16() ) );
            }
            else if ( markerByte == BYTES_32 )
            {
                long size = unpackUINT32();
                if ( size <= Integer.MAX_VALUE )
                {
                    return PackValue.BytesValue.getInstance( unpackBytes( (int) size ) );
                }
                else
                {
                    throw new Overflow( "BYTES_32 too long for Java" );
                }
            }
            else if ( markerHighNibble == TINY_TEXT )
            {
                return PackValue.TextValue.getInstance( unpackBytes( markerLowNibble ) );
            }
            else if ( markerByte == TEXT_8 )
            {
                return PackValue.TextValue.getInstance( unpackBytes( unpackUINT8() ) );
            }
            else if ( markerByte == TEXT_16 )
            {
                return PackValue.TextValue.getInstance( unpackBytes( unpackUINT16() ) );
            }
            else if ( markerByte == TEXT_32 )
            {
                long size = unpackUINT32();
                if ( size <= Integer.MAX_VALUE )
                {
                    return PackValue.TextValue.getInstance( unpackBytes( (int) size ) );
                }
                else
                {
                    throw new Overflow( "TEXT_32 too long for Java" );
                }
            }
            else if ( markerHighNibble == TINY_LIST )
            {
                return PackValue.ListValue.getInstance( unpackList( markerLowNibble ) );
            }
            else if ( markerByte == LIST_8 )
            {
                return PackValue.ListValue.getInstance( unpackList( unpackUINT8() ) );
            }
            else if ( markerByte == LIST_16 )
            {
                return PackValue.ListValue.getInstance( unpackList( unpackUINT16() ) );
            }
            else if ( markerByte == LIST_32 )
            {
                long size = unpackUINT32();
                if ( size <= Integer.MAX_VALUE )
                {
                    return PackValue.ListValue.getInstance( unpackList( (int) size ) );
                }
                else
                {
                    throw new Overflow( "LIST_32 too long for Java" );
                }
            }
            else if ( markerHighNibble == TINY_MAP )
            {
                return PackValue.MapValue.getInstance( unpackMap( markerLowNibble ) );
            }
            else if ( markerByte == MAP_8 )
            {
                return PackValue.MapValue.getInstance( unpackMap( unpackUINT8() ) );
            }
            else if ( markerByte == MAP_16 )
            {
                return PackValue.MapValue.getInstance( unpackMap( unpackUINT16() ) );
            }
            else if ( markerByte == MAP_32 )
            {
                long size = unpackUINT32();
                if ( size <= Integer.MAX_VALUE )
                {
                    return PackValue.MapValue.getInstance( unpackMap( (int) size ) );
                }
                else
                {
                    throw new Overflow( "MAP_32 too long for Java" );
                }
            }
            else if ( markerHighNibble == TINY_STRUCT )
            {
                char signature = unpackStructSignature();
                return PackValue.StructValue.getInstance( signature, unpackList( markerLowNibble ) );
            }
            else if ( markerByte == STRUCT_8 )
            {
                int size = unpackUINT8();
                char signature = unpackStructSignature();
                return PackValue.StructValue.getInstance( signature, unpackList( size ) );
            }
            else if ( markerByte == STRUCT_16 )
            {
                int size = unpackUINT16();
                char signature = unpackStructSignature();
                return PackValue.StructValue.getInstance( signature, unpackList( size ) );
            }
            else { return PackValue.IntegerValue.getInstance( markerByte ); }
        }

        private int unpackUINT8() throws IOException
        {
            ensure( 1 );
            return buffer.get() & 0xFF;
        }

        private int unpackUINT16() throws IOException
        {
            ensure( 2 );
            return buffer.getShort() & 0xFFFF;
        }

        private long unpackUINT32() throws IOException
        {
            ensure( 4 );
            return buffer.getInt() & 0xFFFFFFFFL;
        }

        private byte[] unpackBytes( int size ) throws IOException
        {
            if ( size == 0 ) return EMPTY_BYTE_ARRAY;
            byte[] heapBuffer = new byte[size];
            int index = 0;
            while(index < size)
            {
                int toRead = Math.min( buffer.remaining(), size - index );
                buffer.get( heapBuffer, index, toRead );
                index += toRead;

                if(buffer.remaining() == 0 && index < size)
                {
                    ensure( Math.min(size - index, buffer.capacity()) );
                }
            }
            return heapBuffer;
        }

        private List<PackValue> unpackList( int size ) throws IOException
        {
            if ( size == 0 ) return EMPTY_LIST_OF_VALUES;
            List<PackValue> list = new ArrayList<>( size );
            for ( int i = 0; i < size; i++ )
            {
                list.add( unpack() );
            }
            return list;
        }

        private Map<String,PackValue> unpackMap( int size ) throws IOException
        {
            if ( size == 0 ) return EMPTY_MAP_OF_VALUES;
            Map<String,PackValue> map = new LinkedHashMap<>( size );
            for ( int i = 0; i < size; i++ )
            {
                String key = unpack().stringValue();
                if ( key != null )
                {
                    PackValue value = unpack();
                    map.put( key, value );
                }
            }
            return map;
        }

        public PackType peekNextType() throws IOException
        {
            ensure( 1 );
            final byte markerByte = buffer.get(buffer.position());
            final byte markerHighNibble = (byte) (markerByte & 0xF0);

            switch(markerHighNibble)
            {
            case TINY_TEXT:   return PackType.TEXT;
            case TINY_LIST:   return PackType.LIST;
            case TINY_MAP:    return PackType.MAP;
            case TINY_STRUCT: return PackType.STRUCT;
            }

            switch(markerByte)
            {
            case NULL:
                return PackType.NULL;
            case TRUE:
            case FALSE:
                return PackType.BOOLEAN;
            case FLOAT_64:
                return PackType.FLOAT;
            case BYTES_8:
            case BYTES_16:
            case BYTES_32:
                return PackType.BYTES;
            case TEXT_8:
            case TEXT_16:
            case TEXT_32:
                return PackType.TEXT;
            case LIST_8:
            case LIST_16:
            case LIST_32:
                return PackType.LIST;
            case MAP_8:
            case MAP_16:
            case MAP_32:
                return PackType.MAP;
            case STRUCT_8:
            case STRUCT_16:
                return PackType.STRUCT;
            default:
                return PackType.INTEGER;
            }
        }
    }

    public static class PackstreamException extends IOException
    {
        public PackstreamException( String message )
        {
            super( message );
        }
    }

    public static class EndOfStream extends PackstreamException
    {
        public EndOfStream( String message )
        {
            super( message );
        }
    }

    public static class Overflow extends PackstreamException
    {
        public Overflow( String message )
        {
            super( message );
        }
    }

    public static class Unexpected extends PackstreamException
    {
        public Unexpected( String message )
        {
            super( message );
        }
    }

    public static class Unpackable extends PackstreamException
    {
        public Unpackable( String message )
        {
            super( message );
        }
    }

}
