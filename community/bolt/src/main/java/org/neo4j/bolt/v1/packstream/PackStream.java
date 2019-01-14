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
package org.neo4j.bolt.v1.packstream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.neo4j.bolt.messaging.StructType;
import org.neo4j.bolt.v1.packstream.utf8.UTF8Encoder;

/**
 * PackStream is a messaging serialisation format heavily inspired by MessagePack.
 * The key differences are in the type system itself which (among other things) replaces extensions with structures.
 * The Packer and Unpacker implementations are also faster than their MessagePack counterparts.
 * <p/>
 * Note that several marker byte values are RESERVED for future use.
 * Extra markers should <em>not</em> be added casually and such additions must be follow a strict process involving both
 * client and server software.
 * <p/>
 * The table below shows all allocated marker byte values.
 * <p/>
 * <table>
 * <tr><th>Marker</th><th>Binary</th><th>Type</th><th>Description</th></tr>
 * <tr><td><code>00..7F</code></td><td><code>0xxxxxxx</code></td><td>+TINY_INT</td><td>Integer 0 to 127</td></tr>
 * <tr><td><code>80..8F</code></td><td><code>1000xxxx</code></td><td>TINY_STRING</td><td></td></tr>
 * <tr><td><code>90..9F</code></td><td><code>1001xxxx</code></td><td>TINY_LIST</td><td></td></tr>
 * <tr><td><code>A0..AF</code></td><td><code>1010xxxx</code></td><td>TINY_MAP</td><td></td></tr>
 * <tr><td><code>B0..BF</code></td><td><code>1011xxxx</code></td><td>TINY_STRUCT</td><td></td></tr>
 * <tr><td><code>C0</code></td><td><code>11000000</code></td><td>NULL</td><td></td></tr>
 * <tr><td><code>C1</code></td><td><code>11000001</code></td><td>FLOAT_64</td><td>64-bit floating point number
 * (double)</td></tr>
 * <tr><td><code>C2</code></td><td><code>11000010</code></td><td>FALSE</td><td>Boolean false</td></tr>
 * <tr><td><code>C3</code></td><td><code>11000011</code></td><td>TRUE</td><td>Boolean true</td></tr>
 * <tr><td><code>C4..C7</code></td><td><code>110001xx</code></td><td><em>RESERVED</em></td><td></td></tr>
 * <tr><td><code>C8</code></td><td><code>11001000</code></td><td>INT_8</td><td>8-bit signed integer</td></tr>
 * <tr><td><code>C9</code></td><td><code>11001001</code></td><td>INT_8</td><td>16-bit signed integer</td></tr>
 * <tr><td><code>CA</code></td><td><code>11001010</code></td><td>INT_8</td><td>32-bit signed integer</td></tr>
 * <tr><td><code>CB</code></td><td><code>11001011</code></td><td>INT_8</td><td>64-bit signed integer</td></tr>
 * <tr><td><code>CC</code></td><td><code>11001100</code></td><td>BYTES_8</td><td>Byte string (fewer than 2<sup>8</sup>
 * bytes)</td></tr>
 * <tr><td><code>CD</code></td><td><code>11001101</code></td><td>BYTES_16</td><td>Byte string (fewer than 2<sup>16</sup>
 * bytes)</td></tr>
 * <tr><td><code>CE</code></td><td><code>11001110</code></td><td>BYTES_32</td><td>Byte string (fewer than 2<sup>32</sup>
 * bytes)</td></tr>
 * <tr><td><code>CF</code></td><td><code>11001111</code></td><td><em>RESERVED</em></td><td></td></tr>
 * <tr><td><code>D0</code></td><td><code>11010000</code></td><td>STRING_8</td><td>UTF-8 encoded string (fewer than
 * 2<sup>8</sup> bytes)</td></tr>
 * <tr><td><code>D1</code></td><td><code>11010001</code></td><td>STRING_16</td><td>UTF-8 encoded string (fewer than
 * 2<sup>16</sup> bytes)</td></tr>
 * <tr><td><code>D2</code></td><td><code>11010010</code></td><td>STRING_32</td><td>UTF-8 encoded string (fewer than
 * 2<sup>32</sup> bytes)</td></tr>
 * <tr><td><code>D3</code></td><td><code>11010011</code></td><td><em>RESERVED</em></td><td></td></tr>
 * <tr><td><code>D4</code></td><td><code>11010100</code></td><td>LIST_8</td><td>List (fewer than 2<sup>8</sup>
 * items)</td></tr>
 * <tr><td><code>D5</code></td><td><code>11010101</code></td><td>LIST_16</td><td>List (fewer than 2<sup>16</sup>
 * items)</td></tr>
 * <tr><td><code>D6</code></td><td><code>11010110</code></td><td>LIST_32</td><td>List (fewer than 2<sup>32</sup>
 * items)</td></tr>
 * <tr><td><code>D7</code></td><td><code>11010111</code></td><td><em>RESERVED</em></td><td></td></tr>
 * <tr><td><code>D8</code></td><td><code>11011000</code></td><td>MAP_8</td><td>Map (fewer than 2<sup>8</sup> key:value
 * pairs)</td></tr>
 * <tr><td><code>D9</code></td><td><code>11011001</code></td><td>MAP_16</td><td>Map (fewer than 2<sup>16</sup> key:value
 * pairs)</td></tr>
 * <tr><td><code>DA</code></td><td><code>11011010</code></td><td>MAP_32</td><td>Map (fewer than 2<sup>32</sup> key:value
 * pairs)</td></tr>
 * <tr><td><code>DB</code></td><td><code>11011011</code></td><td><em>RESERVED</em></td><td></td></tr>
 * <tr><td><code>DC</code></td><td><code>11011100</code></td><td>STRUCT_8</td><td>Structure (fewer than 2<sup>8</sup>
 * fields)</td></tr>
 * <tr><td><code>DD</code></td><td><code>11011101</code></td><td>STRUCT_16</td><td>Structure (fewer than 2<sup>16</sup>
 * fields)</td></tr>
 * <tr><td><code>DE</code></td><td><code>11011110</code></td><td>STRUCT_32</td><td>Structure (fewer than 2<sup>32</sup>
 * fields)</td></tr>
 * <tr><td><code>DF</code></td><td><code>11011111</code></td><td><em>RESERVED</em></td><td></td></tr>
 * <tr><td><code>E0..EF</code></td><td><code>1110xxxx</code></td><td><em>RESERVED</em></td><td></td></tr>
 * <tr><td><code>F0..FF</code></td><td><code>1111xxxx</code></td><td>-TINY_INT</td><td>Integer -1 to -16</td></tr>
 * </table>
 */
public class PackStream
{

    public static final byte TINY_STRING = (byte) 0x80;
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
    public static final byte STRING_8 = (byte) 0xD0;
    public static final byte STRING_16 = (byte) 0xD1;
    public static final byte STRING_32 = (byte) 0xD2;
    public static final byte RESERVED_D3 = (byte) 0xD3;
    public static final byte LIST_8 = (byte) 0xD4;
    public static final byte LIST_16 = (byte) 0xD5;
    public static final byte LIST_32 = (byte) 0xD6;
    public static final byte LIST_STREAM = (byte) 0xD7;
    public static final byte MAP_8 = (byte) 0xD8;
    public static final byte MAP_16 = (byte) 0xD9;
    public static final byte MAP_32 = (byte) 0xDA;
    public static final byte MAP_STREAM = (byte) 0xDB;
    public static final byte STRUCT_8 = (byte) 0xDC;
    public static final byte STRUCT_16 = (byte) 0xDD;
    public static final byte RESERVED_DE = (byte) 0xDE;
    public static final byte END_OF_STREAM = (byte) 0xDF;
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

    public static final long UNKNOWN_SIZE = -1;

    private static final long PLUS_2_TO_THE_31 = 2147483648L;
    private static final long PLUS_2_TO_THE_15 = 32768L;
    private static final long PLUS_2_TO_THE_7 = 128L;
    private static final long MINUS_2_TO_THE_4 = -16L;
    private static final long MINUS_2_TO_THE_7 = -128L;
    private static final long MINUS_2_TO_THE_15 = -32768L;
    private static final long MINUS_2_TO_THE_31 = -2147483648L;

    private PackStream()
    {
    }

    private static PackType type( byte markerByte )
    {
        final byte markerHighNibble = (byte) (markerByte & 0xF0);

        switch ( markerHighNibble )
        {
        case TINY_STRING:
            return PackType.STRING;
        case TINY_LIST:
            return PackType.LIST;
        case TINY_MAP:
            return PackType.MAP;
        case TINY_STRUCT:
            return PackType.STRUCT;
        default:
            break;
        }

        if ( markerByte >= MINUS_2_TO_THE_4 )
        {
            return PackType.INTEGER;
        }

        switch ( markerByte )
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
        case STRING_8:
        case STRING_16:
        case STRING_32:
            return PackType.STRING;
        case LIST_8:
        case LIST_16:
        case LIST_32:
        case LIST_STREAM:
            return PackType.LIST;
        case MAP_8:
        case MAP_16:
        case MAP_32:
        case MAP_STREAM:
            return PackType.MAP;
        case STRUCT_8:
        case STRUCT_16:
            return PackType.STRUCT;
        case END_OF_STREAM:
            return PackType.END_OF_STREAM;
        case INT_8:
        case INT_16:
        case INT_32:
        case INT_64:
            return PackType.INTEGER;
        default:
            return PackType.RESERVED;
        }
    }

    public static class Packer
    {
        private static final char PACKED_CHAR_START_CHAR = (char) 32;
        private static final char PACKED_CHAR_END_CHAR = (char) 126;
        private static final String[] PACKED_CHARS = prePackChars();
        private UTF8Encoder utf8 = UTF8Encoder.fastestAvailableEncoder();

        protected PackOutput out;

        public Packer( PackOutput out )
        {
            this.out = out;
        }

        private static String[] prePackChars()
        {
            int size = PACKED_CHAR_END_CHAR + 1 - PACKED_CHAR_START_CHAR;
            String[] packedChars = new String[size];
            for ( int i = 0; i < size; i++ )
            {
                packedChars[i] = String.valueOf( (char) (i + PACKED_CHAR_START_CHAR) );
            }
            return packedChars;
        }

        public void flush() throws IOException
        {
            out.flush();
        }

        public void packNull() throws IOException
        {
            out.writeByte( NULL );
        }

        public void pack( boolean value ) throws IOException
        {
            out.writeByte( value ? TRUE : FALSE );
        }

        public void pack( long value ) throws IOException
        {
            if ( value >= MINUS_2_TO_THE_4 && value < PLUS_2_TO_THE_7 )
            {
                out.writeByte( (byte) value );
            }
            else if ( value >= MINUS_2_TO_THE_7 && value < MINUS_2_TO_THE_4 )
            {
                out.writeByte( INT_8 ).writeByte( (byte) value );
            }
            else if ( value >= MINUS_2_TO_THE_15 && value < PLUS_2_TO_THE_15 )
            {
                out.writeByte( INT_16 ).writeShort( (short) value );
            }
            else if ( value >= MINUS_2_TO_THE_31 && value < PLUS_2_TO_THE_31 )
            {
                out.writeByte( INT_32 ).writeInt( (int) value );
            }
            else
            {
                out.writeByte( INT_64 ).writeLong( value );
            }
        }

        public void pack( double value ) throws IOException
        {
            out.writeByte( FLOAT_64 ).writeDouble( value );
        }

        public void pack( char character ) throws IOException
        {
            if ( character >= PACKED_CHAR_START_CHAR && character <= PACKED_CHAR_END_CHAR )
            {
                pack( PACKED_CHARS[character - PACKED_CHAR_START_CHAR] );
            }
            else
            {
                pack( String.valueOf( character ) );
            }
        }

        public void pack( byte[] value ) throws IOException
        {
            if ( value == null )
            {
                packNull();
            }
            else
            {
                packBytesHeader( value.length );
                out.writeBytes( value, 0, value.length );
            }
        }

        public void pack( String value ) throws IOException
        {
            if ( value == null )
            {
                packNull();
            }
            else
            {
                ByteBuffer encoded = utf8.encode( value );
                packStringHeader( encoded.remaining() );
                out.writeBytes( encoded );
            }
        }

        public void packUTF8( byte[] bytes, int offset, int length ) throws IOException
        {
            if ( bytes == null )
            {
                packNull();
            }
            else
            {
                packStringHeader( length );
                out.writeBytes( bytes, offset, length );
            }
        }

        protected void packBytesHeader( int size ) throws IOException
        {
            if ( size <= Byte.MAX_VALUE )
            {
                out.writeShort( (short) (BYTES_8 << 8 | size) );
            }
            else if ( size <= Short.MAX_VALUE )
            {
                out.writeByte( BYTES_16 ).writeShort( (short) size );
            }
            else
            {
                out.writeByte( BYTES_32 ).writeInt( size );
            }
        }

        private void packStringHeader( int size ) throws IOException
        {
            if ( size < 0x10 )
            {
                out.writeByte( (byte) (TINY_STRING | size) );
            }
            else if ( size <= Byte.MAX_VALUE )
            {
                out.writeShort( (short) (STRING_8 << 8 | size) );
            }
            else if ( size <= Short.MAX_VALUE )
            {
                out.writeByte( STRING_16 ).writeShort( (short) size );
            }
            else
            {
                out.writeByte( STRING_32 ).writeInt( size );
            }
        }

        public void packListHeader( int size ) throws IOException
        {
            if ( size < 0x10 )
            {
                out.writeByte( (byte) (TINY_LIST | size) );
            }
            else if ( size <= Byte.MAX_VALUE )
            {
                out.writeShort( (short) (LIST_8 << 8 | size) );
            }
            else if ( size <= Short.MAX_VALUE )
            {
                out.writeByte( LIST_16 ).writeShort( (short) size );
            }
            else
            {
                out.writeByte( LIST_32 ).writeInt( size );
            }
        }

        public void packListStreamHeader() throws IOException
        {
            out.writeByte( LIST_STREAM );
        }

        public void packMapHeader( int size ) throws IOException
        {
            if ( size < 0x10 )
            {
                out.writeByte( (byte) (TINY_MAP | size) );
            }
            else if ( size <= Byte.MAX_VALUE )
            {
                out.writeShort( (short) (MAP_8 << 8 | size) );
            }
            else if ( size <= Short.MAX_VALUE )
            {
                out.writeByte( MAP_16 ).writeShort( (short) size );
            }
            else
            {
                out.writeByte( MAP_32 ).writeInt( size );
            }
        }

        public void packMapStreamHeader() throws IOException
        {
            out.writeByte( MAP_STREAM );
        }

        public void packStructHeader( int size, byte signature ) throws IOException
        {
            if ( size < 0x10 )
            {
                out.writeShort( (short) ((byte) (TINY_STRUCT | size) << 8 | (signature & 0xFF)) );
            }
            else if ( size <= Byte.MAX_VALUE )
            {
                out.writeByte( STRUCT_8 ).writeByte( (byte) size ).writeByte( signature );
            }
            else if ( size <= Short.MAX_VALUE )
            {
                out.writeByte( STRUCT_16 ).writeShort( (short) size ).writeByte( signature );
            }
            else
            {
                throw new Overflow( "Structures cannot have more than " + Short.MAX_VALUE + " fields" );
            }
        }

        public void packEndOfStream() throws IOException
        {
            out.writeByte( END_OF_STREAM );
        }

    }

    public static class Unpacker
    {
        private static final byte[] EMPTY_BYTE_ARRAY = {};

        protected PackInput in;

        public Unpacker( PackInput in )
        {
            this.in = in;
        }

        // TODO: This currently returns the number of fields in the struct. In 99% of cases we will look at the struct
        // signature to determine how to read it, suggest we make that what we return here,
        // and have the number of fields available through some alternate optional mechanism.
        public long unpackStructHeader() throws IOException
        {
            final byte markerByte = in.readByte();
            final byte markerHighNibble = (byte) (markerByte & 0xF0);
            final byte markerLowNibble = (byte) (markerByte & 0x0F);

            if ( markerHighNibble == TINY_STRUCT )
            {
                return markerLowNibble;
            }
            switch ( markerByte )
            {
            case STRUCT_8:
                return unpackUINT8();
            case STRUCT_16:
                return unpackUINT16();
            default:
                throw new Unexpected( PackType.STRUCT, markerByte );
            }
        }

        public char unpackStructSignature() throws IOException
        {
            return (char) in.readByte();
        }

        public long unpackListHeader() throws IOException
        {
            final byte markerByte = in.readByte();
            final byte markerHighNibble = (byte) (markerByte & 0xF0);
            final byte markerLowNibble = (byte) (markerByte & 0x0F);

            if ( markerHighNibble == TINY_LIST )
            {
                return markerLowNibble;
            }
            switch ( markerByte )
            {
            case LIST_8:
                return unpackUINT8();
            case LIST_16:
                return unpackUINT16();
            case LIST_32:
                return unpackUINT32();
            case LIST_STREAM:
                return UNKNOWN_SIZE;
            default:
                throw new Unexpected( PackType.LIST, markerByte );
            }
        }

        public long unpackMapHeader() throws IOException
        {
            final byte markerByte = in.readByte();
            final byte markerHighNibble = (byte) (markerByte & 0xF0);
            final byte markerLowNibble = (byte) (markerByte & 0x0F);

            if ( markerHighNibble == TINY_MAP )
            {
                return markerLowNibble;
            }
            switch ( markerByte )
            {
            case MAP_8:
                return unpackUINT8();
            case MAP_16:
                return unpackUINT16();
            case MAP_32:
                return unpackUINT32();
            case MAP_STREAM:
                return UNKNOWN_SIZE;
            default:
                throw new Unexpected( PackType.MAP, markerByte );
            }
        }

        public int unpackInteger() throws IOException
        {
            final byte markerByte = in.readByte();
            if ( markerByte >= MINUS_2_TO_THE_4 )
            {
                return markerByte;
            }
            switch ( markerByte )
            {
            case INT_8:
                return in.readByte();
            case INT_16:
                return in.readShort();
            case INT_32:
                return in.readInt();
            case INT_64:
                throw new Overflow( "Unexpectedly large Integer value unpacked (" + in.readLong() + ")" );
            default:
                throw new Unexpected( PackType.INTEGER, markerByte );
            }
        }

        public long unpackLong() throws IOException
        {
            final byte markerByte = in.readByte();
            if ( markerByte >= MINUS_2_TO_THE_4 )
            {
                return markerByte;
            }
            switch ( markerByte )
            {
            case INT_8:
                return in.readByte();
            case INT_16:
                return in.readShort();
            case INT_32:
                return in.readInt();
            case INT_64:
                return in.readLong();
            default:
                throw new Unexpected( PackType.INTEGER, markerByte );
            }
        }

        public double unpackDouble() throws IOException
        {
            final byte markerByte = in.readByte();
            if ( markerByte == FLOAT_64 )
            {
                return in.readDouble();
            }
            throw new Unexpected( PackType.FLOAT, markerByte );
        }

        public byte[] unpackBytes() throws IOException
        {
            int size = unpackBytesHeader();
            return unpackRawBytes( size );
        }

        public String unpackString() throws IOException
        {
            return new String( unpackUTF8(), StandardCharsets.UTF_8 );
        }

        public int unpackBytesHeader() throws IOException
        {
            final byte markerByte = in.readByte();
            int size;
            switch ( markerByte )
            {
            case BYTES_8:
                size = unpackUINT8();
                break;
            case BYTES_16:
                size = unpackUINT16();
                break;
            case BYTES_32:
            {
                long longSize = unpackUINT32();
                if ( longSize <= Integer.MAX_VALUE )
                {
                    size = (int) longSize;
                }
                else
                {
                    throw new Overflow( "BYTES_32 too long for Java" );
                }
                break;
            }
            default:
                throw new Unexpected( PackType.BYTES, markerByte );
            }
            return size;
        }

        public int unpackStringHeader() throws IOException
        {
            final byte markerByte = in.readByte();
            final byte markerHighNibble = (byte) (markerByte & 0xF0);
            final byte markerLowNibble = (byte) (markerByte & 0x0F);

            int size;

            if ( markerHighNibble == TINY_STRING )
            {
                size = markerLowNibble;
            }
            else
            {
                switch ( markerByte )
                {
                case STRING_8:
                    size = unpackUINT8();
                    break;
                case STRING_16:
                    size = unpackUINT16();
                    break;
                case STRING_32:
                {
                    long longSize = unpackUINT32();
                    if ( longSize <= Integer.MAX_VALUE )
                    {
                        size = (int) longSize;
                    }
                    else
                    {
                        throw new Overflow( "STRING_32 too long for Java" );
                    }
                    break;
                }
                default:
                    throw new Unexpected( PackType.STRING, markerByte );
                }
            }

            return size;
        }

        public byte[] unpackUTF8() throws IOException
        {
            int size = unpackStringHeader();
            return unpackRawBytes( size );
        }

        public boolean unpackBoolean() throws IOException
        {
            final byte markerByte = in.readByte();
            switch ( markerByte )
            {
            case TRUE:
                return true;
            case FALSE:
                return false;
            default:
                throw new Unexpected( PackType.BOOLEAN, markerByte );
            }
        }

        public void unpackNull() throws IOException
        {
            final byte markerByte = in.readByte();
            assert markerByte == NULL;
        }

        private int unpackUINT8() throws IOException
        {
            return in.readByte() & 0xFF;
        }

        private int unpackUINT16() throws IOException
        {
            return in.readShort() & 0xFFFF;
        }

        private long unpackUINT32() throws IOException
        {
            return in.readInt() & 0xFFFFFFFFL;
        }

        public void unpackEndOfStream() throws IOException
        {
            final byte markerByte = in.readByte();
            assert markerByte == END_OF_STREAM;
        }

        private byte[] unpackRawBytes( int size ) throws IOException
        {
            if ( size == 0 )
            {
                return EMPTY_BYTE_ARRAY;
            }
            else
            {
                byte[] heapBuffer = new byte[size];
                unpackRawBytesInto( heapBuffer, 0, heapBuffer.length );
                return heapBuffer;
            }
        }

        private void unpackRawBytesInto( byte[] buffer, int offset, int size ) throws IOException
        {
            if ( size > 0 )
            {
                in.readBytes( buffer, offset, size );
            }
        }

        public PackType peekNextType() throws IOException
        {
            final byte markerByte = in.peekByte();
            return type( markerByte );
        }

        public static void ensureCorrectStructSize( StructType structType, int expected, long actual ) throws IOException
        {
            if ( expected != actual )
            {
                throw new PackStreamException(
                        String.format( "Invalid message received, serialized %s structures should have %d fields, " + "received %s structure has %d fields.",
                                structType.description(), expected, structType.description(), actual ) );
            }
        }
    }

    public static class PackStreamException extends IOException
    {
        public PackStreamException( String message )
        {
            super( message );
        }
    }

    public static class EndOfStream extends PackStreamException
    {
        public EndOfStream( String message )
        {
            super( message );
        }
    }

    public static class Overflow extends PackStreamException
    {
        public Overflow( String message )
        {
            super( message );
        }
    }

    public static class Unexpected extends PackStreamException
    {
        public Unexpected( PackType expectedType, byte unexpectedMarkerByte )
        {
            super( "Wrong type received. Expected " + expectedType + ", received: " + type( unexpectedMarkerByte ) +
                   " (0x" + Integer.toHexString( unexpectedMarkerByte ) + ")." );
        }
    }
}
