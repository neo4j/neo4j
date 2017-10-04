/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.internal.store.cursors;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedAction;

import sun.misc.Unsafe;

import static java.lang.Double.doubleToLongBits;
import static java.lang.Double.longBitsToDouble;
import static java.lang.Float.floatToIntBits;
import static java.lang.Float.intBitsToFloat;

class Memory
{
    private static final Unsafe UNSAFE;
    /** is unaligned access allowed? */
    private static final boolean UNALIGNED;
    /** is the native byte-order big endian? */
    private static final boolean BIG_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN;
    /** can arrays be copied as memory? */
    private static final boolean NATIVE_ARRAY;
    private static final long BYTE_ARRAY, SHORT_ARRAY, INT_ARRAY, LONG_ARRAY, CHAR_ARRAY, FLOAT_ARRAY, DOUBLE_ARRAY;

    static void copy( long sourceAddress, long destinationAddress, long size )
    {
        UNSAFE.copyMemory( sourceAddress, destinationAddress, size );
    }

    static byte getByte( long address )
    {
        return UNSAFE.getByte( address );
    }

    static void putByte( long address, byte value )
    {
        UNSAFE.putByte( address, value );
    }

    static short getShort( long address )
    {
        if ( UNALIGNED || (address & 0x01L) == 0 )
        {
            short value = UNSAFE.getShort( address );
            return BIG_ENDIAN ? value : Short.reverseBytes( value );
        }
        return BIG_ENDIAN ? shortB( address ) : shortL( address );
    }

    static void putShort( long address, short value )
    {
        if ( UNALIGNED || (address & 0x01L) == 0 )
        {
            UNSAFE.putShort( address, BIG_ENDIAN ? value : Short.reverseBytes( value ) );
        }
        else if ( BIG_ENDIAN )
        {
            shortB( address, value );
        }
        else
        {
            shortL( address, value );
        }
    }

    static int getInt( long address )
    {
        if ( UNALIGNED || (address & 0x03L) == 0 )
        {
            int value = UNSAFE.getInt( address );
            return BIG_ENDIAN ? value : Integer.reverseBytes( value );
        }
        return BIG_ENDIAN ? intB( address ) : intL( address );
    }

    static void putInt( long address, int value )
    {
        if ( UNALIGNED || (address & 0x01L) == 0 )
        {
            UNSAFE.putInt( address, BIG_ENDIAN ? value : Integer.reverseBytes( value ) );
        }
        else if ( BIG_ENDIAN )
        {
            intB( address, value );
        }
        else
        {
            intL( address, value );
        }
    }

    static long getLong( long address )
    {
        if ( UNALIGNED || (address & 0x07L) == 0 )
        {
            long value = UNSAFE.getLong( address );
            return BIG_ENDIAN ? value : Long.reverseBytes( value );
        }
        return BIG_ENDIAN ? longB( address ) : longL( address );
    }

    static void putLong( long address, long value )
    {
        if ( UNALIGNED || (address & 0x01L) == 0 )
        {
            UNSAFE.putLong( address, BIG_ENDIAN ? value : Long.reverseBytes( value ) );
        }
        else if ( BIG_ENDIAN )
        {
            longB( address, value );
        }
        else
        {
            longL( address, value );
        }
    }

    static char getChar( long address )
    {
        if ( UNALIGNED || (address & 0x01L) == 0 )
        {
            char value = UNSAFE.getChar( address );
            return BIG_ENDIAN ? value : Character.reverseBytes( value );
        }
        return BIG_ENDIAN ? charB( address ) : charL( address );
    }

    static void putChar( long address, char value )
    {
        if ( UNALIGNED || (address & 0x01L) == 0 )
        {
            UNSAFE.putChar( address, BIG_ENDIAN ? value : Character.reverseBytes( value ) );
        }
        else if ( BIG_ENDIAN )
        {
            charB( address, value );
        }
        else
        {
            charL( address, value );
        }
    }

    static void copyToArray( long address, byte[] target, int offset, int length )
    {
        if ( NATIVE_ARRAY )
        {
            UNSAFE.copyMemory( null, address, target, BYTE_ARRAY + offset, length );
        }
        else
        {
            for ( int i = 0; i < length; i++, address += 1 )
            {
                target[offset + i] = getByte( address );
            }
        }
    }

    static void copyFromArray( long address, byte[] source, int offset, int length )
    {
        if ( NATIVE_ARRAY )
        {
            UNSAFE.copyMemory( source, BYTE_ARRAY + offset, null, address, length );
        }
        else
        {
            for ( int i = 0; i < length; i++, address += 1 )
            {
                putByte( address, source[offset + i] );
            }
        }
    }

    static void copyToArray( long address, short[] target, int offset, int length )
    {
        if ( NATIVE_ARRAY && BIG_ENDIAN )
        {
            UNSAFE.copyMemory( null, address, target, SHORT_ARRAY + offset * 2L, length * 2L );
        }
        else
        {
            for ( int i = 0; i < length; i++, address += 2 )
            {
                target[offset + i] = getShort( address );
            }
        }
    }

    static void copyFromArray( long address, short[] source, int offset, int length )
    {
        if ( NATIVE_ARRAY && BIG_ENDIAN )
        {
            UNSAFE.copyMemory( source, SHORT_ARRAY + offset * 2L, null, address, length * 2L );
        }
        else
        {
            for ( int i = 0; i < length; i++, address += 2 )
            {
                putShort( address, source[offset + i] );
            }
        }
    }

    static void copyToArray( long address, int[] target, int offset, int length )
    {
        if ( NATIVE_ARRAY && BIG_ENDIAN )
        {
            UNSAFE.copyMemory( null, address, target, INT_ARRAY + offset * 4L, length * 4L );
        }
        else
        {
            for ( int i = 0; i < length; i++, address += 4 )
            {
                target[offset + i] = getInt( address );
            }
        }
    }

    static void copyFromArray( long address, int[] source, int offset, int length )
    {
        if ( NATIVE_ARRAY && BIG_ENDIAN )
        {
            UNSAFE.copyMemory( source, INT_ARRAY + offset * 4L, null, address, length * 4L );
        }
        else
        {
            for ( int i = 0; i < length; i++, address += 4 )
            {
                putInt( address, source[offset + i] );
            }
        }
    }

    static void copyToArray( long address, long[] target, int offset, int length )
    {
        if ( NATIVE_ARRAY && BIG_ENDIAN )
        {
            UNSAFE.copyMemory( null, address, target, LONG_ARRAY + offset * 8L, length * 8L );
        }
        else
        {
            for ( int i = 0; i < length; i++, address += 8 )
            {
                target[offset + i] = getLong( address );
            }
        }
    }

    static void copyFromArray( long address, long[] source, int offset, int length )
    {
        if ( NATIVE_ARRAY && BIG_ENDIAN )
        {
            UNSAFE.copyMemory( source, LONG_ARRAY + offset * 8L, null, address, length * 8L );
        }
        else
        {
            for ( int i = 0; i < length; i++, address += 8 )
            {
                putLong( address, source[offset + i] );
            }
        }
    }

    static void copyToArray( long address, char[] target, int offset, int length )
    {
        if ( NATIVE_ARRAY && BIG_ENDIAN )
        {
            UNSAFE.copyMemory( null, address, target, CHAR_ARRAY + offset * 2L, length * 2L );
        }
        else
        {
            for ( int i = 0; i < length; i++, address += 2 )
            {
                target[offset + i] = getChar( address );
            }
        }
    }

    static void copyFromArray( long address, char[] source, int offset, int length )
    {
        if ( NATIVE_ARRAY && BIG_ENDIAN )
        {
            UNSAFE.copyMemory( source, CHAR_ARRAY + offset * 2L, null, address, length * 2L );
        }
        else
        {
            for ( int i = 0; i < length; i++, address += 2 )
            {
                putChar( address, source[offset + i] );
            }
        }
    }

    static void copyToArray( long address, float[] target, int offset, int length )
    {
        if ( NATIVE_ARRAY && BIG_ENDIAN )
        {
            UNSAFE.copyMemory( null, address, target, FLOAT_ARRAY + offset * 4L, length * 4L );
        }
        else
        {
            for ( int i = 0; i < length; i++, address += 4 )
            {
                target[offset + i] = intBitsToFloat( getInt( address ) );
            }
        }
    }

    static void copyFromArray( long address, float[] source, int offset, int length )
    {
        if ( NATIVE_ARRAY && BIG_ENDIAN )
        {
            UNSAFE.copyMemory( source, FLOAT_ARRAY + offset * 4L, null, address, length * 4L );
        }
        else
        {
            for ( int i = 0; i < length; i++, address += 4 )
            {
                putInt( address, floatToIntBits( source[offset + i] ) );
            }
        }
    }

    static void copyToArray( long address, double[] target, int offset, int length )
    {
        if ( NATIVE_ARRAY && BIG_ENDIAN )
        {
            UNSAFE.copyMemory( null, address, target, DOUBLE_ARRAY + offset * 8L, length * 8L );
        }
        else
        {
            for ( int i = 0; i < length; i++, address += 8 )
            {
                target[offset + i] = longBitsToDouble( getLong( address ) );
            }
        }
    }

    static void copyFromArray( long address, double[] source, int offset, int length )
    {
        if ( NATIVE_ARRAY && BIG_ENDIAN )
        {
            UNSAFE.copyMemory( source, DOUBLE_ARRAY + offset * 8L, null, address, length * 8L );
        }
        else
        {
            for ( int i = 0; i < length; i++, address += 8 )
            {
                putLong( address, doubleToLongBits( source[offset + i] ) );
            }
        }
    }

    static void fill( long address, int size, byte data )
    {
        UNSAFE.setMemory( address, size, data );
    }

    private static short shortB( long address )
    {
        return makeShort(
                getByte( address ),
                getByte( address + 1 ) );
    }

    private static short shortL( long address )
    {
        return makeShort(
                getByte( address + 1 ),
                getByte( address ) );
    }

    private static void shortB( long address, short value )
    {
        putByte( address, (byte) (value >> 8) );
        putByte( address + 1, (byte) value );
    }

    private static void shortL( long address, short value )
    {
        putByte( address, (byte) value );
        putByte( address + 1, (byte) (value >> 8) );
    }

    private static int intB( long address )
    {
        return makeInt(
                getByte( address ),
                getByte( address + 1 ),
                getByte( address + 2 ),
                getByte( address + 3 ) );
    }

    private static int intL( long address )
    {
        return makeInt(
                getByte( address + 3 ),
                getByte( address + 2 ),
                getByte( address + 1 ),
                getByte( address ) );
    }

    private static void intB( long address, int value )
    {
        putByte( address, (byte) (value >> 24) );
        putByte( address + 1, (byte) (value >> 16) );
        putByte( address + 2, (byte) (value >> 8) );
        putByte( address + 3, (byte) value );
    }

    private static void intL( long address, int value )
    {
        putByte( address, (byte) value );
        putByte( address + 1, (byte) (value >> 8) );
        putByte( address + 2, (byte) (value >> 16) );
        putByte( address + 3, (byte) (value >> 24) );
    }

    private static long longB( long address )
    {
        return makeLong(
                getByte( address ),
                getByte( address + 1 ),
                getByte( address + 2 ),
                getByte( address + 3 ),
                getByte( address + 4 ),
                getByte( address + 5 ),
                getByte( address + 6 ),
                getByte( address + 7 ) );
    }

    private static long longL( long address )
    {
        return makeLong(
                getByte( address + 7 ),
                getByte( address + 6 ),
                getByte( address + 5 ),
                getByte( address + 4 ),
                getByte( address + 3 ),
                getByte( address + 2 ),
                getByte( address + 1 ),
                getByte( address ) );
    }

    private static void longB( long address, long value )
    {
        putByte( address, (byte) (value >> 56) );
        putByte( address + 1, (byte) (value >> 48) );
        putByte( address + 2, (byte) (value >> 40) );
        putByte( address + 3, (byte) (value >> 32) );
        putByte( address + 4, (byte) (value >> 24) );
        putByte( address + 5, (byte) (value >> 16) );
        putByte( address + 6, (byte) (value >> 8) );
        putByte( address + 7, (byte) value );
    }

    private static void longL( long address, long value )
    {
        putByte( address, (byte) value );
        putByte( address + 1, (byte) (value >> 8) );
        putByte( address + 2, (byte) (value >> 16) );
        putByte( address + 3, (byte) (value >> 24) );
        putByte( address + 4, (byte) (value >> 32) );
        putByte( address + 5, (byte) (value >> 40) );
        putByte( address + 6, (byte) (value >> 48) );
        putByte( address + 7, (byte) (value >> 56) );
    }

    private static char charB( long address )
    {
        return makeChar(
                getByte( address ),
                getByte( address + 1 ) );
    }

    private static char charL( long address )
    {
        return makeChar(
                getByte( address + 1 ),
                getByte( address ) );
    }

    private static void charB( long address, char value )
    {
        putByte( address, (byte) (value >> 8) );
        putByte( address + 1, (byte) value );
    }

    private static void charL( long address, char value )
    {
        putByte( address, (byte) value );
        putByte( address + 1, (byte) (value >> 8) );
    }

    private static long makeLong(
            byte b7, byte b6, byte b5, byte b4,
            byte b3, byte b2, byte b1, byte b0 )
    {
        return (((long) b7) << 56) |
                (((long) b6 & 0xff) << 48) |
                (((long) b5 & 0xff) << 40) |
                (((long) b4 & 0xff) << 32) |
                (((long) b3 & 0xff) << 24) |
                (((long) b2 & 0xff) << 16) |
                (((long) b1 & 0xff) << 8) |
                (((long) b0 & 0xff));
    }

    private static int makeInt( byte b3, byte b2, byte b1, byte b0 )
    {
        return (b3 << 24) |
                ((b2 & 0xff) << 16) |
                ((b1 & 0xff) << 8) |
                ((b0 & 0xff));
    }

    private static short makeShort( byte b1, byte b0 )
    {
        return (short) ((b1 << 8) | (b0 & 0xff));
    }

    private static char makeChar( byte b1, byte b0 )
    {
        return (char) ((b1 << 8) | (b0 & 0xff));
    }

    static
    {
        Unsafe instance = null;
        try
        {
            instance = Unsafe.getUnsafe();
        }
        catch ( Throwable initial )
        {
            for ( Field field : Unsafe.class.getDeclaredFields() )
            {
                if ( field.getType() == Unsafe.class && Modifier.isStatic( field.getModifiers() ) )
                {
                    try
                    {
                        field.setAccessible( true );
                        if ( null != (instance = (Unsafe) field.get( null )) )
                        {
                            break;
                        }
                    }
                    catch ( Throwable suppressed )
                    {
                        initial.addSuppressed( suppressed );
                    }
                }
            }
            if ( instance == null )
            {
                throw initial;
            }
        }
        UNSAFE = instance;
        String arch = AccessController.doPrivileged(
                (PrivilegedAction<String>) (() -> System.getProperty( "os.arch" )) );
        UNALIGNED = arch.equals( "i386" ) || arch.equals( "x86" )
                || arch.equals( "amd64" ) || arch.equals( "x86_64" );
        String vm = AccessController.doPrivileged(
                (PrivilegedAction<String>) (() -> System.getProperty( "java.vm.name" )) );
        NATIVE_ARRAY = vm.contains( "HotSpot" ); // there are probably more VMs that use native layout for arrays
        BYTE_ARRAY = UNSAFE.arrayBaseOffset( byte[].class );
        SHORT_ARRAY = UNSAFE.arrayBaseOffset( short[].class );
        INT_ARRAY = UNSAFE.arrayBaseOffset( int[].class );
        LONG_ARRAY = UNSAFE.arrayBaseOffset( long[].class );
        CHAR_ARRAY = UNSAFE.arrayBaseOffset( char[].class );
        FLOAT_ARRAY = UNSAFE.arrayBaseOffset( float[].class );
        DOUBLE_ARRAY = UNSAFE.arrayBaseOffset( double[].class );
    }
}
