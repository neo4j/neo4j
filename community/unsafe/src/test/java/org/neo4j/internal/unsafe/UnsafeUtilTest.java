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
package org.neo4j.internal.unsafe;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

import static java.lang.System.currentTimeMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.unsafe.UnsafeUtil.allocateMemory;
import static org.neo4j.internal.unsafe.UnsafeUtil.arrayBaseOffset;
import static org.neo4j.internal.unsafe.UnsafeUtil.arrayIndexScale;
import static org.neo4j.internal.unsafe.UnsafeUtil.arrayOffset;
import static org.neo4j.internal.unsafe.UnsafeUtil.assertHasUnsafe;
import static org.neo4j.internal.unsafe.UnsafeUtil.compareAndSetMaxLong;
import static org.neo4j.internal.unsafe.UnsafeUtil.compareAndSwapLong;
import static org.neo4j.internal.unsafe.UnsafeUtil.compareAndSwapObject;
import static org.neo4j.internal.unsafe.UnsafeUtil.free;
import static org.neo4j.internal.unsafe.UnsafeUtil.getAndAddInt;
import static org.neo4j.internal.unsafe.UnsafeUtil.getAndSetLong;
import static org.neo4j.internal.unsafe.UnsafeUtil.getAndSetObject;
import static org.neo4j.internal.unsafe.UnsafeUtil.getBoolean;
import static org.neo4j.internal.unsafe.UnsafeUtil.getBooleanVolatile;
import static org.neo4j.internal.unsafe.UnsafeUtil.getByte;
import static org.neo4j.internal.unsafe.UnsafeUtil.getByteVolatile;
import static org.neo4j.internal.unsafe.UnsafeUtil.getChar;
import static org.neo4j.internal.unsafe.UnsafeUtil.getCharVolatile;
import static org.neo4j.internal.unsafe.UnsafeUtil.getDouble;
import static org.neo4j.internal.unsafe.UnsafeUtil.getDoubleVolatile;
import static org.neo4j.internal.unsafe.UnsafeUtil.getFieldOffset;
import static org.neo4j.internal.unsafe.UnsafeUtil.getFloat;
import static org.neo4j.internal.unsafe.UnsafeUtil.getFloatVolatile;
import static org.neo4j.internal.unsafe.UnsafeUtil.getInt;
import static org.neo4j.internal.unsafe.UnsafeUtil.getIntVolatile;
import static org.neo4j.internal.unsafe.UnsafeUtil.getLong;
import static org.neo4j.internal.unsafe.UnsafeUtil.getLongVolatile;
import static org.neo4j.internal.unsafe.UnsafeUtil.getObject;
import static org.neo4j.internal.unsafe.UnsafeUtil.getObjectVolatile;
import static org.neo4j.internal.unsafe.UnsafeUtil.getShort;
import static org.neo4j.internal.unsafe.UnsafeUtil.getShortVolatile;
import static org.neo4j.internal.unsafe.UnsafeUtil.initDirectByteBuffer;
import static org.neo4j.internal.unsafe.UnsafeUtil.newDirectByteBuffer;
import static org.neo4j.internal.unsafe.UnsafeUtil.pageSize;
import static org.neo4j.internal.unsafe.UnsafeUtil.putBoolean;
import static org.neo4j.internal.unsafe.UnsafeUtil.putBooleanVolatile;
import static org.neo4j.internal.unsafe.UnsafeUtil.putByte;
import static org.neo4j.internal.unsafe.UnsafeUtil.putByteVolatile;
import static org.neo4j.internal.unsafe.UnsafeUtil.putChar;
import static org.neo4j.internal.unsafe.UnsafeUtil.putCharVolatile;
import static org.neo4j.internal.unsafe.UnsafeUtil.putDouble;
import static org.neo4j.internal.unsafe.UnsafeUtil.putDoubleVolatile;
import static org.neo4j.internal.unsafe.UnsafeUtil.putFloat;
import static org.neo4j.internal.unsafe.UnsafeUtil.putFloatVolatile;
import static org.neo4j.internal.unsafe.UnsafeUtil.putInt;
import static org.neo4j.internal.unsafe.UnsafeUtil.putIntVolatile;
import static org.neo4j.internal.unsafe.UnsafeUtil.putLong;
import static org.neo4j.internal.unsafe.UnsafeUtil.putLongVolatile;
import static org.neo4j.internal.unsafe.UnsafeUtil.putObject;
import static org.neo4j.internal.unsafe.UnsafeUtil.putObjectVolatile;
import static org.neo4j.internal.unsafe.UnsafeUtil.putShort;
import static org.neo4j.internal.unsafe.UnsafeUtil.putShortVolatile;
import static org.neo4j.internal.unsafe.UnsafeUtil.setMemory;

class UnsafeUtilTest
{
    static class Obj
    {
        boolean aBoolean;
        byte aByte;
        short aShort;
        float aFloat;
        char aChar;
        int anInt;
        long aLong;
        double aDouble;
        Object object;

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            Obj obj = (Obj) o;
            return aBoolean == obj.aBoolean &&
                   aByte == obj.aByte &&
                   aShort == obj.aShort &&
                   Float.compare( obj.aFloat, aFloat ) == 0 &&
                   aChar == obj.aChar &&
                   anInt == obj.anInt &&
                   aLong == obj.aLong &&
                   Double.compare( obj.aDouble, aDouble ) == 0 &&
                   Objects.equals( object, obj.object );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( aBoolean, aByte, aShort, aFloat, aChar, anInt, aLong, aDouble, object );
        }
    }

    @Test
    void mustHaveUnsafe()
    {
        assertHasUnsafe();
    }

    @Test
    void pageSizeIsPowerOfTwo()
    {
        assertThat( pageSize() ).isIn( 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576,
                2097152, 4194304, 8388608, 16777216, 33554432, 67108864, 134217728, 268435456, 536870912, 1073741824 );
    }

    @Test
    void mustSupportReadingFromAndWritingToFields()
    {
        Obj obj;

        long aBooleanOffset = getFieldOffset( Obj.class, "aBoolean" );
        obj = new Obj();
        putBoolean( obj, aBooleanOffset, true );
        assertThat( obj.aBoolean ).isEqualTo( true );
        assertThat( getBoolean( obj, aBooleanOffset ) ).isEqualTo( true );
        obj.aBoolean = false;
        assertThat( obj ).isEqualTo( new Obj() );
        putBooleanVolatile( obj, aBooleanOffset, true );
        assertThat( obj.aBoolean ).isEqualTo( true );
        assertThat( getBooleanVolatile( obj, aBooleanOffset ) ).isEqualTo( true );
        obj.aBoolean = false;
        assertThat( obj ).isEqualTo( new Obj() );

        long aByteOffset = getFieldOffset( Obj.class, "aByte" );
        obj = new Obj();
        putByte( obj, aByteOffset, (byte) 1 );
        assertThat( obj.aByte ).isEqualTo( (byte) 1 );
        assertThat( getByte( obj, aByteOffset ) ).isEqualTo( (byte) 1 );
        obj.aByte = 0;
        assertThat( obj ).isEqualTo( new Obj() );
        putByteVolatile( obj, aByteOffset, (byte) 2 );
        assertThat( obj.aByte ).isEqualTo( (byte) 2 );
        assertThat( getByteVolatile( obj, aByteOffset ) ).isEqualTo( (byte) 2 );
        obj.aByte = 0;
        assertThat( obj ).isEqualTo( new Obj() );

        long aShortOffset = getFieldOffset( Obj.class, "aShort" );
        obj = new Obj();
        putShort( obj, aShortOffset, (byte) 1 );
        assertThat( obj.aShort ).isEqualTo( (short) 1 );
        assertThat( getShort( obj, aShortOffset ) ).isEqualTo( (short) 1 );
        obj.aShort = 0;
        assertThat( obj ).isEqualTo( new Obj() );
        putShortVolatile( obj, aShortOffset, (short) 2 );
        assertThat( obj.aShort ).isEqualTo( (short) 2 );
        assertThat( getShortVolatile( obj, aShortOffset ) ).isEqualTo( (short) 2 );
        obj.aShort = 0;
        assertThat( obj ).isEqualTo( new Obj() );

        long aFloatOffset = getFieldOffset( Obj.class, "aFloat" );
        obj = new Obj();
        putFloat( obj, aFloatOffset, 1 );
        assertThat( obj.aFloat ).isEqualTo( (float) 1 );
        assertThat( getFloat( obj, aFloatOffset ) ).isEqualTo( (float) 1 );
        obj.aFloat = 0;
        assertThat( obj ).isEqualTo( new Obj() );
        putFloatVolatile( obj, aFloatOffset, 2 );
        assertThat( obj.aFloat ).isEqualTo( (float) 2 );
        assertThat( getFloatVolatile( obj, aFloatOffset ) ).isEqualTo( (float) 2 );
        obj.aFloat = 0;
        assertThat( obj ).isEqualTo( new Obj() );

        long aCharOffset = getFieldOffset( Obj.class, "aChar" );
        obj = new Obj();
        putChar( obj, aCharOffset, '1' );
        assertThat( obj.aChar ).isEqualTo( '1' );
        assertThat( getChar( obj, aCharOffset ) ).isEqualTo( '1' );
        obj.aChar = 0;
        assertThat( obj ).isEqualTo( new Obj() );
        putCharVolatile( obj, aCharOffset, '2' );
        assertThat( obj.aChar ).isEqualTo( '2' );
        assertThat( getCharVolatile( obj, aCharOffset ) ).isEqualTo( '2' );
        obj.aChar = 0;
        assertThat( obj ).isEqualTo( new Obj() );

        long anIntOffset = getFieldOffset( Obj.class, "anInt" );
        obj = new Obj();
        putInt( obj, anIntOffset, 1 );
        assertThat( obj.anInt ).isEqualTo( 1 );
        assertThat( getInt( obj, anIntOffset ) ).isEqualTo( 1 );
        obj.anInt = 0;
        assertThat( obj ).isEqualTo( new Obj() );
        putIntVolatile( obj, anIntOffset, 2 );
        assertThat( obj.anInt ).isEqualTo( 2 );
        assertThat( getIntVolatile( obj, anIntOffset ) ).isEqualTo( 2 );
        obj.anInt = 0;
        assertThat( obj ).isEqualTo( new Obj() );

        long aLongOffset = getFieldOffset( Obj.class, "aLong" );
        obj = new Obj();
        putLong( obj, aLongOffset, 1 );
        assertThat( obj.aLong ).isEqualTo( 1L );
        assertThat( getLong( obj, aLongOffset ) ).isEqualTo( 1L );
        obj.aLong = 0;
        assertThat( obj ).isEqualTo( new Obj() );
        putLongVolatile( obj, aLongOffset, 2 );
        assertThat( obj.aLong ).isEqualTo( 2L );
        assertThat( getLongVolatile( obj, aLongOffset ) ).isEqualTo( 2L );
        obj.aLong = 0;
        assertThat( obj ).isEqualTo( new Obj() );

        long aDoubleOffset = getFieldOffset( Obj.class, "aDouble" );
        obj = new Obj();
        putDouble( obj, aDoubleOffset, 1 );
        assertThat( obj.aDouble ).isEqualTo( (double) 1 );
        assertThat( getDouble( obj, aDoubleOffset ) ).isEqualTo( (double) 1 );
        obj.aDouble = 0;
        assertThat( obj ).isEqualTo( new Obj() );
        putDoubleVolatile( obj, aDoubleOffset, 2 );
        assertThat( obj.aDouble ).isEqualTo( (double) 2 );
        assertThat( getDoubleVolatile( obj, aDoubleOffset ) ).isEqualTo( (double) 2 );
        obj.aDouble = 0;
        assertThat( obj ).isEqualTo( new Obj() );

        long objectOffset = getFieldOffset( Obj.class, "object" );
        obj = new Obj();
        Object a = new Object();
        Object b = new Object();
        putObject( obj, objectOffset, a );
        assertThat( obj.object ).isEqualTo( a );
        assertThat( getObject( obj, objectOffset ) ).isEqualTo( a );
        obj.object = null;
        assertThat( obj ).isEqualTo( new Obj() );
        putObjectVolatile( obj, objectOffset, b );
        assertThat( obj.object ).isEqualTo( b );
        assertThat( getObjectVolatile( obj, objectOffset ) ).isEqualTo( b );
        obj.object = null;
        assertThat( obj ).isEqualTo( new Obj() );
    }

    @Test
    void mustSupportReadingAndWritingOfPrimitivesToMemory()
    {
        int sizeInBytes = 8;
        long address = allocateMemory( sizeInBytes );
        try
        {
            putByte( address, (byte) 1 );
            assertThat( getByte( address ) ).isEqualTo( (byte) 1 );
            setMemory( address, sizeInBytes, (byte) 0 );
            assertThat( getByte( address ) ).isEqualTo( (byte) 0 );

            putByteVolatile( address, (byte) 1 );
            assertThat( getByteVolatile( address ) ).isEqualTo( (byte) 1 );
            setMemory( address, sizeInBytes, (byte) 0 );
            assertThat( getByteVolatile( address ) ).isEqualTo( (byte) 0 );

            putShort( address, (short) 1 );
            assertThat( getShort( address ) ).isEqualTo( (short) 1 );
            setMemory( address, sizeInBytes, (byte) 0 );
            assertThat( getShort( address ) ).isEqualTo( (short) 0 );

            putShortVolatile( address, (short) 1 );
            assertThat( getShortVolatile( address ) ).isEqualTo( (short) 1 );
            setMemory( address, sizeInBytes, (byte) 0 );
            assertThat( getShortVolatile( address ) ).isEqualTo( (short) 0 );

            putFloat( address, 1 );
            assertThat( getFloat( address ) ).isEqualTo( (float) 1 );
            setMemory( address, sizeInBytes, (byte) 0 );
            assertThat( getFloat( address ) ).isEqualTo( (float) 0 );

            putFloatVolatile( address, 1 );
            assertThat( getFloatVolatile( address ) ).isEqualTo( (float) 1 );
            setMemory( address, sizeInBytes, (byte) 0 );
            assertThat( getFloatVolatile( address ) ).isEqualTo( (float) 0 );

            putChar( address, '1' );
            assertThat( getChar( address ) ).isEqualTo( '1' );
            setMemory( address, sizeInBytes, (byte) 0 );
            assertThat( getChar( address ) ).isEqualTo( (char) 0 );

            putCharVolatile( address, '1' );
            assertThat( getCharVolatile( address ) ).isEqualTo( '1' );
            setMemory( address, sizeInBytes, (byte) 0 );
            assertThat( getCharVolatile( address ) ).isEqualTo( (char) 0 );

            putInt( address, 1 );
            assertThat( getInt( address ) ).isEqualTo( 1 );
            setMemory( address, sizeInBytes, (byte) 0 );
            assertThat( getInt( address ) ).isEqualTo( 0 );

            putIntVolatile( address, 1 );
            assertThat( getIntVolatile( address ) ).isEqualTo( 1 );
            setMemory( address, sizeInBytes, (byte) 0 );
            assertThat( getIntVolatile( address ) ).isEqualTo( 0 );

            putLong( address, 1 );
            assertThat( getLong( address ) ).isEqualTo( 1L );
            setMemory( address, sizeInBytes, (byte) 0 );
            assertThat( getLong( address ) ).isEqualTo( 0L );

            putLongVolatile( address, 1 );
            assertThat( getLongVolatile( address ) ).isEqualTo( 1L );
            setMemory( address, sizeInBytes, (byte) 0 );
            assertThat( getLongVolatile( address ) ).isEqualTo( 0L );

            putDouble( address, 1 );
            assertThat( getDouble( address ) ).isEqualTo( (double) 1 );
            setMemory( address, sizeInBytes, (byte) 0 );
            assertThat( getDouble( address ) ).isEqualTo( (double) 0 );

            putDoubleVolatile( address, 1 );
            assertThat( getDoubleVolatile( address ) ).isEqualTo( (double) 1 );
            setMemory( address, sizeInBytes, (byte) 0 );
            assertThat( getDoubleVolatile( address ) ).isEqualTo( (double) 0 );
        }
        finally
        {
            free( address, sizeInBytes );
        }
    }

    @Test
    void getAndAddIntOfField()
    {
        Obj obj = new Obj();
        long anIntOffset = getFieldOffset( Obj.class, "anInt" );
        assertThat( getAndAddInt( obj, anIntOffset, 3 ) ).isEqualTo( 0 );
        assertThat( getAndAddInt( obj, anIntOffset, 2 ) ).isEqualTo( 3 );
        assertThat( obj.anInt ).isEqualTo( 5 );
        obj.anInt = 0;
        assertThat( obj ).isEqualTo( new Obj() );
    }

    @Test
    void compareAndSwapLongField()
    {
        Obj obj = new Obj();
        long aLongOffset = getFieldOffset( Obj.class, "aLong" );
        assertTrue( compareAndSwapLong( obj, aLongOffset, 0, 5 ) );
        assertFalse( compareAndSwapLong( obj, aLongOffset, 0, 5 ) );
        assertTrue( compareAndSwapLong( obj, aLongOffset, 5, 0 ) );
        assertThat( obj ).isEqualTo( new Obj() );
    }

    @Test
    void compareAndSwapObjectField()
    {
        Obj obj = new Obj();
        long objectOffset = getFieldOffset( Obj.class, "object" );
        assertTrue( compareAndSwapObject( obj, objectOffset, null, obj ) );
        assertFalse( compareAndSwapObject( obj, objectOffset, null, obj ) );
        assertTrue( compareAndSwapObject( obj, objectOffset, obj, null ) );
        assertThat( obj ).isEqualTo( new Obj() );
    }

    @Test
    void getAndSetObjectField()
    {
        Obj obj = new Obj();
        long objectOffset = getFieldOffset( Obj.class, "object" );
        assertThat( getAndSetObject( obj, objectOffset, obj ) ).isNull();
        assertThat( getAndSetObject( obj, objectOffset, null ) ).isSameAs( obj );
        assertThat( obj ).isEqualTo( new Obj() );
    }

    @Test
    void getAndSetLongField()
    {
        Obj obj = new Obj();
        long offset = getFieldOffset( Obj.class, "aLong" );
        assertThat( getAndSetLong( obj, offset, 42L ) ).isEqualTo( 0L );
        assertThat( getAndSetLong( obj, offset, -1 ) ).isEqualTo( 42L );
    }

    @Test
    void compareAndSetMaxLongField()
    {
        Obj obj = new Obj();
        long offset = getFieldOffset( Obj.class, "aLong" );
        assertThat( getAndSetLong( obj, offset, 42L ) ).isEqualTo( 0L );

        compareAndSetMaxLong( obj, offset, 5 );
        assertEquals( 42, getLong( obj, offset ) );

        compareAndSetMaxLong( obj, offset, 105 );
        assertEquals( 105, getLong( obj, offset ) );
    }

    @Test
    void unsafeArrayElementAccess()
    {
        int len = 3;
        int scale;
        int base;

        boolean[] booleans = new boolean[len];
        scale = arrayIndexScale( booleans.getClass() );
        base = arrayBaseOffset( booleans.getClass() );
        putBoolean( booleans, arrayOffset( 1, base, scale ), true );
        assertThat( booleans[0] ).isEqualTo( false );
        assertThat( booleans[1] ).isEqualTo( true );
        assertThat( booleans[2] ).isEqualTo( false );

        byte[] bytes = new byte[len];
        scale = arrayIndexScale( bytes.getClass() );
        base = arrayBaseOffset( bytes.getClass() );
        putByte( bytes, arrayOffset( 1, base, scale ), (byte) -1 );
        assertThat( bytes[0] ).isEqualTo( (byte) 0 );
        assertThat( bytes[1] ).isEqualTo( (byte) -1 );
        assertThat( bytes[2] ).isEqualTo( (byte) 0 );

        short[] shorts = new short[len];
        scale = arrayIndexScale( shorts.getClass() );
        base = arrayBaseOffset( shorts.getClass() );
        putShort( shorts, arrayOffset( 1, base, scale ), (short) -1 );
        assertThat( shorts[0] ).isEqualTo( (short) 0 );
        assertThat( shorts[1] ).isEqualTo( (short) -1 );
        assertThat( shorts[2] ).isEqualTo( (short) 0 );

        float[] floats = new float[len];
        scale = arrayIndexScale( floats.getClass() );
        base = arrayBaseOffset( floats.getClass() );
        putFloat( floats, arrayOffset( 1, base, scale ), -1 );
        assertThat( floats[0] ).isEqualTo( (float) 0 );
        assertThat( floats[1] ).isEqualTo( (float) -1 );
        assertThat( floats[2] ).isEqualTo( (float) 0 );

        char[] chars = new char[len];
        scale = arrayIndexScale( chars.getClass() );
        base = arrayBaseOffset( chars.getClass() );
        putChar( chars, arrayOffset( 1, base, scale ), (char) -1 );
        assertThat( chars[0] ).isEqualTo( (char) 0 );
        assertThat( chars[1] ).isEqualTo( (char) -1 );
        assertThat( chars[2] ).isEqualTo( (char) 0 );

        int[] ints = new int[len];
        scale = arrayIndexScale( ints.getClass() );
        base = arrayBaseOffset( ints.getClass() );
        putInt( ints, arrayOffset( 1, base, scale ), -1 );
        assertThat( ints[0] ).isEqualTo( 0 );
        assertThat( ints[1] ).isEqualTo( -1 );
        assertThat( ints[2] ).isEqualTo( 0 );

        long[] longs = new long[len];
        scale = arrayIndexScale( longs.getClass() );
        base = arrayBaseOffset( longs.getClass() );
        putLong( longs, arrayOffset( 1, base, scale ), -1 );
        assertThat( longs[0] ).isEqualTo( 0L );
        assertThat( longs[1] ).isEqualTo( -1L );
        assertThat( longs[2] ).isEqualTo( 0L );

        double[] doubles = new double[len];
        scale = arrayIndexScale( doubles.getClass() );
        base = arrayBaseOffset( doubles.getClass() );
        putDouble( doubles, arrayOffset( 1, base, scale ), -1 );
        assertThat( doubles[0] ).isEqualTo( (double) 0 );
        assertThat( doubles[1] ).isEqualTo( (double) -1 );
        assertThat( doubles[2] ).isEqualTo( (double) 0 );

        Object[] objects = new Object[len];
        scale = arrayIndexScale( objects.getClass() );
        base = arrayBaseOffset( objects.getClass() );
        putObject( objects, arrayOffset( 1, base, scale ), objects );
        assertThat( objects[0] ).isNull();
        assertThat( objects[1] ).isSameAs( objects );
        assertThat( objects[2] ).isNull();
    }

    @Test
    void directByteBufferCreationAndInitialisation() throws Exception
    {
        int sizeInBytes = 313;
        long address = allocateMemory( sizeInBytes );
        try
        {
            setMemory( address, sizeInBytes, (byte) 0 );
            ByteBuffer a = newDirectByteBuffer( address, sizeInBytes );
            assertThat( a ).isNotSameAs( newDirectByteBuffer( address, sizeInBytes ) );
            assertThat( a.hasArray() ).isEqualTo( false );
            assertThat( a.isDirect() ).isEqualTo( true );
            assertThat( a.capacity() ).isEqualTo( sizeInBytes );
            assertThat( a.limit() ).isEqualTo( sizeInBytes );
            assertThat( a.position() ).isEqualTo( 0 );
            assertThat( a.remaining() ).isEqualTo( sizeInBytes );
            assertThat( getByte( address ) ).isEqualTo( (byte) 0 );
            a.put( (byte) -1 );
            assertThat( getByte( address ) ).isEqualTo( (byte) -1 );

            a.position( 101 );
            a.mark();
            a.limit( 202 );

            int sizeInBytes2 = 424;
            long address2 = allocateMemory( sizeInBytes2 );
            try
            {
                setMemory( address2, sizeInBytes2, (byte) 0 );
                initDirectByteBuffer( a, address2, sizeInBytes2 );
                assertThat( a.hasArray() ).isEqualTo( false );
                assertThat( a.isDirect() ).isEqualTo( true );
                assertThat( a.capacity() ).isEqualTo( sizeInBytes2 );
                assertThat( a.limit() ).isEqualTo( sizeInBytes2 );
                assertThat( a.position() ).isEqualTo( 0 );
                assertThat( a.remaining() ).isEqualTo( sizeInBytes2 );
                assertThat( getByte( address2 ) ).isEqualTo( (byte) 0 );
                a.put( (byte) -1 );
                assertThat( getByte( address2 ) ).isEqualTo( (byte) -1 );
            }
            finally
            {
                free( address2, sizeInBytes2 );
            }
        }
        finally
        {
            free( address, sizeInBytes );
        }
    }

    @Test
    void getAddressOfDirectByteBuffer()
    {
        ByteBuffer buf = ByteBuffer.allocateDirect( 8 );
        long address = UnsafeUtil.getDirectByteBufferAddress( buf );
        long expected = ThreadLocalRandom.current().nextLong();
        // Disable native access checking, because UnsafeUtil doesn't know about the memory allocation in the
        // ByteBuffer.allocateDirect( … ) call.
        boolean nativeAccessCheckEnabled = UnsafeUtil.exchangeNativeAccessCheckEnabled( false );
        try
        {
            UnsafeUtil.putLong( address, expected );
            long actual = buf.getLong();
            assertThat( actual ).isIn( expected, Long.reverseBytes( expected ) );
        }
        finally
        {
            UnsafeUtil.exchangeNativeAccessCheckEnabled( nativeAccessCheckEnabled );
        }
    }

    @Test
    void shouldAlignMemoryTo4ByteBoundary()
    {
        // GIVEN
        long allocatedMemory = currentTimeMillis();
        int alignBy = 4;

        // WHEN
        for ( int i = 0; i < 10; i++ )
        {
            // THEN
            long alignedMemory = UnsafeUtil.alignedMemory( allocatedMemory, alignBy );
            assertTrue( alignedMemory >= allocatedMemory );
            assertEquals( 0, alignedMemory % Integer.BYTES );
            assertTrue( alignedMemory - allocatedMemory <= 3 );
            allocatedMemory++;
        }
    }

    @Test
    void shouldPutAndGetByteWiseLittleEndianShort()
    {
        // GIVEN
        int sizeInBytes = 2;
        long p = allocateMemory( sizeInBytes );
        short value = (short) 0b11001100_10101010;

        // WHEN
        UnsafeUtil.putShortByteWiseLittleEndian( p, value );
        short readValue = UnsafeUtil.getShortByteWiseLittleEndian( p );

        // THEN
        free( p, sizeInBytes );
        assertEquals( value, readValue );
    }

    @Test
    void shouldPutAndGetByteWiseLittleEndianInt()
    {
        // GIVEN
        int sizeInBytes = 4;
        long p = allocateMemory( sizeInBytes );
        int value = 0b11001100_10101010_10011001_01100110;

        // WHEN
        UnsafeUtil.putIntByteWiseLittleEndian( p, value );
        int readValue = UnsafeUtil.getIntByteWiseLittleEndian( p );

        // THEN
        free( p, sizeInBytes );
        assertEquals( value, readValue );
    }

    @Test
    void shouldPutAndGetByteWiseLittleEndianLong()
    {
        // GIVEN
        int sizeInBytes = 8;
        long p = allocateMemory( sizeInBytes );
        long value = 0b11001100_10101010_10011001_01100110__10001000_01000100_00100010_00010001L;

        // WHEN
        UnsafeUtil.putLongByteWiseLittleEndian( p, value );
        long readValue = UnsafeUtil.getLongByteWiseLittleEndian( p );

        // THEN
        free( p, sizeInBytes );
        assertEquals( value, readValue );
    }

    @Test
    void closeNativeByteBufferWithUnsafe()
    {
        ByteBuffer directBuffer = ByteBuffer.allocateDirect( 1024 );
        assertDoesNotThrow( () -> UnsafeUtil.invokeCleaner( directBuffer ) );
    }
}
