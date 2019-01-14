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
package org.neo4j.unsafe.impl.internal.dragons;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.memory.GlobalMemoryTracker;

import static java.lang.System.currentTimeMillis;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.allocateMemory;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.arrayBaseOffset;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.arrayIndexScale;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.arrayOffset;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.assertHasUnsafe;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.compareAndSetMaxLong;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.compareAndSwapLong;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.compareAndSwapObject;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.free;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.getAndAddInt;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.getAndSetLong;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.getAndSetObject;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.getBoolean;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.getBooleanVolatile;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.getByte;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.getByteVolatile;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.getChar;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.getCharVolatile;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.getDouble;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.getDoubleVolatile;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.getFieldOffset;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.getFloat;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.getFloatVolatile;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.getInt;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.getIntVolatile;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.getLong;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.getLongVolatile;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.getObject;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.getObjectVolatile;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.getShort;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.getShortVolatile;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.initDirectByteBuffer;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.newDirectByteBuffer;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.pageSize;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.putBoolean;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.putBooleanVolatile;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.putByte;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.putByteVolatile;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.putChar;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.putCharVolatile;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.putDouble;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.putDoubleVolatile;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.putFloat;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.putFloatVolatile;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.putInt;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.putIntVolatile;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.putLong;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.putLongVolatile;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.putObject;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.putObjectVolatile;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.putShort;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.putShortVolatile;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.setMemory;

public class UnsafeUtilTest
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
    public void mustHaveUnsafe()
    {
        assertHasUnsafe();
    }

    @Test
    public void pageSizeIsPowerOfTwo()
    {
        assertThat( pageSize(), isOneOf(
                1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144,
                524288, 1048576, 2097152, 4194304, 8388608, 16777216, 33554432, 67108864, 134217728, 268435456,
                536870912, 1073741824 ) );
    }

    @Test
    public void mustSupportReadingFromAndWritingToFields()
    {
        Obj obj;

        long aBooleanOffset = getFieldOffset( Obj.class, "aBoolean" );
        obj = new Obj();
        putBoolean( obj, aBooleanOffset, true );
        assertThat( obj.aBoolean, is( true ) );
        assertThat( getBoolean( obj, aBooleanOffset ), is( true ) );
        obj.aBoolean = false;
        assertThat( obj, is( new Obj() ) );
        putBooleanVolatile( obj, aBooleanOffset, true );
        assertThat( obj.aBoolean, is( true ) );
        assertThat( getBooleanVolatile( obj, aBooleanOffset ), is( true ) );
        obj.aBoolean = false;
        assertThat( obj, is( new Obj() ) );

        long aByteOffset = getFieldOffset( Obj.class, "aByte" );
        obj = new Obj();
        putByte( obj, aByteOffset, (byte) 1 );
        assertThat( obj.aByte, is( (byte) 1 ) );
        assertThat( getByte( obj, aByteOffset ), is( (byte) 1 ) );
        obj.aByte = 0;
        assertThat( obj, is( new Obj() ) );
        putByteVolatile( obj, aByteOffset, (byte) 2 );
        assertThat( obj.aByte, is( (byte) 2 ) );
        assertThat( getByteVolatile( obj, aByteOffset ), is( (byte) 2 ) );
        obj.aByte = 0;
        assertThat( obj, is( new Obj() ) );

        long aShortOffset = getFieldOffset( Obj.class, "aShort" );
        obj = new Obj();
        putShort( obj, aShortOffset, (byte) 1 );
        assertThat( obj.aShort, is( (short) 1 ) );
        assertThat( getShort( obj, aShortOffset ), is( (short) 1 ) );
        obj.aShort = 0;
        assertThat( obj, is( new Obj() ) );
        putShortVolatile( obj, aShortOffset, (short) 2 );
        assertThat( obj.aShort, is( (short) 2 ) );
        assertThat( getShortVolatile( obj, aShortOffset ), is( (short) 2 ) );
        obj.aShort = 0;
        assertThat( obj, is( new Obj() ) );

        long aFloatOffset = getFieldOffset( Obj.class, "aFloat" );
        obj = new Obj();
        putFloat( obj, aFloatOffset, 1 );
        assertThat( obj.aFloat, is( (float) 1 ) );
        assertThat( getFloat( obj, aFloatOffset ), is( (float) 1 ) );
        obj.aFloat = 0;
        assertThat( obj, is( new Obj() ) );
        putFloatVolatile( obj, aFloatOffset, 2 );
        assertThat( obj.aFloat, is( (float) 2 ) );
        assertThat( getFloatVolatile( obj, aFloatOffset ), is( (float) 2 ) );
        obj.aFloat = 0;
        assertThat( obj, is( new Obj() ) );

        long aCharOffset = getFieldOffset( Obj.class, "aChar" );
        obj = new Obj();
        putChar( obj, aCharOffset, '1' );
        assertThat( obj.aChar, is( '1' ) );
        assertThat( getChar( obj, aCharOffset ), is( '1' ) );
        obj.aChar = 0;
        assertThat( obj, is( new Obj() ) );
        putCharVolatile( obj, aCharOffset, '2' );
        assertThat( obj.aChar, is( '2' ) );
        assertThat( getCharVolatile( obj, aCharOffset ), is( '2' ) );
        obj.aChar = 0;
        assertThat( obj, is( new Obj() ) );

        long anIntOffset = getFieldOffset( Obj.class, "anInt" );
        obj = new Obj();
        putInt( obj, anIntOffset, 1 );
        assertThat( obj.anInt, is( 1 ) );
        assertThat( getInt( obj, anIntOffset ), is( 1 ) );
        obj.anInt = 0;
        assertThat( obj, is( new Obj() ) );
        putIntVolatile( obj, anIntOffset, 2 );
        assertThat( obj.anInt, is( 2 ) );
        assertThat( getIntVolatile( obj, anIntOffset ), is( 2 ) );
        obj.anInt = 0;
        assertThat( obj, is( new Obj() ) );

        long aLongOffset = getFieldOffset( Obj.class, "aLong" );
        obj = new Obj();
        putLong( obj, aLongOffset, 1 );
        assertThat( obj.aLong, is( 1L ) );
        assertThat( getLong( obj, aLongOffset ), is( 1L ) );
        obj.aLong = 0;
        assertThat( obj, is( new Obj() ) );
        putLongVolatile( obj, aLongOffset, 2 );
        assertThat( obj.aLong, is( 2L ) );
        assertThat( getLongVolatile( obj, aLongOffset ), is( 2L ) );
        obj.aLong = 0;
        assertThat( obj, is( new Obj() ) );

        long aDoubleOffset = getFieldOffset( Obj.class, "aDouble" );
        obj = new Obj();
        putDouble( obj, aDoubleOffset, 1 );
        assertThat( obj.aDouble, is( (double) 1 ) );
        assertThat( getDouble( obj, aDoubleOffset ), is( (double) 1 ) );
        obj.aDouble = 0;
        assertThat( obj, is( new Obj() ) );
        putDoubleVolatile( obj, aDoubleOffset, 2 );
        assertThat( obj.aDouble, is( (double) 2 ) );
        assertThat( getDoubleVolatile( obj, aDoubleOffset ), is( (double) 2 ) );
        obj.aDouble = 0;
        assertThat( obj, is( new Obj() ) );

        long objectOffset = getFieldOffset( Obj.class, "object" );
        obj = new Obj();
        Object a = new Object();
        Object b = new Object();
        putObject( obj, objectOffset, a );
        assertThat( obj.object, is( a ) );
        assertThat( getObject( obj, objectOffset ), is( a ) );
        obj.object = null;
        assertThat( obj, is( new Obj() ) );
        putObjectVolatile( obj, objectOffset, b );
        assertThat( obj.object, is( b ) );
        assertThat( getObjectVolatile( obj, objectOffset ), is( b ) );
        obj.object = null;
        assertThat( obj, is( new Obj() ) );
    }

    @Test
    public void mustSupportReadingAndWritingOfPrimitivesToMemory()
    {
        int sizeInBytes = 8;
        long address = allocateMemory( sizeInBytes );
        try
        {
            putByte( address, (byte) 1 );
            assertThat( getByte( address ), is( (byte) 1 ) );
            setMemory( address, sizeInBytes, (byte) 0 );
            assertThat( getByte( address ), is( (byte) 0 ) );

            putByteVolatile( address, (byte) 1 );
            assertThat( getByteVolatile( address ), is( (byte) 1 ) );
            setMemory( address, sizeInBytes, (byte) 0 );
            assertThat( getByteVolatile( address ), is( (byte) 0 ) );

            putShort( address, (short) 1 );
            assertThat( getShort( address ), is( (short) 1 ) );
            setMemory( address, sizeInBytes, (byte) 0 );
            assertThat( getShort( address ), is( (short) 0 ) );

            putShortVolatile( address, (short) 1 );
            assertThat( getShortVolatile( address ), is( (short) 1 ) );
            setMemory( address, sizeInBytes, (byte) 0 );
            assertThat( getShortVolatile( address ), is( (short) 0 ) );

            putFloat( address, 1 );
            assertThat( getFloat( address ), is( (float) 1 ) );
            setMemory( address, sizeInBytes, (byte) 0 );
            assertThat( getFloat( address ), is( (float) 0 ) );

            putFloatVolatile( address, 1 );
            assertThat( getFloatVolatile( address ), is( (float) 1 ) );
            setMemory( address, sizeInBytes, (byte) 0 );
            assertThat( getFloatVolatile( address ), is( (float) 0 ) );

            putChar( address, '1' );
            assertThat( getChar( address ), is( '1' ) );
            setMemory( address, sizeInBytes, (byte) 0 );
            assertThat( getChar( address ), is( (char) 0 ) );

            putCharVolatile( address, '1' );
            assertThat( getCharVolatile( address ), is( '1' ) );
            setMemory( address, sizeInBytes, (byte) 0 );
            assertThat( getCharVolatile( address ), is( (char) 0 ) );

            putInt( address, 1 );
            assertThat( getInt( address ), is( 1 ) );
            setMemory( address, sizeInBytes, (byte) 0 );
            assertThat( getInt( address ), is( 0 ) );

            putIntVolatile( address, 1 );
            assertThat( getIntVolatile( address ), is( 1 ) );
            setMemory( address, sizeInBytes, (byte) 0 );
            assertThat( getIntVolatile( address ), is( 0 ) );

            putLong( address, 1 );
            assertThat( getLong( address ), is( 1L ) );
            setMemory( address, sizeInBytes, (byte) 0 );
            assertThat( getLong( address ), is( 0L ) );

            putLongVolatile( address, 1 );
            assertThat( getLongVolatile( address ), is( 1L ) );
            setMemory( address, sizeInBytes, (byte) 0 );
            assertThat( getLongVolatile( address ), is( 0L ) );

            putDouble( address, 1 );
            assertThat( getDouble( address ), is( (double) 1 ) );
            setMemory( address, sizeInBytes, (byte) 0 );
            assertThat( getDouble( address ), is( (double) 0 ) );

            putDoubleVolatile( address, 1 );
            assertThat( getDoubleVolatile( address ), is( (double) 1 ) );
            setMemory( address, sizeInBytes, (byte) 0 );
            assertThat( getDoubleVolatile( address ), is( (double) 0 ) );
        }
        finally
        {
            free( address, sizeInBytes, GlobalMemoryTracker.INSTANCE );
        }
    }

    @Test
    public void getAndAddIntOfField()
    {
        Obj obj = new Obj();
        long anIntOffset = getFieldOffset( Obj.class, "anInt" );
        assertThat( getAndAddInt( obj, anIntOffset, 3 ), is( 0 ) );
        assertThat( getAndAddInt( obj, anIntOffset, 2 ), is( 3 ) );
        assertThat( obj.anInt, is( 5 ) );
        obj.anInt = 0;
        assertThat( obj, is( new Obj() ) );
    }

    @Test
    public void compareAndSwapLongField()
    {
        Obj obj = new Obj();
        long aLongOffset = getFieldOffset( Obj.class, "aLong" );
        assertTrue( compareAndSwapLong( obj, aLongOffset, 0, 5 ) );
        assertFalse( compareAndSwapLong( obj, aLongOffset, 0, 5 ) );
        assertTrue( compareAndSwapLong( obj, aLongOffset, 5, 0 ) );
        assertThat( obj, is( new Obj() ) );
    }

    @Test
    public void compareAndSwapObjectField()
    {
        Obj obj = new Obj();
        long objectOffset = getFieldOffset( Obj.class, "object" );
        assertTrue( compareAndSwapObject( obj, objectOffset, null, obj ) );
        assertFalse( compareAndSwapObject( obj, objectOffset, null, obj ) );
        assertTrue( compareAndSwapObject( obj, objectOffset, obj, null ) );
        assertThat( obj, is( new Obj() ) );
    }

    @Test
    public void getAndSetObjectField()
    {
        Obj obj = new Obj();
        long objectOffset = getFieldOffset( Obj.class, "object" );
        assertThat( getAndSetObject( obj, objectOffset, obj ), is( nullValue() ) );
        assertThat( getAndSetObject( obj, objectOffset, null ), sameInstance( obj ) );
        assertThat( obj, is( new Obj() ) );
    }

    @Test
    public void getAndSetLongField()
    {
        Obj obj = new Obj();
        long offset = getFieldOffset( Obj.class, "aLong" );
        assertThat( getAndSetLong( obj, offset, 42L ), equalTo( 0L ) );
        assertThat( getAndSetLong( obj, offset, -1 ), equalTo( 42L ) );
    }

    @Test
    public void compareAndSetMaxLongField()
    {
        Obj obj = new Obj();
        long offset = getFieldOffset( Obj.class, "aLong" );
        assertThat( getAndSetLong( obj, offset, 42L ), equalTo( 0L ) );

        compareAndSetMaxLong( obj, offset, 5 );
        assertEquals( 42, getLong( obj, offset ) );

        compareAndSetMaxLong( obj, offset, 105 );
        assertEquals( 105, getLong( obj, offset ) );
    }

    @Test
    public void unsafeArrayElementAccess() throws Exception
    {
        int len = 3;
        int scale;
        int base;

        boolean[] booleans = new boolean[len];
        scale = arrayIndexScale( booleans.getClass() );
        base = arrayBaseOffset( booleans.getClass() );
        putBoolean( booleans, arrayOffset( 1, base, scale ), true );
        assertThat( booleans[0], is( false ) );
        assertThat( booleans[1], is( true ) );
        assertThat( booleans[2], is( false ) );

        byte[] bytes = new byte[len];
        scale = arrayIndexScale( bytes.getClass() );
        base = arrayBaseOffset( bytes.getClass() );
        putByte( bytes, arrayOffset( 1, base, scale ), (byte) -1 );
        assertThat( bytes[0], is( (byte) 0 ) );
        assertThat( bytes[1], is( (byte) -1 ) );
        assertThat( bytes[2], is( (byte) 0 ) );

        short[] shorts = new short[len];
        scale = arrayIndexScale( shorts.getClass() );
        base = arrayBaseOffset( shorts.getClass() );
        putShort( shorts, arrayOffset( 1, base, scale ), (short) -1 );
        assertThat( shorts[0], is( (short) 0 ) );
        assertThat( shorts[1], is( (short) -1 ) );
        assertThat( shorts[2], is( (short) 0 ) );

        float[] floats = new float[len];
        scale = arrayIndexScale( floats.getClass() );
        base = arrayBaseOffset( floats.getClass() );
        putFloat( floats, arrayOffset( 1, base, scale ), -1 );
        assertThat( floats[0], is( (float) 0 ) );
        assertThat( floats[1], is( (float) -1 ) );
        assertThat( floats[2], is( (float) 0 ) );

        char[] chars = new char[len];
        scale = arrayIndexScale( chars.getClass() );
        base = arrayBaseOffset( chars.getClass() );
        putChar( chars, arrayOffset( 1, base, scale ), (char) -1 );
        assertThat( chars[0], is( (char) 0 ) );
        assertThat( chars[1], is( (char) -1 ) );
        assertThat( chars[2], is( (char) 0 ) );

        int[] ints = new int[len];
        scale = arrayIndexScale( ints.getClass() );
        base = arrayBaseOffset( ints.getClass() );
        putInt( ints, arrayOffset( 1, base, scale ), -1 );
        assertThat( ints[0], is( 0 ) );
        assertThat( ints[1], is( -1 ) );
        assertThat( ints[2], is( 0 ) );

        long[] longs = new long[len];
        scale = arrayIndexScale( longs.getClass() );
        base = arrayBaseOffset( longs.getClass() );
        putLong( longs, arrayOffset( 1, base, scale ), -1 );
        assertThat( longs[0], is( 0L ) );
        assertThat( longs[1], is( -1L ) );
        assertThat( longs[2], is( 0L ) );

        double[] doubles = new double[len];
        scale = arrayIndexScale( doubles.getClass() );
        base = arrayBaseOffset( doubles.getClass() );
        putDouble( doubles, arrayOffset( 1, base, scale ), -1 );
        assertThat( doubles[0], is( (double) 0 ) );
        assertThat( doubles[1], is( (double) -1 ) );
        assertThat( doubles[2], is( (double) 0 ) );

        Object[] objects = new Object[len];
        scale = arrayIndexScale( objects.getClass() );
        base = arrayBaseOffset( objects.getClass() );
        putObject( objects, arrayOffset( 1, base, scale ), objects );
        assertThat( objects[0], is( nullValue() ) );
        assertThat( objects[1], is( sameInstance( objects ) ) );
        assertThat( objects[2], is( nullValue() ) );
    }

    @Test
    public void directByteBufferCreationAndInitialisation() throws Exception
    {
        int sizeInBytes = 313;
        long address = allocateMemory( sizeInBytes );
        try
        {
            setMemory( address, sizeInBytes, (byte) 0 );
            ByteBuffer a = newDirectByteBuffer( address, sizeInBytes );
            assertThat( a, is( not( sameInstance( newDirectByteBuffer( address, sizeInBytes ) ) ) ) );
            assertThat( a.hasArray(), is( false ) );
            assertThat( a.isDirect(), is( true ) );
            assertThat( a.capacity(), is( sizeInBytes ) );
            assertThat( a.limit(), is( sizeInBytes ) );
            assertThat( a.position(), is( 0 ) );
            assertThat( a.remaining(), is( sizeInBytes ) );
            assertThat( getByte( address ), is( (byte) 0 ) );
            a.put( (byte) -1 );
            assertThat( getByte( address ), is( (byte) -1 ) );

            a.position( 101 );
            a.mark();
            a.limit( 202 );

            int sizeInBytes2 = 424;
            long address2 = allocateMemory( sizeInBytes2 );
            try
            {
                setMemory( address2, sizeInBytes2, (byte) 0 );
                initDirectByteBuffer( a, address2, sizeInBytes2 );
                assertThat( a.hasArray(), is( false ) );
                assertThat( a.isDirect(), is( true ) );
                assertThat( a.capacity(), is( sizeInBytes2 ) );
                assertThat( a.limit(), is( sizeInBytes2 ) );
                assertThat( a.position(), is( 0 ) );
                assertThat( a.remaining(), is( sizeInBytes2 ) );
                assertThat( getByte( address2 ), is( (byte) 0 ) );
                a.put( (byte) -1 );
                assertThat( getByte( address2 ), is( (byte) -1 ) );
            }
            finally
            {
                free( address2, sizeInBytes2, GlobalMemoryTracker.INSTANCE );
            }
        }
        finally
        {
            free( address, sizeInBytes, GlobalMemoryTracker.INSTANCE );
        }
    }

    @Test
    public void getAddressOfDirectByteBuffer() throws Exception
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
            assertThat( actual, isOneOf( expected, Long.reverseBytes( expected ) ) );
        }
        finally
        {
            UnsafeUtil.exchangeNativeAccessCheckEnabled( nativeAccessCheckEnabled );
        }
    }

    @Test
    public void shouldAlignMemoryTo4ByteBoundary()
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
    public void shouldPutAndGetByteWiseLittleEndianShort()
    {
        // GIVEN
        int sizeInBytes = 2;
        GlobalMemoryTracker tracker = GlobalMemoryTracker.INSTANCE;
        long p = allocateMemory( sizeInBytes, tracker );
        short value = (short) 0b11001100_10101010;

        // WHEN
        UnsafeUtil.putShortByteWiseLittleEndian( p, value );
        short readValue = UnsafeUtil.getShortByteWiseLittleEndian( p );

        // THEN
        free( p, sizeInBytes, tracker );
        assertEquals( value, readValue );
    }

    @Test
    public void shouldPutAndGetByteWiseLittleEndianInt()
    {
        // GIVEN
        int sizeInBytes = 4;
        GlobalMemoryTracker tracker = GlobalMemoryTracker.INSTANCE;
        long p = allocateMemory( sizeInBytes, tracker );
        int value = 0b11001100_10101010_10011001_01100110;

        // WHEN
        UnsafeUtil.putIntByteWiseLittleEndian( p, value );
        int readValue = UnsafeUtil.getIntByteWiseLittleEndian( p );

        // THEN
        free( p, sizeInBytes, tracker );
        assertEquals( value, readValue );
    }

    @Test
    public void shouldPutAndGetByteWiseLittleEndianLong()
    {
        // GIVEN
        int sizeInBytes = 8;
        GlobalMemoryTracker tracker = GlobalMemoryTracker.INSTANCE;
        long p = allocateMemory( sizeInBytes, tracker );
        long value = 0b11001100_10101010_10011001_01100110__10001000_01000100_00100010_00010001L;

        // WHEN
        UnsafeUtil.putLongByteWiseLittleEndian( p, value );
        long readValue = UnsafeUtil.getLongByteWiseLittleEndian( p );

        // THEN
        free( p, sizeInBytes, tracker );
        assertEquals( value, readValue );
    }
}
