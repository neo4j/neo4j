/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.unsafe.impl.internal.dragons;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.*;

public class UnsafeUtilTest
{
    class Obj
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
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }

            Obj obj = (Obj) o;

            return aByte == obj.aByte && aShort == obj.aShort && Float.compare( obj.aFloat, aFloat ) == 0 &&
                   aChar == obj.aChar && anInt == obj.anInt && aLong == obj.aLong &&
                   Double.compare( obj.aDouble, aDouble ) == 0 &&
                   !(object != null ? !object.equals( obj.object ) : obj.object != null);
        }
    }

    @Test
    public void mustHaveUnsafe() throws Exception
    {
        assertHasUnsafe();
    }

    @Test
    public void pageSizeIsPowerOfTwo() throws Exception
    {
        assertThat( pageSize(), isOneOf(
                1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144,
                524288, 1048576, 2097152, 4194304, 8388608, 16777216, 33554432, 67108864, 134217728, 268435456,
                536870912, 1073741824 ) );
    }

    @Test
    public void mustSupportReadingFromAndWritingToFields() throws Exception
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
    public void mustSupportReadingAndWritingOfPrimitivesToMemory() throws Exception
    {
        long address = allocateMemory( 8 );
        try
        {
            putByte( address, (byte) 1 );
            assertThat( getByte( address ), is( (byte) 1 ) );
            setMemory( address, 8, (byte) 0 );
            assertThat( getByte( address ), is( (byte) 0 ) );

            putByteVolatile( address, (byte) 1 );
            assertThat( getByteVolatile( address ), is( (byte) 1 ) );
            setMemory( address, 8, (byte) 0 );
            assertThat( getByteVolatile( address ), is( (byte) 0 ) );

            putShort( address, (short) 1 );
            assertThat( getShort( address ), is( (short) 1 ) );
            setMemory( address, 8, (byte) 0 );
            assertThat( getShort( address ), is( (short) 0 ) );

            putShortVolatile( address, (short) 1 );
            assertThat( getShortVolatile( address ), is( (short) 1 ) );
            setMemory( address, 8, (byte) 0 );
            assertThat( getShortVolatile( address ), is( (short) 0 ) );

            putFloat( address, (float) 1 );
            assertThat( getFloat( address ), is( (float) 1 ) );
            setMemory( address, 8, (byte) 0 );
            assertThat( getFloat( address ), is( (float) 0 ) );

            putFloatVolatile( address, (float) 1 );
            assertThat( getFloatVolatile( address ), is( (float) 1 ) );
            setMemory( address, 8, (byte) 0 );
            assertThat( getFloatVolatile( address ), is( (float) 0 ) );

            putChar( address, '1' );
            assertThat( getChar( address ), is( '1' ) );
            setMemory( address, 8, (byte) 0 );
            assertThat( getChar( address ), is( (char) 0 ) );

            putCharVolatile( address, '1' );
            assertThat( getCharVolatile( address ), is( '1' ) );
            setMemory( address, 8, (byte) 0 );
            assertThat( getCharVolatile( address ), is( (char) 0 ) );

            putInt( address, 1 );
            assertThat( getInt( address ), is( 1 ) );
            setMemory( address, 8, (byte) 0 );
            assertThat( getInt( address ), is( 0 ) );

            putIntVolatile( address, 1 );
            assertThat( getIntVolatile( address ), is( 1 ) );
            setMemory( address, 8, (byte) 0 );
            assertThat( getIntVolatile( address ), is( 0 ) );

            putLong( address, 1 );
            assertThat( getLong( address ), is( 1L ) );
            setMemory( address, 8, (byte) 0 );
            assertThat( getLong( address ), is( 0L ) );

            putLongVolatile( address, 1 );
            assertThat( getLongVolatile( address ), is( 1L ) );
            setMemory( address, 8, (byte) 0 );
            assertThat( getLongVolatile( address ), is( 0L ) );

            putDouble( address, 1 );
            assertThat( getDouble( address ), is( (double) 1 ) );
            setMemory( address, 8, (byte) 0 );
            assertThat( getDouble( address ), is( (double) 0 ) );

            putDoubleVolatile( address, 1 );
            assertThat( getDoubleVolatile( address ), is( (double) 1 ) );
            setMemory( address, 8, (byte) 0 );
            assertThat( getDoubleVolatile( address ), is( (double) 0 ) );
        }
        finally
        {
            free( address );
        }
    }

    @Test
    public void getAndAddIntOfField() throws Exception
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
    public void compareAndSwapLongField() throws Exception
    {
        Obj obj = new Obj();
        long aLongOffset = getFieldOffset( Obj.class, "aLong" );
        assertTrue( compareAndSwapLong( obj, aLongOffset, 0, 5 ) );
        assertFalse( compareAndSwapLong( obj, aLongOffset, 0, 5 ) );
        assertTrue( compareAndSwapLong( obj, aLongOffset, 5, 0 ) );
        assertThat( obj, is( new Obj() ) );
    }

    @Test
    public void compareAndSwapObjectField() throws Exception
    {
        Obj obj = new Obj();
        long objectOffset = getFieldOffset( Obj.class, "object" );
        assertTrue( compareAndSwapObject( obj, objectOffset, null, obj ) );
        assertFalse( compareAndSwapObject( obj, objectOffset, null, obj ) );
        assertTrue( compareAndSwapObject( obj, objectOffset, obj, null ) );
        assertThat( obj, is( new Obj() ) );
    }

    @Test
    public void getAndSetObjectField() throws Exception
    {
        Obj obj = new Obj();
        long objectOffset = getFieldOffset( Obj.class, "object" );
        assertThat( getAndSetObject( obj, objectOffset, obj ), is( nullValue() ) );
        assertThat( getAndSetObject( obj, objectOffset, null ), sameInstance( (Object) obj ) );
        assertThat( obj, is( new Obj() ) );
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
        assertThat( objects[1], is( sameInstance( (Object) objects ) ) );
        assertThat( objects[2], is( nullValue() ) );
    }

    @Test
    public void directByteBufferCreationAndInitialisation() throws Exception
    {
        long address = allocateMemory( 313 );
        try
        {
            setMemory( address, 313, (byte) 0 );
            ByteBuffer a = newDirectByteBuffer( address, 313 );
            assertThat( a, is( not( sameInstance( newDirectByteBuffer( address, 313 ) ) ) ) );
            assertThat( a.hasArray(), is( false ) );
            assertThat( a.isDirect(), is( true ) );
            assertThat( a.capacity(), is( 313 ) );
            assertThat( a.limit(), is( 313 ) );
            assertThat( a.position(), is( 0 ) );
            assertThat( a.remaining(), is( 313 ) );
            assertThat( getByte( address ), is( (byte) 0 ) );
            a.put( (byte) -1 );
            assertThat( getByte( address ), is( (byte) -1 ) );

            a.position( 101 );
            a.mark();
            a.limit( 202 );

            long address2 = allocateMemory( 424 );
            try
            {
                setMemory( address2, 424, (byte) 0 );
                initDirectByteBuffer( a, address2, 424 );
                assertThat( a.hasArray(), is( false ) );
                assertThat( a.isDirect(), is( true ) );
                assertThat( a.capacity(), is( 424 ) );
                assertThat( a.limit(), is( 424 ) );
                assertThat( a.position(), is( 0 ) );
                assertThat( a.remaining(), is( 424 ) );
                assertThat( getByte( address2 ), is( (byte) 0 ) );
                a.put( (byte) -1 );
                assertThat( getByte( address2 ), is( (byte) -1 ) );
            }
            finally
            {
                free( address2 );
            }
        }
        finally
        {
            free( address );
        }
    }
}
