/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.io.pagecache.impl.muninn;

import sun.misc.Unsafe;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public final class UnsafeUtil
{
    private static final Unsafe unsafe;
    private static final Object nullSentinelBase;
    private static final long nullSentinelOffset;
    private static final MethodHandle getAndAddInt;
    private static final MethodHandle getAndSetObject;
    private static Object nullSentinel; // see the retainReference() method
    private static final String allowUnalignedMemoryAccessProperty =
            "org.neo4j.io.pagecache.impl.muninn.UnsafeUtil.allowUnalignedMemoryAccess";

    private static final Class<?> directByteBufferClass;
    private static final Constructor<?> directByteBufferCtor;
    private static final long directByteBufferCapacityOffset;
    private static final long directByteBufferLimitOffset;
    private static final long directByteBufferMarkOffset;
    private static final long directByteBufferAddressOffset;


    public static final boolean allowUnalignedMemoryAccess;
    public static final boolean storeByteOrderIsNative;

    static
    {
        Unsafe theUnsafe = null;
        Object sentinelBase = null;
        long sentinelOffset = 0;
        try
        {
            Field unsafeField = Unsafe.class.getDeclaredField( "theUnsafe" );
            unsafeField.setAccessible( true );
            theUnsafe = (Unsafe) unsafeField.get( null );

            Field field = UnsafeUtil.class.getDeclaredField( "nullSentinel" );
            sentinelBase = theUnsafe.staticFieldBase( field );
            sentinelOffset = theUnsafe.staticFieldOffset( field );
        }
        catch ( NoSuchFieldException | IllegalAccessException e )
        {
            e.printStackTrace();
        }
        unsafe = theUnsafe;
        nullSentinelBase = sentinelBase;
        nullSentinelOffset = sentinelOffset;
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        getAndAddInt = getGetAndAddIntMethodHandle( lookup );
        getAndSetObject = getGetAndSetObjectMethodHandle( lookup );

        Class<?> dbbClass = null;
        Constructor<?> ctor = null;
        long dbbCapacityOffset = 0;
        long dbbLimitOffset = 0;
        long dbbMarkOffset = 0;
        long dbbAddressOffset = 0;
        try
        {
            dbbClass = Class.forName( "java.nio.DirectByteBuffer" );
            Class<?> bufferClass = Class.forName( "java.nio.Buffer" );
            dbbCapacityOffset = unsafe.objectFieldOffset( bufferClass.getDeclaredField( "capacity" ) );
            dbbLimitOffset = unsafe.objectFieldOffset( bufferClass.getDeclaredField( "limit" ) );
            dbbMarkOffset = unsafe.objectFieldOffset( bufferClass.getDeclaredField( "mark" ) );
            dbbAddressOffset = unsafe.objectFieldOffset( bufferClass.getDeclaredField( "address" ) );
        }
        catch ( Throwable e )
        {
            if ( dbbClass == null )
            {
                throw new AssertionError( e );
            }
            try
            {
                ctor = dbbClass.getConstructor( Long.TYPE, Integer.TYPE );
                ctor.setAccessible( true );
            }
            catch ( NoSuchMethodException e1 )
            {
                throw new AssertionError( e1 );
            }
        }
        directByteBufferClass = dbbClass;
        directByteBufferCtor = ctor;
        directByteBufferCapacityOffset = dbbCapacityOffset;
        directByteBufferLimitOffset = dbbLimitOffset;
        directByteBufferMarkOffset = dbbMarkOffset;
        directByteBufferAddressOffset = dbbAddressOffset;

        // See java.nio.Bits.unaligned() and its uses.
        String alignmentProperty = System.getProperty(
                allowUnalignedMemoryAccessProperty );
        if ( alignmentProperty != null &&
                (alignmentProperty.equalsIgnoreCase( "true" )
                        || alignmentProperty.equalsIgnoreCase( "false" )) )
        {
            allowUnalignedMemoryAccess = Boolean.parseBoolean( alignmentProperty );
        }
        else
        {
            String arch = System.getProperty( "os.arch", "?" );
            allowUnalignedMemoryAccess =
                    arch.equals( "x86_64" ) || arch.equals( "i386" )
                    || arch.equals( "x86" ) || arch.equals( "amd64" );
        }
        storeByteOrderIsNative = ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN;
    }

    private static MethodHandle getGetAndAddIntMethodHandle(
            MethodHandles.Lookup lookup )
    {
        // int getAndAddInt(Object o, long offset, int delta)
        MethodType type = MethodType.methodType( Integer.TYPE, Object.class, Long.TYPE, Integer.TYPE );
        try
        {
            return lookup.findVirtual( Unsafe.class, "getAndAddInt", type );
        }
        catch ( Exception e )
        {
            return null;
        }
    }

    private static MethodHandle getGetAndSetObjectMethodHandle(
            MethodHandles.Lookup lookup )
    {
        // Object getAndSetObject(Object o, long offset, Object newValue)
        MethodType type = MethodType.methodType( Object.class, Object.class, Long.TYPE, Object.class );
        try
        {
            return lookup.findVirtual( Unsafe.class, "getAndSetObject", type );
        }
        catch ( Exception e )
        {
            return null;
        }
    }

    public static long getFieldOffset( Class<?> type, String field )
    {
        try
        {
            return unsafe.objectFieldOffset( type.getDeclaredField( field ) );
        }
        catch ( NoSuchFieldException e )
        {
            throw new Error( e );
        }
    }

    public static int getAndAddInt( Object obj, long offset, int delta )
    {
        // The Java 8 specific version:
        if ( getAndAddInt != null )
        {
            try
            {
                return (int) getAndAddInt.invokeExact( unsafe, obj, offset, delta );
            }
            catch ( Throwable throwable )
            {
                throw new AssertionError( "Unexpected intrinsic failure", throwable );
            }
        }

        // The Java 7 version:
        int x;
        do
        {
            x = unsafe.getIntVolatile( obj, offset );
        }
        while ( !unsafe.compareAndSwapInt( obj, offset, x, x + delta ) );
        return x;
    }

    public static boolean compareAndSwapLong(
            Object obj, long offset, long expected, long update )
    {
        return unsafe.compareAndSwapLong( obj, offset, expected, update );
    }

    public static boolean compareAndSwapObject(
            Object obj, long offset, Object expected, Object update )
    {
        return unsafe.compareAndSwapObject( obj, offset, expected, update );
    }

    public static Object getAndSetObject( Object obj, long offset, Object newValue )
    {
        // The Java 8 specific version:
        if ( getAndSetObject != null )
        {
            try
            {
                return getAndSetObject.invokeExact( unsafe, obj, offset, newValue );
            }
            catch ( Throwable throwable )
            {
                throw new AssertionError( "Unexpected intrinsic failure", throwable );
            }
        }

        // The Java 7 version:
        Object current;
        do
        {
            current = unsafe.getObjectVolatile( obj, offset );
        }
        while ( !unsafe.compareAndSwapObject( obj, offset, current, newValue ) );
        return current;
    }

    public static long malloc( long sizeInBytes )
    {
        long pointer = unsafe.allocateMemory( sizeInBytes );
        unsafe.setMemory( pointer, sizeInBytes, (byte) 0 );
        return pointer;
    }

    public static void free( long pointer )
    {
        unsafe.freeMemory( pointer );
    }

    public static byte getByte( long address )
    {
        return unsafe.getByte( address );
    }

    public static void putByte( long address, byte value )
    {
        unsafe.putByte( address, value );
    }

    public static byte getByteVolatile( Object obj, long offset )
    {
        return unsafe.getByteVolatile( obj, offset );
    }

    public static void putByteVolatile( Object obj, long offset, byte value )
    {
        unsafe.putByteVolatile( obj, offset, value );
    }

    public static long getLong( long address )
    {
        return unsafe.getLong( address );
    }

    public static void putLong( long address, long value )
    {
        unsafe.putLong( address, value );
    }

    public static int getInt( long address )
    {
        return unsafe.getInt( address );
    }

    public static void putInt( long address, int value )
    {
        unsafe.putInt( address, value );
    }

    public static short getShort( long address )
    {
        return unsafe.getShort( address );
    }

    public static void putShort( long address, short value )
    {
        unsafe.putShort( address, value );
    }

    public static boolean hasUnsafe()
    {
        return unsafe != null;
    }

    public static void putLong( Object obj, long offset, long value )
    {
        unsafe.putLong( obj, offset, value );
    }

    public static long getLongVolatile( Object obj, long offset )
    {
        return unsafe.getLongVolatile( obj, offset );
    }

    public static int getIntVolatile( Object obj, long offset )
    {
        return unsafe.getIntVolatile( obj, offset );
    }

    public static Object getObjectVolatile( Object obj, long offset )
    {
        return unsafe.getObjectVolatile( obj, offset );
    }

    public static void putObjectVolatile( Object obj, long offset, Object value )
    {
        unsafe.putObjectVolatile( obj, offset, value );
    }

    public static void setMemory( long address, long bytes, byte value )
    {
        unsafe.setMemory( address, bytes, value );
    }

    public static ByteBuffer newDirectByteBuffer(long addr, int cap) throws Exception
    {
        if ( directByteBufferCtor == null )
        {
            // Simulate the JNI NewDirectByteBuffer(void*, long) invocation.
            Object dbb = unsafe.allocateInstance( directByteBufferClass );
            unsafe.putInt( dbb, directByteBufferCapacityOffset, cap );
            unsafe.putInt( dbb, directByteBufferLimitOffset, cap );
            unsafe.putInt( dbb, directByteBufferMarkOffset, -1 );
            unsafe.putLong( dbb, directByteBufferAddressOffset, addr );
            return (ByteBuffer) dbb;
        }
        // Reflection based fallback code.
        return (ByteBuffer) directByteBufferCtor.newInstance( addr, cap );
    }

    /**
     * This method prevents the given object from becoming finalizable until
     * this method has been called.
     * This method will prevent reordering with other reads and writes, and
     * can as such be used to synchronize the finalization of an object, with
     * the access of its fields.
     *
     * See this email thread for more gory details:
     * https://groups.google.com/forum/#!topic/mechanical-sympathy/PbVDvcKmm9g
     */
    public static void retainReference( Object obj )
    {
        Object sentinel = UnsafeUtil.getObjectVolatile( nullSentinelBase, nullSentinelOffset );

        if ( sentinel == obj )
        {
            UnsafeUtil.putObjectVolatile( nullSentinelBase, nullSentinelOffset, obj );
        }
    }
}
