/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import sun.misc.Unsafe;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.neo4j.unsafe.impl.internal.dragons.FeatureToggles.flag;

/**
 * Always check that the Unsafe utilities are available with the {@link UnsafeUtil#assertHasUnsafe} method, before
 * calling any of the other methods.
 * <p>
 * Avoid `import static` for these individual methods. Always qualify method usages with `UnsafeUtil` so use sites
 * show up in code greps.
 */
public final class UnsafeUtil
{
    /**
     * Whether or not to explicitly dirty the allocated memory. This is off by default.
     * The {@link UnsafeUtil#allocateMemory(long)} method is not guaranteed to allocate zeroed out memory, but might
     * often do so by pure chance.
     * Enabling this feature will make sure that the allocated memory is full of random data, such that we can test
     * and verify that our code does not assume that memory is clean when allocated.
     */
    private static final boolean DIRTY_MEMORY = flag( UnsafeUtil.class, "DIRTY_MEMORY", false );
    private static final boolean CHECK_NATIVE_ACCESS = flag( UnsafeUtil.class, "CHECK_NATIVE_ACCESS", false );

    private static final Unsafe unsafe;
    private static final MethodHandle sharedStringConstructor;
    private static final String allowUnalignedMemoryAccessProperty =
            "org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.allowUnalignedMemoryAccess";

    private static final Class<?> directByteBufferClass;
    private static final Constructor<?> directByteBufferCtor;
    private static final long directByteBufferMarkOffset;
    private static final long directByteBufferPositionOffset;
    private static final long directByteBufferLimitOffset;
    private static final long directByteBufferCapacityOffset;
    private static final long directByteBufferAddressOffset;

    private static final int pageSize;

    public static final boolean allowUnalignedMemoryAccess;
    public static final boolean storeByteOrderIsNative;

    static
    {
        unsafe = getUnsafe();

        MethodHandles.Lookup lookup = MethodHandles.lookup();
        sharedStringConstructor = getSharedStringConstructorMethodHandle( lookup );

        Class<?> dbbClass = null;
        Constructor<?> ctor = null;
        long dbbMarkOffset = 0;
        long dbbPositionOffset = 0;
        long dbbLimitOffset = 0;
        long dbbCapacityOffset = 0;
        long dbbAddressOffset = 0;
        int ps = 4096;
        try
        {
            dbbClass = Class.forName( "java.nio.DirectByteBuffer" );
            Class<?> bufferClass = Class.forName( "java.nio.Buffer" );
            dbbMarkOffset = unsafe.objectFieldOffset( bufferClass.getDeclaredField( "mark" ) );
            dbbPositionOffset = unsafe.objectFieldOffset( bufferClass.getDeclaredField( "position" ) );
            dbbLimitOffset = unsafe.objectFieldOffset( bufferClass.getDeclaredField( "limit" ) );
            dbbCapacityOffset = unsafe.objectFieldOffset( bufferClass.getDeclaredField( "capacity" ) );
            dbbAddressOffset = unsafe.objectFieldOffset( bufferClass.getDeclaredField( "address" ) );
            ps = unsafe.pageSize();
        }
        catch ( Throwable e )
        {
            if ( dbbClass == null )
            {
                throw new LinkageError( "Cannot to link java.nio.DirectByteBuffer", e );
            }
            try
            {
                ctor = dbbClass.getConstructor( Long.TYPE, Integer.TYPE );
                ctor.setAccessible( true );
            }
            catch ( NoSuchMethodException e1 )
            {
                throw new LinkageError( "Cannot find JNI constructor for java.nio.DirectByteBuffer", e1 );
            }
        }
        directByteBufferClass = dbbClass;
        directByteBufferCtor = ctor;
        directByteBufferMarkOffset = dbbMarkOffset;
        directByteBufferPositionOffset = dbbPositionOffset;
        directByteBufferLimitOffset = dbbLimitOffset;
        directByteBufferCapacityOffset = dbbCapacityOffset;
        directByteBufferAddressOffset = dbbAddressOffset;
        pageSize = ps;

        // See java.nio.Bits.unaligned() and its uses.
        String alignmentProperty = System.getProperty( allowUnalignedMemoryAccessProperty );
        if ( alignmentProperty != null &&
                (alignmentProperty.equalsIgnoreCase( "true" )
                        || alignmentProperty.equalsIgnoreCase( "false" )) )
        {
            allowUnalignedMemoryAccess = Boolean.parseBoolean( alignmentProperty );
        }
        else
        {
            boolean unaligned;
            String arch = System.getProperty( "os.arch", "?" );
            switch ( arch ) // list of architectures that support unaligned access to memory
            {
            case "x86_64":
            case "i386":
            case "x86":
            case "amd64":
            case "ppc64":
            case "ppc64le":
            case "ppc64be":
                unaligned = true;
                break;
            default:
                unaligned = false;
                break;
            }
            allowUnalignedMemoryAccess = unaligned;
        }
        storeByteOrderIsNative = ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN;
    }

    private static Unsafe getUnsafe()
    {
        try
        {
            PrivilegedExceptionAction<Unsafe> getUnsafe = () -> {
                try
                {
                    return Unsafe.getUnsafe();
                }
                catch ( Exception e )
                {
                    Class<Unsafe> type = Unsafe.class;
                    Field[] fields = type.getDeclaredFields();
                    for ( Field field : fields )
                    {
                        if ( Modifier.isStatic( field.getModifiers() )
                             && type.isAssignableFrom( field.getType() ) )
                        {
                            field.setAccessible( true );
                            return type.cast( field.get( null ) );
                        }
                    }
                    LinkageError error = new LinkageError( "No static field of type sun.misc.Unsafe" );
                    error.addSuppressed( e );
                    throw error;
                }
            };
            return AccessController.doPrivileged( getUnsafe );
        }
        catch ( Exception e )
        {
            throw new LinkageError( "Cannot access sun.misc.Unsafe", e );
        }
    }

    /**
     * @throws java.lang.LinkageError if the Unsafe tools are not available on in this JVM.
     */
    public static void assertHasUnsafe()
    {
        if ( unsafe == null )
        {
            throw new LinkageError( "Unsafe not available" );
        }
    }

    private static MethodHandle getSharedStringConstructorMethodHandle(
            MethodHandles.Lookup lookup )
    {
        try
        {
            Constructor<String> constructor = String.class.getDeclaredConstructor( char[].class, Boolean.TYPE );
            constructor.setAccessible( true );
            return lookup.unreflectConstructor( constructor );
        }
        catch ( Exception e )
        {
            return null;
        }
    }

    /**
     * Get the object-relative field offset.
     */
    public static long getFieldOffset( Class<?> type, String field )
    {
        try
        {
            return unsafe.objectFieldOffset( type.getDeclaredField( field ) );
        }
        catch ( NoSuchFieldException e )
        {
            String message = "Could not get offset of '" + field + "' field on type " + type;
            throw new LinkageError( message, e );
        }
    }

    /**
     * Atomically add the given delta to the int field, and return its previous value.
     * <p>
     * This has the memory visibility semantics of a volatile read followed by a volatile write.
     */
    public static int getAndAddInt( Object obj, long offset, int delta )
    {
        return unsafe.getAndAddInt( obj, offset, delta );
    }

    /**
     * Orders loads before the fence, with loads and stores after the fence.
     */
    public static void loadFence()
    {
        unsafe.loadFence();
    }

    /**
     * Orders stores before the fence, with loads and stores after the fence.
     */
    public static void storeFence()
    {
        unsafe.storeFence();
    }

    /**
     * Atomically compare the current value of the given long field with the expected value, and if they are the
     * equal, set the field to the updated value and return true. Otherwise return false.
     * <p>
     * If this method returns true, then it has the memory visibility semantics of a volatile read followed by a
     * volatile write.
     */
    public static boolean compareAndSwapLong(
            Object obj, long offset, long expected, long update )
    {
        return unsafe.compareAndSwapLong( obj, offset, expected, update );
    }

    /**
     * Same as compareAndSwapLong, but for object references.
     */
    public static boolean compareAndSwapObject(
            Object obj, long offset, Object expected, Object update )
    {
        return unsafe.compareAndSwapObject( obj, offset, expected, update );
    }

    /**
     * Same as getAndAddInt, but for object references.
     */
    public static Object getAndSetObject( Object obj, long offset, Object newValue )
    {
        return unsafe.getAndSetObject( obj, offset, newValue );
    }

    /**
     * Create a string with a char[] that you know is not going to be modified, so avoid the copy constructor.
     *
     * @param chars array that will back the new string
     * @return the created string
     */
    public static String newSharedArrayString( char[] chars )
    {
        if ( sharedStringConstructor != null )
        {
            try
            {
                return (String) sharedStringConstructor.invokeExact( chars, true );
            }
            catch ( Throwable throwable )
            {
                throw new LinkageError( "Unexpected 'String constructor' intrinsic failure", throwable );
            }
        }
        else
        {
            return new String( chars );
        }
    }

    /**
     * Allocate a slab of memory of the given size in bytes, and return a pointer to that memory.
     * <p>
     * The memory is aligned such that it can be used for any data type. The memory is cleared, so all bytes are zero.
     */
    public static long allocateMemory( long sizeInBytes )
    {
        final long pointer = unsafe.allocateMemory( sizeInBytes );
        if ( DIRTY_MEMORY )
        {
            setMemory( pointer, sizeInBytes, (byte) 0xA5 );
        }
        addAllocatedPointer( pointer, sizeInBytes );
        return pointer;
    }

    /**
     * Free the memory that was allocated with {@link #allocateMemory}.
     */
    public static void free( long pointer )
    {
        checkFree( pointer );
        unsafe.freeMemory( pointer );
    }

    private static final class FreeTrace extends Throwable implements Comparable<FreeTrace>
    {
        private final long pointer;
        private final long size;
        private final long id;
        private final long nanoTime;
        private long referenceTime;

        private FreeTrace( long pointer, long size, long id )
        {
            this.pointer = pointer;
            this.size = size;
            this.id = id;
            this.nanoTime = System.nanoTime();
        }

        private boolean contains( long pointer )
        {
            return this.pointer <= pointer && pointer <= this.pointer + size;
        }

        @Override
        public int compareTo( FreeTrace that )
        {
            return Long.compare( this.id, that.id );
        }

        @Override
        public String getMessage()
        {
            return format( "0x%x of %6d bytes, freed %s µs ago at", pointer, size, (referenceTime - nanoTime) / 1000 );
        }
    }

    private static final ConcurrentSkipListMap<Long, Long> pointers = new ConcurrentSkipListMap<>();
    private static final FreeTrace[] freeTraces = CHECK_NATIVE_ACCESS? new FreeTrace[4096] : null;
    private static final AtomicLong freeTraceCounter = new AtomicLong();

    private static void addAllocatedPointer( long pointer, long sizeInBytes )
    {
        if ( CHECK_NATIVE_ACCESS )
        {
            pointers.put( pointer, sizeInBytes );
        }
    }

    private static void checkFree( long pointer )
    {
        if ( CHECK_NATIVE_ACCESS )
        {
            doCheckFree( pointer );
        }
    }

    private static void doCheckFree( long pointer )
    {
        Long size = pointers.remove( pointer );
        if ( size == null )
        {
            StringBuilder sb = new StringBuilder( format( "Bad free: 0x%x, valid pointers are:", pointer ) );
            pointers.forEach( (k,v) -> sb.append( '\n' ).append( k ) );
            throw new AssertionError( sb.toString() );
        }
        long count = freeTraceCounter.getAndIncrement();
        int idx = (int) (count & 4095);
        freeTraces[idx] = new FreeTrace( pointer, size, count );
    }

    private static void checkAccess( long pointer, int size )
    {
        if ( CHECK_NATIVE_ACCESS )
        {
            doCheckAccess( pointer, size );
        }
    }

    private static void doCheckAccess( long pointer, int size )
    {
        long now = System.nanoTime();
        Map.Entry<Long,Long> fentry = pointers.floorEntry( pointer + size );
        Map.Entry<Long,Long> centry = pointers.ceilingEntry( pointer );
        if ( fentry == null || fentry.getKey() + fentry.getValue() < pointer + size )
        {
            long faddr = fentry == null? 0 : fentry.getKey();
            long fsize = fentry == null? 0 : fentry.getValue();
            long foffset = pointer - (faddr + fsize);
            long caddr = centry == null? 0 : centry.getKey();
            long csize = centry == null? 0 : centry.getValue();
            long coffset = caddr - (pointer + size);
            boolean floorIsNearest = foffset < coffset;
            long naddr = floorIsNearest? faddr : caddr;
            long nsize = floorIsNearest? fsize : csize;
            long noffset = floorIsNearest? foffset : coffset;
            List<FreeTrace> recentFrees = Arrays.stream( freeTraces )
                                                .filter( trace -> trace != null )
                                                .filter( trace -> trace.contains( pointer ) )
                                                .sorted()
                                                .collect( Collectors.toList() );
            AssertionError error = new AssertionError( format(
                    "Bad access at address 0x%x with size %s, nearest valid allocation is " +
                    "0x%x (%s bytes, off by %s bytes). " +
                    "Recent relevant frees (of %s) are attached as suppressed exceptions.",
                    pointer, size, naddr, nsize, noffset, freeTraceCounter.get() ) );
            for ( FreeTrace recentFree : recentFrees )
            {
                recentFree.referenceTime = now;
                error.addSuppressed( recentFree );
            }
            throw error;
        }
    }

    /**
     * Return the power-of-2 native memory page size.
     */
    public static int pageSize()
    {
        return pageSize;
    }

    public static void putBoolean( Object obj, long offset, boolean value )
    {
        unsafe.putBoolean( obj, offset, value );
    }

    public static boolean getBoolean( Object obj, long offset )
    {
        return unsafe.getBoolean( obj, offset );
    }

    public static void putBooleanVolatile( Object obj, long offset, boolean value )
    {
        unsafe.putBooleanVolatile( obj, offset, value );
    }

    public static boolean getBooleanVolatile( Object obj, long offset )
    {
        return unsafe.getBooleanVolatile( obj, offset );
    }

    public static void putByte( long address, byte value )
    {
        checkAccess( address, Byte.BYTES );
        unsafe.putByte( address, value );
    }

    public static byte getByte( long address )
    {
        checkAccess( address, Byte.BYTES );
        return unsafe.getByte( address );
    }

    public static void putByteVolatile( long address, byte value )
    {
        checkAccess( address, Byte.BYTES );
        unsafe.putByteVolatile( null, address, value );
    }

    public static byte getByteVolatile( long address )
    {
        checkAccess( address, Byte.BYTES );
        return unsafe.getByteVolatile( null, address );
    }

    public static void putByte( Object obj, long offset, byte value )
    {
        unsafe.putByte( obj, offset, value );
    }

    public static byte getByte( Object obj, long offset )
    {
        return unsafe.getByte( obj, offset );
    }

    public static byte getByteVolatile( Object obj, long offset )
    {
        return unsafe.getByteVolatile( obj, offset );
    }

    public static void putByteVolatile( Object obj, long offset, byte value )
    {
        unsafe.putByteVolatile( obj, offset, value );
    }

    public static void putShort( long address, short value )
    {
        checkAccess( address, Short.BYTES );
        unsafe.putShort( address, value );
    }

    public static short getShort( long address )
    {
        checkAccess( address, Short.BYTES );
        return unsafe.getShort( address );
    }

    public static void putShortVolatile( long address, short value )
    {
        checkAccess( address, Short.BYTES );
        unsafe.putShortVolatile( null, address, value );
    }

    public static short getShortVolatile( long address )
    {
        checkAccess( address, Short.BYTES );
        return unsafe.getShortVolatile( null, address );
    }

    public static void putShort( Object obj, long offset, short value )
    {
        unsafe.putShort( obj, offset, value );
    }

    public static short getShort( Object obj, long offset )
    {
        return unsafe.getShort( obj, offset );
    }

    public static void putShortVolatile( Object obj, long offset, short value )
    {
        unsafe.putShortVolatile( obj, offset, value );
    }

    public static short getShortVolatile( Object obj, long offset )
    {
        return unsafe.getShortVolatile( obj, offset );
    }

    public static void putFloat( long address, float value )
    {
        checkAccess( address, Float.BYTES );
        unsafe.putFloat( address, value );
    }

    public static float getFloat( long address )
    {
        checkAccess( address, Float.BYTES );
        return unsafe.getFloat( address );
    }

    public static void putFloatVolatile( long address, float value )
    {
        checkAccess( address, Float.BYTES );
        unsafe.putFloatVolatile( null, address, value );
    }

    public static float getFloatVolatile( long address )
    {
        checkAccess( address, Float.BYTES );
        return unsafe.getFloatVolatile( null, address );
    }

    public static void putFloat( Object obj, long offset, float value )
    {
        unsafe.putFloat( obj, offset, value );
    }

    public static float getFloat( Object obj, long offset )
    {
        return unsafe.getFloat( obj, offset );
    }

    public static void putFloatVolatile( Object obj, long offset, float value )
    {
        unsafe.putFloatVolatile( obj, offset, value );
    }

    public static float getFloatVolatile( Object obj, long offset )
    {
        return unsafe.getFloatVolatile( obj, offset );
    }

    public static void putChar( long address, char value )
    {
        checkAccess( address, Character.BYTES );
        unsafe.putChar( address, value );
    }

    public static char getChar( long address )
    {
        checkAccess( address, Character.BYTES );
        return unsafe.getChar( address );
    }

    public static void putCharVolatile( long address, char value )
    {
        checkAccess( address, Character.BYTES );
        unsafe.putCharVolatile( null, address, value );
    }

    public static char getCharVolatile( long address )
    {
        checkAccess( address, Character.BYTES );
        return unsafe.getCharVolatile( null, address );
    }

    public static void putChar( Object obj, long offset, char value )
    {
        unsafe.putChar( obj, offset, value );
    }

    public static char getChar( Object obj, long offset )
    {
        return unsafe.getChar( obj, offset );
    }

    public static void putCharVolatile( Object obj, long offset, char value )
    {
        unsafe.putCharVolatile( obj, offset, value );
    }

    public static char getCharVolatile( Object obj, long offset )
    {
        return unsafe.getCharVolatile( obj, offset );
    }

    public static void putInt( long address, int value )
    {
        checkAccess( address, Integer.BYTES );
        unsafe.putInt( address, value );
    }

    public static int getInt( long address )
    {
        checkAccess( address, Integer.BYTES );
        return unsafe.getInt( address );
    }

    public static void putIntVolatile( long address, int value )
    {
        checkAccess( address, Integer.BYTES );
        unsafe.putIntVolatile( null, address, value );
    }

    public static int getIntVolatile( long address )
    {
        checkAccess( address, Integer.BYTES );
        return unsafe.getIntVolatile( null, address );
    }

    public static void putInt( Object obj, long offset, int value )
    {
        unsafe.putInt( obj, offset, value );
    }

    public static int getInt( Object obj, long offset )
    {
        return unsafe.getInt( obj, offset );
    }

    public static void putIntVolatile( Object obj, long offset, int value )
    {
        unsafe.putIntVolatile( obj, offset, value );
    }

    public static int getIntVolatile( Object obj, long offset )
    {
        return unsafe.getIntVolatile( obj, offset );
    }

    public static void putLongVolatile( long address, long value )
    {
        checkAccess( address, Long.BYTES );
        unsafe.putLongVolatile( null, address, value );
    }

    public static long getLongVolatile( long address )
    {
        checkAccess( address, Long.BYTES );
        return unsafe.getLongVolatile( null, address );
    }

    public static void putLong( long address, long value )
    {
        checkAccess( address, Long.BYTES );
        unsafe.putLong( address, value );
    }

    public static long getLong( long address )
    {
        checkAccess( address, Long.BYTES );
        return unsafe.getLong( address );
    }

    public static void putLong( Object obj, long offset, long value )
    {
        unsafe.putLong( obj, offset, value );
    }

    public static long getLong( Object obj, long offset )
    {
        return unsafe.getLong( obj, offset );
    }

    public static void putLongVolatile( Object obj, long offset, long value )
    {
        unsafe.putLongVolatile( obj, offset, value );
    }

    public static long getLongVolatile( Object obj, long offset )
    {
        return unsafe.getLongVolatile( obj, offset );
    }

    public static void putDouble( long address, double value )
    {
        checkAccess( address, Double.BYTES );
        unsafe.putDouble( address, value );
    }

    public static double getDouble( long address )
    {
        checkAccess( address, Double.BYTES );
        return unsafe.getDouble( address );
    }

    public static void putDoubleVolatile( long address, double value )
    {
        checkAccess( address, Double.BYTES );
        unsafe.putDoubleVolatile( null, address, value );
    }

    public static double getDoubleVolatile( long address )
    {
        checkAccess( address, Double.BYTES );
        return unsafe.getDoubleVolatile( null, address );
    }

    public static void putDouble( Object obj, long offset, double value )
    {
        unsafe.putDouble( obj, offset, value );
    }

    public static double getDouble( Object obj, long offset )
    {
        return unsafe.getDouble( obj, offset );
    }

    public static void putDoubleVolatile( Object obj, long offset, double value )
    {
        unsafe.putDoubleVolatile( obj, offset, value );
    }

    public static double getDoubleVolatile( Object obj, long offset )
    {
        return unsafe.getDoubleVolatile( obj, offset );
    }

    public static void putObject( Object obj, long offset, Object value )
    {
        unsafe.putObject( obj, offset, value );
    }

    public static Object getObject( Object obj, long offset )
    {
        return unsafe.getObject( obj, offset );
    }

    public static Object getObjectVolatile( Object obj, long offset )
    {
        return unsafe.getObjectVolatile( obj, offset );
    }

    public static void putObjectVolatile( Object obj, long offset, Object value )
    {
        unsafe.putObjectVolatile( obj, offset, value );
    }

    public static int arrayBaseOffset( Class klass )
    {
        return unsafe.arrayBaseOffset( klass );
    }

    public static int arrayIndexScale( Class klass )
    {
        int scale = unsafe.arrayIndexScale( klass );
        if ( scale == 0 )
        {
            throw new AssertionError( "Array type too narrow for unsafe access: " + klass );
        }
        return scale;
    }

    public static int arrayOffset( int index, int base, int scale )
    {
        return base + index * scale;
    }

    /**
     * Set the given number of bytes to the given value, starting from the given address.
     */
    public static void setMemory( long address, long bytes, byte value )
    {
        unsafe.setMemory( address, bytes, value );
    }

    /**
     * Copy the given number of bytes from the source address to the destination address.
     */
    public static void copyMemory( long srcAddress, long destAddress, long bytes )
    {
        unsafe.copyMemory( srcAddress, destAddress, bytes );
    }

    /**
     * Create a new DirectByteBuffer that wraps the given address and has the given capacity.
     * <p>
     * The ByteBuffer does NOT create a Cleaner, or otherwise register the pointer for freeing.
     */
    public static ByteBuffer newDirectByteBuffer( long addr, int cap ) throws Exception
    {
        if ( directByteBufferCtor == null )
        {
            // Simulate the JNI NewDirectByteBuffer(void*, long) invocation.
            Object dbb = unsafe.allocateInstance( directByteBufferClass );
            initDirectByteBuffer( dbb, addr, cap );
            return (ByteBuffer) dbb;
        }
        // Reflection based fallback code.
        return (ByteBuffer) directByteBufferCtor.newInstance( addr, cap );
    }

    /**
     * Initialize (simulate calling the constructor of) the given DirectByteBuffer.
     */
    public static void initDirectByteBuffer( Object dbb, long addr, int cap )
    {
        unsafe.putInt( dbb, directByteBufferMarkOffset, -1 );
        unsafe.putInt( dbb, directByteBufferPositionOffset, 0 );
        unsafe.putInt( dbb, directByteBufferLimitOffset, cap );
        unsafe.putInt( dbb, directByteBufferCapacityOffset, cap );
        unsafe.putLong( dbb, directByteBufferAddressOffset, addr );
    }
}
