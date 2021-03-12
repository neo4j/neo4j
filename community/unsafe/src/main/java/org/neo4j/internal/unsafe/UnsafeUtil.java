/*
 * Copyright (c) "Neo4j"
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

import com.sun.jna.Native;
import com.sun.jna.Pointer;
import sun.misc.Unsafe;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.neo4j.memory.MemoryTracker;

import static java.lang.Long.compareUnsigned;
import static java.lang.String.format;
import static org.neo4j.util.FeatureToggles.flag;

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
     * The {@link UnsafeUtil#allocateMemory(long, MemoryTracker)} method is not guaranteed to allocate
     * zeroed out memory, but might often do so by pure chance.
     * <p>
     * Enabling this feature will make sure that the allocated memory is full of random data, such that we can test
     * and verify that our code does not assume that memory is clean when allocated.
     */
    private static final boolean DIRTY_MEMORY = flag( UnsafeUtil.class, "DIRTY_MEMORY", false );
    private static final boolean CHECK_NATIVE_ACCESS = flag( UnsafeUtil.class, "CHECK_NATIVE_ACCESS", false );
    // this allows us to temporarily disable the checking, for performance:
    private static boolean nativeAccessCheckEnabled = true;

    private static final Unsafe unsafe;
    private static final String allowUnalignedMemoryAccessProperty = "org.neo4j.internal.unsafe.UnsafeUtil.allowUnalignedMemoryAccess";

    private static final ConcurrentSkipListMap<Long, Allocation> allocations = new ConcurrentSkipListMap<>( Long::compareUnsigned );
    private static final ThreadLocal<Allocation> lastUsedAllocation = new ThreadLocal<>();
    private static final FreeTrace[] freeTraces = CHECK_NATIVE_ACCESS ? new FreeTrace[4096] : null;
    private static final AtomicLong freeCounter = new AtomicLong();

    public static final Class<?> DIRECT_BYTE_BUFFER_CLASS;
    private static final VarHandle DIRECT_BYTE_BUFFER_MARK;
    private static final VarHandle DIRECT_BYTE_BUFFER_POSITION;
    private static final VarHandle DIRECT_BYTE_BUFFER_LIMIT;
    private static final VarHandle DIRECT_BYTE_BUFFER_CAPACITY;
    private static final VarHandle DIRECT_BYTE_BUFFER_ADDRESS;
    private static final VarHandle DIRECT_BYTE_BUFFER_ATTACHMENT;
    private static final MethodHandle DIRECT_BYTE_BUFFER_CONSTRUCTOR;

    private static final int pageSize;

    public static final boolean allowUnalignedMemoryAccess;
    public static final boolean storeByteOrderIsNative;

    static
    {
        unsafe = UnsafeAccessor.getUnsafe();

        Class<?> dbbClass = null;
        VarHandle dbbMark = null;
        VarHandle dbbPosition = null;
        VarHandle dbbLimit = null;
        VarHandle dbbCapacity = null;
        VarHandle dbbAddress = null;
        VarHandle dbbAttachment = null;
        MethodHandle dbbCtor = null;
        int ps = 4096;
        try
        {
            MethodHandles.Lookup bufferLookup = MethodHandles.privateLookupIn( Buffer.class, MethodHandles.lookup() );
            dbbMark = bufferLookup.findVarHandle( Buffer.class, "mark", int.class );
            dbbPosition = bufferLookup.findVarHandle( Buffer.class, "position", int.class );
            dbbLimit = bufferLookup.findVarHandle( Buffer.class, "limit", int.class );
            dbbCapacity = bufferLookup.findVarHandle( Buffer.class, "capacity", int.class );
            dbbAddress = bufferLookup.findVarHandle( Buffer.class, "address", long.class );

            dbbClass = Class.forName( "java.nio.DirectByteBuffer" );
            MethodHandles.Lookup directByteBufferLookup = MethodHandles.privateLookupIn( dbbClass, MethodHandles.lookup() );
            dbbAttachment = directByteBufferLookup.findVarHandle( dbbClass, "att", Object.class );

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
                MethodHandles.Lookup directByteBufferLookup = MethodHandles.privateLookupIn( dbbClass, MethodHandles.lookup() );
                dbbCtor = directByteBufferLookup.findConstructor( dbbClass, MethodType.methodType( void.class, long.class, int.class ) );
            }
            catch ( Throwable e1 )
            {
                throw new LinkageError( "Cannot find JNI constructor for java.nio.DirectByteBuffer", e1 );
            }
        }
        DIRECT_BYTE_BUFFER_CLASS = dbbClass;
        DIRECT_BYTE_BUFFER_MARK = dbbMark;
        DIRECT_BYTE_BUFFER_POSITION = dbbPosition;
        DIRECT_BYTE_BUFFER_LIMIT = dbbLimit;
        DIRECT_BYTE_BUFFER_CAPACITY = dbbCapacity;
        DIRECT_BYTE_BUFFER_ADDRESS = dbbAddress;
        DIRECT_BYTE_BUFFER_ATTACHMENT = dbbAttachment;
        DIRECT_BYTE_BUFFER_CONSTRUCTOR = dbbCtor;
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

    private UnsafeUtil()
    {
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
     * Get the object-relative field offset.
     */
    public static long getFieldOffset( Field field )
    {
        return unsafe.objectFieldOffset( field );
    }

    /**
     * Atomically add the given delta to the long field, and return its previous value.
     * <p>
     * This has the memory visibility semantics of a volatile read followed by a volatile write.
     */
    public static long getAndAddLong( Object obj, long offset, long delta )
    {
        return unsafe.getAndAddLong( obj, offset, delta );
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
     * Atomically exchanges provided <code>newValue</code> with the current value of field or array element, with
     * provided <code>offset</code>.
     */
    public static long getAndSetLong( Object object, long offset, long newValue )
    {
        return unsafe.getAndSetLong( object, offset, newValue );
    }

    /**
     * Atomically set field or array element to a maximum between current value and provided <code>newValue</code>
     */
    public static void compareAndSetMaxLong( Object object, long fieldOffset, long newValue )
    {
        long currentValue;
        do
        {
            currentValue = UnsafeUtil.getLongVolatile( object, fieldOffset );
            if ( currentValue >= newValue )
            {
                return;
            }
        }
        while ( !UnsafeUtil.compareAndSwapLong( object, fieldOffset, currentValue, newValue ) );
    }

    /**
     * Allocates a {@link ByteBuffer}
     * @param size The size of the buffer to allocate
     */
    public static ByteBuffer allocateByteBuffer( int size, MemoryTracker memoryTracker )
    {
        try
        {
            long addr = allocateMemory( size, memoryTracker );
            setMemory( addr, size, (byte) 0 );
            return newDirectByteBuffer( addr, size );
        }
        catch ( Throwable e )
        {
            throw new RuntimeException( e );
        }
    }

    /**
     * Allocates a {@link ByteBuffer}
     * @param byteBuffer The ByteBuffer to free, allocated by {@link #allocateByteBuffer(int, MemoryTracker)}
     */
    public static void freeByteBuffer( ByteBuffer byteBuffer, MemoryTracker memoryTracker )
    {
        int bytes = byteBuffer.capacity();
        long addr = getDirectByteBufferAddress( byteBuffer );
        if ( addr == 0 )
        {
            return; // This buffer has already been freed.
        }

        // Nerf the byte buffer, causing all future accesses to get out-of-bounds.
        DIRECT_BYTE_BUFFER_MARK.set( byteBuffer, -1 );
        DIRECT_BYTE_BUFFER_POSITION.set( byteBuffer, 0 );
        DIRECT_BYTE_BUFFER_LIMIT.set( byteBuffer, 0 );
        DIRECT_BYTE_BUFFER_CAPACITY.set( byteBuffer, 0 );
        DIRECT_BYTE_BUFFER_ADDRESS.set( byteBuffer, 0L );

        // Free the buffer.
        free( addr, bytes, memoryTracker );
    }

    /**
     * Unwraps an original buffer from a suspected buffer slice.
     * If the submitted buffer is not a slice, {@code null} will be returned.
     * <p>
     * This method works only with direct buffers and an exception
     * will be thrown if a heap buffer is submitted.
     */
    public static ByteBuffer getOriginalBufferFromSlice( ByteBuffer byteBuffer )
    {
        if ( !byteBuffer.isDirect() )
        {
            throw new IllegalArgumentException( "The submitted buffer is not a direct one:" + byteBuffer );
        }
        return (ByteBuffer) DIRECT_BYTE_BUFFER_ATTACHMENT.get( byteBuffer );
    }

    /**
     * Invokes cleaner for provided direct byte buffer.
     *
     * @param byteBuffer provided byte buffer.
     */
    public static void invokeCleaner( ByteBuffer byteBuffer )
    {
        unsafe.invokeCleaner( byteBuffer );
    }

    /**
     * Allocate a block of memory of the given size in bytes, and return a pointer to that memory.
     * <p>
     * The memory is aligned such that it can be used for any data type.
     * The memory is uninitialised, so it may contain random garbage, or it may not.
     *
     * @return a pointer to the allocated memory
     */
    public static long allocateMemory( long bytes, MemoryTracker memoryTracker ) throws NativeMemoryAllocationRefusedError
    {
        final long pointer = Native.malloc( bytes );
        if ( pointer == 0 )
        {
            throw new NativeMemoryAllocationRefusedError( bytes, memoryTracker.usedNativeMemory() );
        }

        addAllocatedPointer( pointer, bytes );
        memoryTracker.allocateNative( bytes );
        if ( DIRTY_MEMORY )
        {
            setMemory( pointer, bytes, (byte) 0xA5 );
        }
        return pointer;
    }

    /**
     * Free the memory that was allocated with {@link #allocateMemory} and update memory allocation tracker accordingly.
     */
    public static void free( long pointer, long bytes, MemoryTracker memoryTracker )
    {
        checkFree( pointer );
        Native.free( pointer );
        memoryTracker.releaseNative( bytes );
    }

    private static void addAllocatedPointer( long pointer, long sizeInBytes )
    {
        if ( CHECK_NATIVE_ACCESS )
        {
            allocations.put( pointer, new Allocation( pointer, sizeInBytes, freeCounter.get() ) );
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
        long count = freeCounter.getAndIncrement();
        Allocation allocation = allocations.remove( pointer );
        if ( allocation == null )
        {
            StringBuilder sb = new StringBuilder( format( "Bad free: 0x%x, valid pointers are:", pointer ) );
            allocations.forEach( ( k, v ) -> sb.append( '\n' ).append( "0x" ).append( Long.toHexString( k ) ) );
            throw new AssertionError( sb.toString() );
        }
        allocation.freed = true;
        int idx = (int) (count & 4095);
        freeTraces[idx] = new FreeTrace( pointer, allocation, count );
    }

    private static void checkAccess( long pointer, long size )
    {
        if ( CHECK_NATIVE_ACCESS && nativeAccessCheckEnabled )
        {
            doCheckAccess( pointer, size );
        }
    }

    private static void doCheckAccess( long pointer, long size )
    {
        long boundary = pointer + size;
        Allocation allocation = lastUsedAllocation.get();
        if ( allocation != null && !allocation.freed )
        {
            if ( compareUnsigned( allocation.pointer, pointer ) <= 0 &&
                 compareUnsigned( allocation.boundary, boundary ) > 0 &&
                 allocation.freeCounter == freeCounter.get() )
            {
                return;
            }
        }

        Map.Entry<Long,Allocation> fentry = allocations.floorEntry( boundary );
        if ( fentry == null || compareUnsigned( fentry.getValue().boundary, boundary ) < 0 )
        {
            Map.Entry<Long,Allocation> centry = allocations.ceilingEntry( pointer );
            throwBadAccess( pointer, size, fentry, centry );
        }
        //noinspection ConstantConditions
        lastUsedAllocation.set( fentry.getValue() );
    }

    private static void throwBadAccess( long pointer, long size, Map.Entry<Long,Allocation> fentry,
                                        Map.Entry<Long,Allocation> centry )
    {
        long now = System.nanoTime();
        long faddr = fentry == null ? 0 : fentry.getKey();
        long fsize = fentry == null ? 0 : fentry.getValue().sizeInBytes;
        long foffset = pointer - (faddr + fsize);
        long caddr = centry == null ? 0 : centry.getKey();
        long csize = centry == null ? 0 : centry.getValue().sizeInBytes;
        long coffset = caddr - (pointer + size);
        boolean floorIsNearest = foffset < coffset;
        long naddr = floorIsNearest ? faddr : caddr;
        long nsize = floorIsNearest ? fsize : csize;
        long noffset = floorIsNearest ? foffset : coffset;
        List<FreeTrace> recentFrees = Arrays.stream( freeTraces )
                                            .filter( Objects::nonNull )
                                            .filter( trace -> trace.contains( pointer ) )
                                            .sorted()
                                            .collect( Collectors.toList() );
        AssertionError error = new AssertionError( format(
                "Bad access to address 0x%x with size %s, nearest valid allocation is " +
                "0x%x (%s bytes, off by %s bytes). " +
                "Recent relevant frees (of %s) are attached as suppressed exceptions.",
                pointer, size, naddr, nsize, noffset, freeCounter.get() ) );
        for ( FreeTrace recentFree : recentFrees )
        {
            recentFree.referenceTime = now;
            error.addSuppressed( recentFree );
        }
        throw error;
    }

    /**
     * Return the power-of-2 native memory page size.
     */
    public static int pageSize()
    {
        return pageSize;
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

    public static void putByte( Object obj, long offset, byte value )
    {
        unsafe.putByte( obj, offset, value );
    }

    public static byte getByte( Object obj, long offset )
    {
        return unsafe.getByte( obj, offset );
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

    public static long getLongVolatile( Object obj, long offset )
    {
        return unsafe.getLongVolatile( obj, offset );
    }

    public static int arrayBaseOffset( Class<?> klass )
    {
        return unsafe.arrayBaseOffset( klass );
    }

    public static int arrayIndexScale( Class<?> klass )
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
        checkAccess( address, bytes );
        new Pointer( address ).setMemory( 0, bytes, value );
    }

    /**
     * Copy the given number of bytes from the source address to the destination address.
     */
    public static void copyMemory( long srcAddress, long destAddress, long bytes )
    {
        checkAccess( srcAddress, bytes );
        checkAccess( destAddress, bytes );
        unsafe.copyMemory( srcAddress, destAddress, bytes );
    }

    /**
     * Same as {@link #copyMemory(long, long, long)}, but with an object-relative addressing mode,
     * where {@code null} object bases imply that the offset is an absolute address.
     */
    public static void copyMemory( Object srcBase, long srcOffset, Object destBase, long destOffset, long bytes )
    {
        unsafe.copyMemory( srcBase, srcOffset, destBase, destOffset, bytes );
    }

    /**
     * Create a new DirectByteBuffer that wraps the given address and has the given capacity.
     * <p>
     * The ByteBuffer does NOT create a Cleaner, or otherwise register the pointer for freeing.
     */
    public static ByteBuffer newDirectByteBuffer( long addr, int cap ) throws Throwable
    {
        checkAccess( addr, cap );
        if ( DIRECT_BYTE_BUFFER_CONSTRUCTOR == null )
        {
            // Simulate the JNI NewDirectByteBuffer(void*, long) invocation.
            ByteBuffer dbb = (ByteBuffer) unsafe.allocateInstance( DIRECT_BYTE_BUFFER_CLASS );
            initDirectByteBuffer( dbb, addr, cap );
            return dbb;
        }
        // Reflection based fallback code.
        return (ByteBuffer) DIRECT_BYTE_BUFFER_CONSTRUCTOR.invokeExact( addr, cap );
    }

    /**
     * Initialize (simulate calling the constructor of) the given DirectByteBuffer.
     */
    public static void initDirectByteBuffer( ByteBuffer dbb, long addr, int cap )
    {
        checkAccess( addr, cap );
        dbb.order( ByteOrder.BIG_ENDIAN );
        DIRECT_BYTE_BUFFER_MARK.set( dbb, -1 );
        DIRECT_BYTE_BUFFER_POSITION.set( dbb, 0 );
        DIRECT_BYTE_BUFFER_LIMIT.set( dbb, cap );
        DIRECT_BYTE_BUFFER_CAPACITY.set( dbb, cap );
        DIRECT_BYTE_BUFFER_ADDRESS.set( dbb, addr );
    }

    /**
     * Read the value of the address field in the (assumed to be) DirectByteBuffer.
     * <p>
     * <strong>NOTE:</strong> calling this method on a non-direct ByteBuffer is undefined behaviour.
     *
     * @param dbb The direct byte buffer to read the address field from.
     * @return The native memory address in the given direct byte buffer.
     */
    public static long getDirectByteBufferAddress( ByteBuffer dbb )
    {
        return (long) DIRECT_BYTE_BUFFER_ADDRESS.get( dbb );
    }

    /**
     * Change if native access checking is enabled by setting it to the given new setting, and returning the old
     * setting.
     * <p>
     * This is only useful for speeding up tests when you have a lot of them, and they access native memory a lot.
     * This does not disable the recording of memory allocations or frees.
     * <p>
     * Remember to restore the old value so other tests in the same JVM get the benefit of native access checks.
     * <p>
     * The changing of this setting is completely unsynchronized, so you have to order this modification before and
     * after the tests that you want to run without native access checks.
     *
     * @param newSetting The new setting.
     * @return the previous value of this setting.
     */
    public static boolean exchangeNativeAccessCheckEnabled( boolean newSetting )
    {
        boolean previousSetting = nativeAccessCheckEnabled;
        nativeAccessCheckEnabled = newSetting;
        return previousSetting;
    }

    /**
     * Gets a {@code short} at memory address {@code p} by reading byte for byte, instead of the whole value
     * in one go. This can be useful, even necessary in some scenarios where {@link #allowUnalignedMemoryAccess}
     * is {@code false} and {@code p} isn't aligned properly. Values read with this method should have been
     * previously put using {@link #putShortByteWiseLittleEndian(long, short)}.
     *
     * @param p address pointer to start reading at.
     * @return the read value, which was read byte for byte.
     */
    public static short getShortByteWiseLittleEndian( long p )
    {
        short a = (short) (UnsafeUtil.getByte( p ) & 0xFF);
        short b = (short) (UnsafeUtil.getByte( p + 1 ) & 0xFF);
        return (short) ((b << 8) | a);
    }

    /**
     * Gets a {@code int} at memory address {@code p} by reading byte for byte, instead of the whole value
     * in one go. This can be useful, even necessary in some scenarios where {@link #allowUnalignedMemoryAccess}
     * is {@code false} and {@code p} isn't aligned properly. Values read with this method should have been
     * previously put using {@link #putIntByteWiseLittleEndian(long, int)}.
     *
     * @param p address pointer to start reading at.
     * @return the read value, which was read byte for byte.
     */
    public static int getIntByteWiseLittleEndian( long p )
    {
        int a = UnsafeUtil.getByte( p ) & 0xFF;
        int b = UnsafeUtil.getByte( p + 1 ) & 0xFF;
        int c = UnsafeUtil.getByte( p + 2 ) & 0xFF;
        int d = UnsafeUtil.getByte( p + 3 ) & 0xFF;
        return (d << 24) | (c << 16) | (b << 8) | a;
    }

    /**
     * Gets a {@code long} at memory address {@code p} by reading byte for byte, instead of the whole value
     * in one go. This can be useful, even necessary in some scenarios where {@link #allowUnalignedMemoryAccess}
     * is {@code false} and {@code p} isn't aligned properly. Values read with this method should have been
     * previously put using {@link #putLongByteWiseLittleEndian(long, long)}.
     *
     * @param p address pointer to start reading at.
     * @return the read value, which was read byte for byte.
     */
    public static long getLongByteWiseLittleEndian( long p )
    {
        long a = UnsafeUtil.getByte( p ) & 0xFF;
        long b = UnsafeUtil.getByte( p + 1 ) & 0xFF;
        long c = UnsafeUtil.getByte( p + 2 ) & 0xFF;
        long d = UnsafeUtil.getByte( p + 3 ) & 0xFF;
        long e = UnsafeUtil.getByte( p + 4 ) & 0xFF;
        long f = UnsafeUtil.getByte( p + 5 ) & 0xFF;
        long g = UnsafeUtil.getByte( p + 6 ) & 0xFF;
        long h = UnsafeUtil.getByte( p + 7 ) & 0xFF;
        return (h << 56) | (g << 48) | (f << 40) | (e << 32) | (d << 24) | (c << 16) | (b << 8) | a;
    }

    /**
     * Puts a {@code short} at memory address {@code p} by writing byte for byte, instead of the whole value
     * in one go. This can be useful, even necessary in some scenarios where {@link #allowUnalignedMemoryAccess}
     * is {@code false} and {@code p} isn't aligned properly. Values written with this method should be
     * read using {@link #getShortByteWiseLittleEndian(long)}.
     *
     * @param p address pointer to start writing at.
     * @param value value to write byte for byte.
     */
    public static void putShortByteWiseLittleEndian( long p, short value )
    {
        UnsafeUtil.putByte( p, (byte) value );
        UnsafeUtil.putByte( p + 1, (byte) (value >> 8) );
    }

    /**
     * Puts a {@code int} at memory address {@code p} by writing byte for byte, instead of the whole value
     * in one go. This can be useful, even necessary in some scenarios where {@link #allowUnalignedMemoryAccess}
     * is {@code false} and {@code p} isn't aligned properly. Values written with this method should be
     * read using {@link #getIntByteWiseLittleEndian(long)}.
     *
     * @param p address pointer to start writing at.
     * @param value value to write byte for byte.
     */
    public static void putIntByteWiseLittleEndian( long p, int value )
    {
        UnsafeUtil.putByte( p, (byte) value );
        UnsafeUtil.putByte( p + 1, (byte) (value >> 8) );
        UnsafeUtil.putByte( p + 2, (byte) (value >> 16) );
        UnsafeUtil.putByte( p + 3, (byte) (value >> 24) );
    }

    /**
     * Puts a {@code long} at memory address {@code p} by writing byte for byte, instead of the whole value
     * in one go. This can be useful, even necessary in some scenarios where {@link #allowUnalignedMemoryAccess}
     * is {@code false} and {@code p} isn't aligned properly. Values written with this method should be
     * read using {@link #getShortByteWiseLittleEndian(long)}.
     *
     * @param p address pointer to start writing at.
     * @param value value to write byte for byte.
     */
    public static void putLongByteWiseLittleEndian( long p, long value )
    {
        UnsafeUtil.putByte( p, (byte) value );
        UnsafeUtil.putByte( p + 1, (byte) (value >> 8) );
        UnsafeUtil.putByte( p + 2, (byte) (value >> 16) );
        UnsafeUtil.putByte( p + 3, (byte) (value >> 24) );
        UnsafeUtil.putByte( p + 4, (byte) (value >> 32) );
        UnsafeUtil.putByte( p + 5, (byte) (value >> 40) );
        UnsafeUtil.putByte( p + 6, (byte) (value >> 48) );
        UnsafeUtil.putByte( p + 7, (byte) (value >> 56) );
    }

    /**
     * Define a class but do not make it known to the class loader or system dictionary.
     * @param host the host class
     * @param byteBuffer the code to load
     * @return the defined class
     */
    public static Class<?> defineAnonymousClass( Class<?> host, ByteBuffer byteBuffer )
    {
        if ( byteBuffer.hasArray() )
        {
            return unsafe.defineAnonymousClass( host, byteBuffer.array(), null );
        }
        else
        {
            //buffer not backed by an array, copy data into an array
            byte[] bytes = new byte[byteBuffer.remaining()];
            byteBuffer.get( bytes );
            return unsafe.defineAnonymousClass( host, bytes, null );
        }
    }

    private static final class Allocation
    {
        private final long pointer;
        private final long sizeInBytes;
        private final long boundary;
        private final long freeCounter;
        public volatile boolean freed;

        Allocation( long pointer, long sizeInBytes, long freeCounter )
        {
            this.pointer = pointer;
            this.sizeInBytes = sizeInBytes;
            this.freeCounter = freeCounter;
            this.boundary = pointer + sizeInBytes;
        }

        @Override
        public String toString()
        {
            return format( "Allocation[pointer=%s (%x), size=%s, boundary=%s (%x), free counter=%s]",
                    pointer, pointer, sizeInBytes, boundary, boundary, freeCounter );
        }
    }

    private static final class FreeTrace extends Throwable implements Comparable<FreeTrace>
    {
        private final long pointer;
        private final Allocation allocation;
        private final long id;
        private final long nanoTime;
        private long referenceTime;

        private FreeTrace( long pointer, Allocation allocation, long id )
        {
            this.pointer = pointer;
            this.allocation = allocation;
            this.id = id;
            this.nanoTime = System.nanoTime();
        }

        private boolean contains( long pointer )
        {
            return this.pointer <= pointer && pointer <= this.pointer + allocation.sizeInBytes;
        }

        @Override
        public int compareTo( FreeTrace that )
        {
            return Long.compare( this.id, that.id );
        }

        @Override
        public String getMessage()
        {
            return format( "0x%x of %6d bytes, freed %s µs ago at", pointer, allocation.sizeInBytes, (referenceTime - nanoTime) / 1000 );
        }
    }

}
