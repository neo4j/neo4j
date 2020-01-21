/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.memory;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;

import static com.sun.jna.Platform.is64Bit;

public final class HeapEstimator
{
    private HeapEstimator()
    {
    }

    /**
     * Number of bytes this JVM uses to represent an object reference.
     */
    public static final int OBJECT_REFERENCE_BYTES;

    /**
     * Number of bytes to represent an object header (no fields, no alignments).
     */
    public static final int OBJECT_HEADER_BYTES;

    /**
     * Number of bytes to represent an array header (no content, but with alignments).
     */
    public static final int ARRAY_HEADER_BYTES;

    /**
     * A constant specifying the object alignment boundary inside the JVM. Objects will always take a full multiple of this constant, possibly wasting some
     * space.
     */
    public static final int OBJECT_ALIGNMENT_BYTES;

    /**
     * Sizes of primitive classes.
     */
    private static final Map<Class<?>,Integer> PRIMITIVE_SIZES;
    static
    {
        Map<Class<?>,Integer> primitiveSizesMap = new IdentityHashMap<>( 8 );
        primitiveSizesMap.put( boolean.class, 1 );
        primitiveSizesMap.put( byte.class, 1 );
        primitiveSizesMap.put( char.class, Character.BYTES );
        primitiveSizesMap.put( short.class, Short.BYTES );
        primitiveSizesMap.put( int.class, Integer.BYTES );
        primitiveSizesMap.put( float.class, Float.BYTES );
        primitiveSizesMap.put( double.class, Double.BYTES );
        primitiveSizesMap.put( long.class, Long.BYTES );
        PRIMITIVE_SIZES = Collections.unmodifiableMap( primitiveSizesMap );
    }

    private static final int LONG_SIZE;
    private static final int STRING_SIZE;

    static
    {
        if ( is64Bit() )
        {
            OBJECT_ALIGNMENT_BYTES = RuntimeInternals.OBJECT_ALIGNMENT;
            OBJECT_REFERENCE_BYTES = RuntimeInternals.COMPRESSED_OOPS ? 4 : 8;
            OBJECT_HEADER_BYTES = RuntimeInternals.HEADER_SIZE;
            ARRAY_HEADER_BYTES = (int) alignObjectSize( OBJECT_HEADER_BYTES + Integer.BYTES );
        }
        else
        {
            // Values are fixed for 32 bit JVM
            OBJECT_ALIGNMENT_BYTES = 8;
            OBJECT_REFERENCE_BYTES = 4;
            OBJECT_HEADER_BYTES = 8;
            ARRAY_HEADER_BYTES = OBJECT_HEADER_BYTES + Integer.BYTES;
        }

        LONG_SIZE = (int) shallowSizeOfInstance( Long.class );
        STRING_SIZE = (int) shallowSizeOfInstance( String.class );

        if ( RuntimeInternals.DEBUG_ESTIMATIONS )
        {
            System.err.println( String.format( "### %s static values: ###%n" +
                            "  NUM_BYTES_OBJECT_ALIGNMENT=%d%n" +
                            "  NUM_BYTES_OBJECT_REF=%d%n" +
                            "  NUM_BYTES_OBJECT_HEADER=%d%n" +
                            "  NUM_BYTES_ARRAY_HEADER=%d%n" +
                            "  LONG_SIZE=%d%n" +
                            "  STRING_SIZE=%d%n", HeapEstimator.class.getName(), OBJECT_ALIGNMENT_BYTES, OBJECT_REFERENCE_BYTES, OBJECT_HEADER_BYTES,
                    ARRAY_HEADER_BYTES, LONG_SIZE, STRING_SIZE ) );
        }
    }

    /**
     * Aligns an object size to be the next multiple of {@link #OBJECT_ALIGNMENT_BYTES}.
     */
    public static long alignObjectSize( long size )
    {
        return (size + OBJECT_ALIGNMENT_BYTES - 1) & -OBJECT_ALIGNMENT_BYTES;
    }

    /**
     * Return the size of the provided {@link Long} object, returning 0 if it is cached by the JVM and its shallow size otherwise.
     */
    public static long sizeOf( Long value )
    {
        if ( value >= RuntimeInternals.LONG_CACHE_MIN_VALUE && value <= RuntimeInternals.LONG_CACHE_MAX_VALUE )
        {
            return 0;
        }
        return LONG_SIZE;
    }

    /**
     * Returns the size in bytes of the byte[] object.
     */
    public static long sizeOf( byte[] arr )
    {
        return alignObjectSize( (long) ARRAY_HEADER_BYTES + arr.length );
    }

    /**
     * Returns the size in bytes of the boolean[] object.
     */
    public static long sizeOf( boolean[] arr )
    {
        return alignObjectSize( (long) ARRAY_HEADER_BYTES + arr.length );
    }

    /**
     * Returns the size in bytes of the char[] object.
     */
    public static long sizeOf( char[] arr )
    {
        return alignObjectSize( (long) ARRAY_HEADER_BYTES + (long) Character.BYTES * arr.length );
    }

    /**
     * Returns the size in bytes of the short[] object.
     */
    public static long sizeOf( short[] arr )
    {
        return alignObjectSize( (long) ARRAY_HEADER_BYTES + (long) Short.BYTES * arr.length );
    }

    /**
     * Returns the size in bytes of the int[] object.
     */
    public static long sizeOf( int[] arr )
    {
        return alignObjectSize( (long) ARRAY_HEADER_BYTES + (long) Integer.BYTES * arr.length );
    }

    /**
     * Returns the size in bytes of the float[] object.
     */
    public static long sizeOf( float[] arr )
    {
        return alignObjectSize( (long) ARRAY_HEADER_BYTES + (long) Float.BYTES * arr.length );
    }

    /**
     * Returns the size in bytes of the long[] object.
     */
    public static long sizeOf( long[] arr )
    {
        return alignObjectSize( (long) ARRAY_HEADER_BYTES + (long) Long.BYTES * arr.length );
    }

    /**
     * Returns the size in bytes of the double[] object.
     */
    public static long sizeOf( double[] arr )
    {
        return alignObjectSize( (long) ARRAY_HEADER_BYTES + (long) Double.BYTES * arr.length );
    }

    /**
     * Returns the size in bytes of the String[] object.
     */
    public static long sizeOf( String[] arr )
    {
        long size = shallowSizeOf( arr );
        for ( String s : arr )
        {
            if ( s == null )
            {
                continue;
            }
            size += sizeOf( s );
        }
        return size;
    }

    /**
     * Recurse only into immediate descendants.
     */
    private static final int MAX_DEPTH = 1;

    /**
     * Returns the size in bytes of a Map object, including sizes of its keys and values, supplying default object size when object type is not well known. This
     * method recurses up to {@link #MAX_DEPTH}.
     */
    public static long sizeOfMap( Map<?,?> map, long defSize )
    {
        return sizeOfMap( map, 0, defSize );
    }

    private static long sizeOfMap( Map<?,?> map, int depth, long defSize )
    {
        if ( map == null )
        {
            return 0;
        }
        long size = shallowSizeOf( map );
        if ( depth > MAX_DEPTH )
        {
            return size;
        }
        long sizeOfEntry = -1;
        for ( Map.Entry<?,?> entry : map.entrySet() )
        {
            if ( sizeOfEntry == -1 )
            {
                sizeOfEntry = shallowSizeOf( entry );
            }
            size += sizeOfEntry;
            size += sizeOfObject( entry.getKey(), depth, defSize );
            size += sizeOfObject( entry.getValue(), depth, defSize );
        }
        return alignObjectSize( size );
    }

    /**
     * Returns the size in bytes of a Collection object, including sizes of its values, supplying default object size when object type is not well known. This
     * method recurses up to {@link #MAX_DEPTH}.
     */
    public static long sizeOfCollection( Collection<?> collection, long defSize )
    {
        return sizeOfCollection( collection, 0, defSize );
    }

    private static long sizeOfCollection( Collection<?> collection, int depth, long defSize )
    {
        if ( collection == null )
        {
            return 0;
        }
        long size = shallowSizeOf( collection );
        if ( depth > MAX_DEPTH )
        {
            return size;
        }
        // assume array-backed collection and add per-object references
        size += ARRAY_HEADER_BYTES + collection.size() * OBJECT_REFERENCE_BYTES;
        for ( Object o : collection )
        {
            size += sizeOfObject( o, depth, defSize );
        }
        return alignObjectSize( size );
    }

    /**
     * Best effort attempt to estimate the size in bytes of an undetermined object. Known types will be estimated according to their formulas, and all other
     * object sizes will be estimated using {@link #shallowSizeOf(Object)}, or using the supplied <code>defSize</code> parameter if its value is greater than
     * 0.
     */
    public static long sizeOfObject( Object o, long defSize )
    {
        return sizeOfObject( o, 0, defSize );
    }

    private static long sizeOfObject( Object o, int depth, long defSize )
    {
        if ( o == null )
        {
            return 0;
        }
        long size;
        if ( o instanceof String )
        {
            size = sizeOf( (String) o );
        }
        else if ( o instanceof boolean[] )
        {
            size = sizeOf( (boolean[]) o );
        }
        else if ( o instanceof byte[] )
        {
            size = sizeOf( (byte[]) o );
        }
        else if ( o instanceof char[] )
        {
            size = sizeOf( (char[]) o );
        }
        else if ( o instanceof double[] )
        {
            size = sizeOf( (double[]) o );
        }
        else if ( o instanceof float[] )
        {
            size = sizeOf( (float[]) o );
        }
        else if ( o instanceof int[] )
        {
            size = sizeOf( (int[]) o );
        }
        else if ( o instanceof Long )
        {
            size = sizeOf( (Long) o );
        }
        else if ( o instanceof long[] )
        {
            size = sizeOf( (long[]) o );
        }
        else if ( o instanceof short[] )
        {
            size = sizeOf( (short[]) o );
        }
        else if ( o instanceof String[] )
        {
            size = sizeOf( (String[]) o );
        }
        else if ( o instanceof Map )
        {
            size = sizeOfMap( (Map) o, ++depth, defSize );
        }
        else if ( o instanceof Collection )
        {
            size = sizeOfCollection( (Collection) o, ++depth, defSize );
        }
        else
        {
            if ( defSize > 0 )
            {
                size = defSize;
            }
            else
            {
                size = shallowSizeOf( o );
            }
        }
        return size;
    }

    /**
     * Returns the size in bytes of the String object.
     */
    public static long sizeOf( String s )
    {
        if ( s == null )
        {
            return 0;
        }
        // may not be true in Java 9+ and CompactStrings - but we have no way to determine this

        // char[] + hashCode
        long size = STRING_SIZE + (long) ARRAY_HEADER_BYTES + (long) Character.BYTES * s.length();
        return alignObjectSize( size );
    }

    /**
     * Returns the shallow size in bytes of the Object[] object.
     */
    public static long shallowSizeOf( Object[] arr )
    {
        return alignObjectSize( (long) ARRAY_HEADER_BYTES + (long) OBJECT_REFERENCE_BYTES * arr.length );
    }

    /**
     * Estimates a "shallow" memory usage of the given object. For arrays, this will be the memory taken by array storage (no subreferences will be followed).
     * For objects, this will be the memory taken by the fields.
     * <p>
     * JVM object alignments are also applied.
     */
    public static long shallowSizeOf( Object obj )
    {
        if ( obj == null )
        {
            return 0;
        }
        final Class<?> clz = obj.getClass();
        if ( clz.isArray() )
        {
            return shallowSizeOfArray( obj );
        }
        else
        {
            return shallowSizeOfInstance( clz );
        }
    }

    /**
     * Returns the shallow instance size in bytes an instance of the given class would occupy. This works with all conventional classes and primitive types, but
     * not with arrays (the size then depends on the number of elements and varies from object to object).
     *
     * @throws IllegalArgumentException if {@code clazz} is an array class.
     * @see #shallowSizeOf(Object)
     */
    public static long shallowSizeOfInstance( Class<?> clazz )
    {
        if ( clazz.isArray() )
        {
            throw new IllegalArgumentException( "This method does not work with array classes." );
        }
        if ( clazz.isPrimitive() )
        {
            return PRIMITIVE_SIZES.get( clazz );
        }

        long size = OBJECT_HEADER_BYTES;

        // Walk type hierarchy
        for ( ; clazz != null; clazz = clazz.getSuperclass() )
        {
            final Field[] fields = AccessController.doPrivileged( (PrivilegedAction<Field[]>) clazz::getDeclaredFields );
            for ( Field f : fields )
            {
                if ( !Modifier.isStatic( f.getModifiers() ) )
                {
                    size = adjustForField( size, f );
                }
            }
        }
        return alignObjectSize( size );
    }

    /**
     * Return shallow size of any <code>array</code>.
     */
    private static long shallowSizeOfArray( Object array )
    {
        long size = ARRAY_HEADER_BYTES;
        final int len = Array.getLength( array );
        if ( len > 0 )
        {
            Class<?> arrayElementClazz = array.getClass().getComponentType();
            if ( arrayElementClazz.isPrimitive() )
            {
                size += (long) len * PRIMITIVE_SIZES.get( arrayElementClazz );
            }
            else
            {
                size += (long) OBJECT_REFERENCE_BYTES * len;
            }
        }
        return alignObjectSize( size );
    }

    /**
     * This method returns the maximum representation size of an object. <code>sizeSoFar</code> is the object's size measured so far. <code>f</code> is the
     * field being probed.
     *
     * <p>The returned offset will be the maximum of whatever was measured so far and
     * <code>f</code> field's offset and representation size (unaligned).
     */
    static long adjustForField( long sizeSoFar, final Field f )
    {
        final Class<?> type = f.getType();
        final int fsize = type.isPrimitive() ? PRIMITIVE_SIZES.get( type ) : OBJECT_REFERENCE_BYTES;
        // TODO: No alignments based on field type/ subclass fields alignments?
        return sizeSoFar + fsize;
    }
}
