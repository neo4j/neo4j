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
package org.neo4j.helpers;

import java.lang.reflect.Array;
import java.util.Arrays;

import static java.util.Arrays.copyOf;

/**
 * Methods "missing" from {@link Arrays} are provided here.
 */
public abstract class ArrayUtil
{
    /**
     * Convert an array to a {@link String}.
     * I can't believe this method is missing from {@link Arrays}.
     *
     * @see Arrays#toString(byte[]) for similar functionality.
     * @deprecated use {@link ObjectUtil#toString(Object)} instead.
     * @param array Array to convert.
     * @return A String representing the array.
     */
    @Deprecated
    public static String toString( Object array )
    {
        assert array.getClass().isArray() : array + " is not an array";
        return ObjectUtil.arrayToString( array );
    }

    public static int hashCode( Object array )
    {
        assert array.getClass().isArray() : array + " is not an array";

        int length = Array.getLength( array ), result = length;
        for ( int i = 0; i < length; i++ )
        {
            result = 31 * result + Array.get( array, i ).hashCode();
        }
        return result;
    }

    public interface ArrayEquality
    {
        boolean typeEquals( Class<?> firstType, Class<?> otherType );

        boolean itemEquals( Object firstArray, Object otherArray );
    }

    public static final ArrayEquality DEFAULT_ARRAY_EQUALITY = new ArrayEquality()
    {
        @Override
        public boolean typeEquals( Class<?> firstType, Class<?> otherType )
        {
            return firstType == otherType;
        }

        @Override
        public boolean itemEquals( Object lhs, Object rhs )
        {
            return lhs == rhs || lhs != null && lhs.equals( rhs );
        }
    };

    public static final ArrayEquality BOXING_AWARE_ARRAY_EQUALITY = new ArrayEquality()
    {
        @Override
        public boolean typeEquals( Class<?> firstType, Class<?> otherType )
        {
            return boxedType( firstType ) == boxedType( otherType );
        }

        private Class<?> boxedType( Class<?> type )
        {
            if ( !type.isPrimitive() )
            {
                return type;
            }

            if ( type.equals( Boolean.TYPE ) )
            {
                return Boolean.class;
            }
            if ( type.equals( Byte.TYPE ) )
            {
                return Byte.class;
            }
            if ( type.equals( Short.TYPE ) )
            {
                return Short.class;
            }
            if ( type.equals( Character.TYPE ) )
            {
                return Character.class;
            }
            if ( type.equals( Integer.TYPE ) )
            {
                return Integer.class;
            }
            if ( type.equals( Long.TYPE ) )
            {
                return Long.class;
            }
            if ( type.equals( Float.TYPE ) )
            {
                return Float.class;
            }
            if ( type.equals( Double.TYPE ) )
            {
                return Double.class;
            }
            throw new IllegalArgumentException( "Oops, forgot to include a primitive type " + type );
        }

        @Override
        public boolean itemEquals( Object lhs, Object rhs )
        {
            return lhs == rhs || lhs != null && lhs.equals( rhs );
        }
    };

    public static boolean equals( Object firstArray, Object otherArray )
    {
        return equals( firstArray, otherArray, DEFAULT_ARRAY_EQUALITY );
    }

    /**
     * Check if two arrays are equal.
     * I also can't believe this method is missing from {@link Arrays}.
     * Both arguments must be arrays of some type.
     *
     * @param firstArray value to compare to the other value
     * @param otherArray value to compare to the first value
     * @param equality equality logic
     * @return Returns {@code true} if the arrays are equal
     *
     * @see Arrays#equals(byte[], byte[]) for similar functionality.
     */
    public static boolean equals( Object firstArray, Object otherArray, ArrayEquality equality )
    {
        assert firstArray.getClass().isArray() : firstArray + " is not an array";
        assert otherArray.getClass().isArray() : otherArray + " is not an array";

        int length;
        if ( equality.typeEquals( firstArray.getClass().getComponentType(), otherArray.getClass().getComponentType() )
                && (length = Array.getLength( firstArray )) == Array.getLength( otherArray ) )
        {
            for ( int i = 0; i < length; i++ )
            {
                if ( !equality.itemEquals( Array.get( firstArray, i ), Array.get( otherArray, i ) ) )
                {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public static Object clone( Object array )
    {
        if ( array instanceof Object[] )
        {
            return ((Object[]) array).clone();
        }
        if ( array instanceof boolean[] )
        {
            return ((boolean[]) array).clone();
        }
        if ( array instanceof byte[] )
        {
            return ((byte[]) array).clone();
        }
        if ( array instanceof short[] )
        {
            return ((short[]) array).clone();
        }
        if ( array instanceof char[] )
        {
            return ((char[]) array).clone();
        }
        if ( array instanceof int[] )
        {
            return ((int[]) array).clone();
        }
        if ( array instanceof long[] )
        {
            return ((long[]) array).clone();
        }
        if ( array instanceof float[] )
        {
            return ((float[]) array).clone();
        }
        if ( array instanceof double[] )
        {
            return ((double[]) array).clone();
        }
        throw new IllegalArgumentException( "Not an array type: " + array.getClass() );
    }

    public static boolean approximatelyEqual( double[] that, double[] other, double tolerance )
    {
        if ( that == other )
        {
            return true;
        }

        if ( ( null == that ) || ( null == other ) )
        {
            return false;
        }

        if ( that.length != other.length )
        {
            return false;
        }

        for ( int i = 0; i < that.length; i++ )
        {
            if ( Math.abs( other[i] - that[i] ) > tolerance )
            {
                return false;
            }
        }

        return true;
    }

    /**
     * Count missing items in an array.
     * The order of items doesn't matter.
     *
     * @param array Array to examine
     * @param contains Items to look for
     * @param <T> The type of the array items
     * @return how many of the items in {@code contains} are missing from {@code array}.
     */
    public static <T> int missing( T[] array, T[] contains )
    {
        int missing = 0;
        for ( T check : contains )
        {
            if ( !contains( array, check ) )
            {
                missing++;
            }
        }
        return missing;
    }

    /**
     * Count items from a different array contained in an array.
     * The order of items doesn't matter.
     *
     * @param array Array to examine
     * @param contains Items to look for
     * @param <T> The type of the array items
     * @return {@code true} if all items in {@code contains} exists in {@code array}, otherwise {@code false}.
     */
    public static <T> boolean containsAll( T[] array, T[] contains )
    {
        for ( T check : contains )
        {
            if ( !contains( array, check ) )
            {
                return false;
            }
        }
        return true;
    }

    /**
     * Check if array contains item.
     *
     * @param array Array to examine
     * @param contains Single item to look for
     * @param <T> The type of the array items
     * @return {@code true} if {@code contains} exists in {@code array}, otherwise {@code false}.
     */
    public static <T> boolean contains( T[] array, T contains )
    {
        return contains( array, array.length, contains );
    }

    /**
     * Check if array contains item.
     *
     * @param array Array to examine
     * @param arrayLength Number of items to check, from the start of the array
     * @param contains Single item to look for
     * @param <T> The type of the array items
     * @return {@code true} if {@code contains} exists in {@code array}, otherwise {@code false}.
     */
    public static <T> boolean contains( T[] array, int arrayLength, T contains )
    {
        for ( int i = 0; i < arrayLength; i++ )
        {
            T item = array[i];
            if ( nullSafeEquals( item, contains ) )
            {
                return true;
            }
        }
        return false;
    }

    /**
     * Compare two items for equality; if both are {@code null} they are regarded as equal.
     *
     * @param first First item to compare
     * @param other Other item to compare
     * @param <T> The type of the items
     * @return {@code true} if {@code first} and {@code other} are both {@code null} or are both equal.
     */
    public static <T> boolean nullSafeEquals( T first, T other )
    {
        return first == null ? first == other : first.equals( other );
    }

    /**
     * Get the union of two arrays.
     * The resulting array will not contain any duplicates.
     *
     * @param first First array
     * @param other Other array
     * @param <T> The type of the arrays
     * @return an array containing the union of {@code first} and {@code other}. Items occurring in
     * both {@code first} and {@code other} will only have of the two in the resulting union.
     */
    public static <T> T[] union( T[] first, T[] other )
    {
        if ( first == null || other == null )
        {
            return first == null ? other : first;
        }

        int missing = missing( first, other );
        if ( missing == 0 )
        {
            return first;
        }

        // An attempt to add the labels as efficiently as possible
        T[] union = copyOf( first, first.length + missing );
        int cursor = first.length;
        for ( T candidate : other )
        {
            if ( !contains( first, candidate ) )
            {
                union[cursor++] = candidate;
                missing--;
            }
        }
        assert missing == 0;
        return union;
    }

    /**
     * Check if provided array is empty
     * @param array - array to check
     * @return true if array is null or empty
     */
    public static boolean isEmpty( Object[] array )
    {
        return (array == null) || (array.length == 0);
    }

    /**
     * Convert an array to a String using a custom delimiter.
     *
     * @param items The array to convert
     * @param delimiter The delimiter to use
     * @param <T> The type of the array
     * @return a {@link String} representation of {@code items} with a custom delimiter in between.
     */
    public static <T> String join( T[] items, String delimiter )
    {
        StringBuilder builder = new StringBuilder();
        for ( int i = 0; i < items.length; i++ )
        {
            builder.append( i > 0 ? delimiter : "" ).append( items[i] );
        }
        return builder.toString();
    }

    /**
     * Create new array with all items converted into a new type using a supplied transformer.
     *
     * @param from original array
     * @param transformer transformer that converts an item from the original to the target type
     * @param toClass target type for items
     * @param <FROM> type of original items
     * @param <TO> type of the converted items
     * @return a new array with all items from {@code from} converted into type {@code toClass}.
     */
    public static <FROM,TO> TO[] map( FROM[] from, org.neo4j.function.Function<FROM,TO> transformer,
            Class<TO> toClass )
    {
        @SuppressWarnings( "unchecked" )
        TO[] result = (TO[]) Array.newInstance( toClass, from.length );
        for ( int i = 0; i < from.length; i++ )
        {
            result[i] = transformer.apply( from[i] );
        }
        return result;
    }

    /**
     * Create an array from a single first item and additional items following it.
     *
     * @param first the item to put first
     * @param additional the additional items to add to the array
     * @param <T> the type of the items
     * @return a concatenated array where {@code first} as the item at index {@code 0} and the additional
     * items following it.
     */
    public static <T> T[] concat( T first, T... additional )
    {
        @SuppressWarnings( "unchecked" )
        T[] result = (T[]) Array.newInstance( additional.getClass().getComponentType(), additional.length+1 );
        result[0] = first;
        System.arraycopy( additional, 0, result, 1, additional.length );
        return result;
    }

    /**
     * @return a concatenated array where {@code first} as the item at index {@code 0} and the additional
     * items following it.
     */
    public static <T> T[] concat( T[] initial, T... additional )
    {
        @SuppressWarnings( "unchecked" )
        T[] result = (T[]) Array.newInstance( additional.getClass().getComponentType(), initial.length+additional.length );
        System.arraycopy( initial, 0, result, 0, initial.length );
        System.arraycopy( additional, 0, result, initial.length, additional.length );
        return result;
    }

    /**
     * @param varargs the items
     * @param <T> the type of the items
     * @return the array version of the vararg argument.
     */
    @SafeVarargs
    public static <T> T[] array( T... varargs )
    {
        return varargs;
    }

    public static <T> int indexOf( T[] array, T item )
    {
        for ( int i = 0; i < array.length; i++ )
        {
            if ( array[i].equals( item ) )
            {
                return i;
            }
        }
        return -1;
    }

    public static <T> T[] without( T[] source, T... toRemove )
    {
        T[] result = source.clone();
        int length = result.length;
        for ( T candidate : toRemove )
        {
            int index = indexOf( result, candidate );
            if ( index != -1 )
            {
                if ( index+1 < length )
                {   // not the last one
                    result[index] = result[length-1];
                }
                length--;
            }
        }
        return length == result.length ? result : Arrays.copyOf( result, length );
    }

    private ArrayUtil()
    {   // No instances allowed
    }
}
