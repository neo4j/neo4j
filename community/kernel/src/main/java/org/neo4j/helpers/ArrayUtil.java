/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

/**
 * Methods "missing" from {@link Arrays} are provided here.
 */
public abstract class ArrayUtil
{
    /**
     * I can't believe this method is missing from {@link Arrays}.
     * @see Arrays#toString(byte[]) for similar functionality.
     */
    public static String toString( Object array )
    {
        assert array.getClass().isArray() : array + " is not an array";

        StringBuilder result = new StringBuilder();
        String separator = "[";
        for ( int size = Array.getLength( array ), i = 0; i < size; i++ )
        {
            result.append( separator ).append( Array.get( array, i ) );
            separator = ", ";
        }
        return result.append( ']' ).toString();
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

    public static boolean equals( Object firstArray, Object otherArray )
    {
        return equals( firstArray, otherArray, DEFAULT_ARRAY_EQUALITY );
    }

    /**
     * I also can't believe this method is missing from {@link Arrays}.
     * Both arguments must be arrays of some type.
     *
     * @param firstArray value to compare to the other value
     * @param otherArray value to compare to the first value
     * @param equality equality logic
     *
     * @see Arrays#equals(byte[], byte[]) for similar functionality.
     */
    public static boolean equals( Object firstArray, Object otherArray, ArrayEquality equality )
    {
        assert firstArray.getClass().isArray() : firstArray + " is not an array";
        assert otherArray.getClass().isArray() : otherArray + " is not an array";

        int length;
        if ( equality.typeEquals( firstArray.getClass(), otherArray.getClass() )
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

    private ArrayUtil()
    {   // No instances allowed
    }
}
