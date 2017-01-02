/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.codegen;

import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.helpers.MathUtil;

/**
 * Helper class for dealing with equivalence an hash code in compiled code.
 *
 * Note this class contains a lot of duplicated code in order to minimize boxing.
 */
public final class CompiledEquivalenceUtils
{
    /**
     * Do not instantiate this class
     */
    private CompiledEquivalenceUtils()
    {
        throw new UnsupportedOperationException(  );
    }

    /**
     * Checks if two objects are equal according to Cypher semantics
     * @param lhs the left-hand side to check
     * @param rhs the right-hand sid to check
     * @return <tt>true</tt> if the two objects are equal otherwise <tt>false</tt>
     */
    @SuppressWarnings( "unchecked" )
    public static boolean equals( Object lhs, Object rhs )
    {
        if ( lhs == rhs )
        {
            return true;
        }
        else if ( lhs == null || rhs == null )
        {
            return false;
        }
        //if floats compare float values if integer types,
        //compare long values
        else if ( lhs instanceof Number && rhs instanceof Number )
        {
            if ( lhs instanceof Double && rhs instanceof Float )
            {
                return mixedFloatEquality( (Float) rhs, (Double) lhs );
            }
            else if ( lhs instanceof Float && rhs instanceof Double )
            {
                return mixedFloatEquality( (Float) lhs, (Double) rhs );
            }
            else if ( (lhs instanceof Double || lhs instanceof Float)
                      && (rhs instanceof Double || rhs instanceof Float) )
            {
                double left = ((Number) lhs).doubleValue();
                double right = ((Number) rhs).doubleValue();
                return left == right;
            }
            else if ( (lhs instanceof Double || lhs instanceof Float) )
            {
                double left = ((Number) lhs).doubleValue();
                long right = ((Number) rhs).longValue();
                return MathUtil.numbersEqual( left, right );
            }
            else if ( (rhs instanceof Double || rhs instanceof Float) )
            {
                long left = ((Number) lhs).longValue();
                double right = ((Number) rhs).doubleValue();
                return MathUtil.numbersEqual( right, left );
            }

            //everything else is a long from cyphers point-of-view
            long left = ((Number) lhs).longValue();
            long right = ((Number) rhs).longValue();
            return left == right;
        }
        else if ( lhs instanceof Character && rhs instanceof String )
        {
            return lhs.toString().equals( rhs );
        }
        else if ( lhs instanceof String && rhs instanceof Character )
        {
            return lhs.equals( rhs.toString() );
        }
        else if ( lhs.getClass().isArray() && rhs.getClass().isArray() )
        {
            int length = Array.getLength( lhs );
            if ( length != Array.getLength( rhs ) )
            {
                return false;
            }
            for ( int i = 0; i < length; i++ )
            {
                if ( !equals( Array.get( lhs, i ), Array.get( rhs, i ) ) )
                {
                    return false;
                }
            }
            return true;
        }
        else if ( lhs.getClass().isArray() && rhs instanceof List<?> )
        {
            return compareArrayAndList( lhs, (List<?>) rhs );
        }
        else if ( lhs instanceof List<?> && rhs.getClass().isArray() )
        {
            return compareArrayAndList( rhs, (List<?>) lhs );
        }
        else if ( lhs instanceof List<?> && rhs instanceof List<?> )
        {
            List<?> lhsList = (List<?>) lhs;
            List<?> rhsList = (List<?>) rhs;
            if ( lhsList.size() != rhsList.size() )
            {
                return false;
            }
            Iterator<?> lhsIterator = lhsList.iterator();
            Iterator<?> rhsIterator = rhsList.iterator();
            while ( lhsIterator.hasNext() )
            {
                if ( !equals( lhsIterator.next(), rhsIterator.next() ) )
                {
                    return false;
                }
            }
            return true;
        }
        else if (lhs instanceof Map<?,?> && rhs instanceof Map<?,?>)
        {
            Map<String, Object> rMap = (Map<String, Object>) rhs;
            Map<String, Object> lMap = (Map<String, Object>) lhs;
            if (rMap.size() != lMap.size())
            {
                return false;
            }
            for ( Map.Entry<String,Object> e : rMap.entrySet() )
            {
                String key = e.getKey();
                Object value = e.getValue();
                if ( value == null )
                {
                    if ( !(lMap.get( key ) == null && lMap.containsKey( key )) )
                    {
                        return false;
                    }
                }
                else
                {
                    if ( !equals( value, lMap.get( key ) ) )
                    {
                        return false;
                    }
                }
            }
            return true;
        }

        //for everything else call equals
        return lhs.equals( rhs );
    }

    /**
     * Calculates hash code of a given object
     * @param element the element to calculate hash code for
     * @return the hash code of the given object
     */
    @SuppressWarnings( "unchecked" )
    public static int hashCode( Object element )
    {
        if ( element == null )
        {
            return 0;
        }
        else if ( element instanceof Number )
        {
            return hashCode( ((Number) element).longValue() );
        }
        else if ( element instanceof Character )
        {
            return hashCode( (char) element );
        }
        else if ( element instanceof Boolean )
        {
            return hashCode( (boolean) element );
        }
        else if ( element instanceof List<?> )
        {
            return hashCode( (List<?>) element );
        }
        else if ( element instanceof Map<?,?> )
        {
            return hashCode( ((Map<String,Object>) element) );
        }
        else if ( element instanceof Object[] )
        {
            return hashCode( (Object[]) element );
        }
        else if ( element instanceof byte[] )
        {
            return hashCode( (byte[]) element );
        }
        else if ( element instanceof short[] )
        {
            return hashCode( (short[]) element );
        }
        else if ( element instanceof int[] )
        {
            return hashCode( (int[]) element );
        }
        else if ( element instanceof long[] )
        {
            return hashCode( (long[]) element );
        }
        else if ( element instanceof char[] )
        {
            return hashCode( (char[]) element );
        }
        else if ( element instanceof float[] )
        {
            return hashCode( (float[]) element );
        }
        else if ( element instanceof double[] )
        {
            return hashCode( (double[]) element );
        }
        else if ( element instanceof boolean[] )
        {
            return hashCode( (boolean[]) element );
        }
        else
        {
            return element.hashCode();
        }
    }

    /**
     * Calculate hash code of a map
     * @param map the element to calculate hash code for
     * @return the hash code of the given map
     */
    public static int hashCode( Map<String,Object> map )
    {
        int h = 0;
        for ( Map.Entry<String,Object> next : map.entrySet() )
        {
            String k = next.getKey();
            Object v = next.getValue();
            h += (k == null ? 0 : k.hashCode()) ^ (v == null ? 0 : hashCode( v ));
        }
        return h;
    }

    /**
     * Calculate hash code of a long value
     * @param value the value to compute hash code for
     * @return the hash code of the given value
     */
    public static int hashCode( long value )
    {
        return Long.hashCode( value );
    }

    /**
     * Calculate hash code of a boolean value
     * @param value the value to compute hash code for
     * @return the hash code of the given value
     */
    public static int hashCode( boolean value )
    {
        return Boolean.hashCode( value );
    }

    /**
     * Calculate hash code of a char value
     * @param value the value to compute hash code for
     * @return the hash code of the given value
     */
    public static int hashCode( char value )
    {
        return Character.hashCode( value );
    }

    /**
     * Calculate hash code of a char[] value
     * @param array the value to compute hash code for
     * @return the hash code of the given value
     */
    public static int hashCode( char[] array )
    {
        int len = array.length;
        switch ( len )
        {
        case 0:
            return 42;
        case 1:
            return hashCode( array[0] );
        case 2:
            return 31 * hashCode( array[0] ) + hashCode( array[1] );
        case 3:
            return (31 * hashCode( array[0] ) + hashCode( array[1] )) * 31  + hashCode( array[2] );
        default:
            return len * (31 * hashCode( array[0] ) + hashCode( array[len / 2] ) * 31 + hashCode( array[len - 1] ));
        }
    }

    /**
     * Calculate hash code of a list value
     * @param list the value to compute hash code for
     * @return the hash code of the given value
     */
    public static int hashCode( List<?> list )
    {
        int len = list.size();
        switch ( len )
        {
        case 0:
            return 42;
        case 1:
            return hashCode( list.get( 0 ) );
        case 2:
            return 31 * hashCode( list.get( 0 ) ) + hashCode( list.get( 1 ) );
        case 3:
            return (31 * hashCode( list.get( 0 ) ) + hashCode( list.get( 1 ) )) * 31  + hashCode( list.get( 2 ) );
        default:
            return len * (31 * hashCode( list.get( 0 ) ) + hashCode( list.get( len / 2 ) ) * 31 +
                          hashCode( list.get( len - 1 ) ));
        }
    }

    /**
     * Calculate hash code of a Object[] value
     * @param array the value to compute hash code for
     * @return the hash code of the given value
     */
    public static int hashCode( Object[] array )
    {
        int len = array.length;
        switch ( len )
        {
        case 0:
            return 42;
        case 1:
            return hashCode( array[0] );
        case 2:
            return 31 * hashCode( array[0] ) + hashCode( array[1] );
        case 3:
            return (31 * hashCode( array[0] ) + hashCode( array[1] )) * 31  + hashCode( array[2] );
        default:
            return len * (31 * hashCode( array[0] ) + hashCode( array[len / 2] ) * 31 + hashCode( array[len - 1] ));
        }
    }

    /**
     * Calculate hash code of a byte[] value
     * @param array the value to compute hash code for
     * @return the hash code of the given value
     */
    public static int hashCode( byte[] array )
    {
        int len = array.length;
        switch ( len )
        {
        case 0:
            return 42;
        case 1:
            return hashCode( array[0] );
        case 2:
            return 31 * hashCode( array[0] ) + hashCode( array[1] );
        case 3:
            return (31 * hashCode( array[0] ) + hashCode( array[1] )) * 31  + hashCode( array[2] );
        default:
            return len * (31 * hashCode( array[0] ) + hashCode( array[len / 2] ) * 31 + hashCode( array[len - 1] ));
        }
    }

    /**
     * Calculate hash code of a short[] value
     * @param array the value to compute hash code for
     * @return the hash code of the given value
     */
    public static int hashCode( short[] array )
    {
        int len = array.length;
        switch ( len )
        {
        case 0:
            return 42;
        case 1:
            return hashCode( array[0] );
        case 2:
            return 31 * hashCode( array[0] ) + hashCode( array[1] );
        case 3:
            return (31 * hashCode( array[0] ) + hashCode( array[1] )) * 31  + hashCode( array[2] );
        default:
            return len * (31 * hashCode( array[0] ) + hashCode( array[len / 2] ) * 31 + hashCode( array[len - 1] ));
        }
    }

    /**
     * Calculate hash code of a int[] value
     * @param array the value to compute hash code for
     * @return the hash code of the given value
     */
    public static int hashCode( int[] array )
    {
        int len = array.length;
        switch ( len )
        {
        case 0:
            return 42;
        case 1:
            return hashCode( array[0] );
        case 2:
            return 31 * hashCode( array[0] ) + hashCode( array[1] );
        case 3:
            return (31 * hashCode( array[0] ) + hashCode( array[1] )) * 31  + hashCode( array[2] );
        default:
            return len * (31 * hashCode( array[0] ) + hashCode( array[len / 2] ) * 31 + hashCode( array[len - 1] ));
        }
    }

    /**
     * Calculate hash code of a long[] value
     * @param array the value to compute hash code for
     * @return the hash code of the given value
     */
    public static int hashCode( long[] array )
    {
        int len = array.length;
        switch ( len )
        {
        case 0:
            return 42;
        case 1:
            return hashCode( array[0] );
        case 2:
            return 31 * hashCode( array[0] ) + hashCode( array[1] );
        case 3:
            return (31 * hashCode( array[0] ) + hashCode( array[1] )) * 31  + hashCode( array[2] );
        default:
            return len * (31 * hashCode( array[0] ) + hashCode( array[len / 2] ) * 31 + hashCode( array[len - 1] ));
        }
    }

    /**
     * Calculate hash code of a float[] value
     * @param array the value to compute hash code for
     * @return the hash code of the given value
     */
    public static int hashCode( float[] array )
    {
        int len = array.length;
        switch ( len )
        {
        case 0:
            return 42;
        case 1:
            return hashCode( (long) array[0] );
        case 2:
            return 31 * hashCode( (long) array[0] ) + hashCode( (long) array[1] );
        case 3:
            return (31 * hashCode( (long) array[0] ) + hashCode( (long) array[1] )) * 31  + hashCode( (long) array[2] );
        default:
            return len * (31 * hashCode( (long) array[0] ) + hashCode( (long) array[len / 2] ) * 31 +
                          hashCode( (long) array[len - 1] ));
        }
    }

    /**
     * Calculate hash code of a double[] value
     * @param array the value to compute hash code for
     * @return the hash code of the given value
     */
    public static int hashCode( double[] array )
    {
        int len = array.length;
        switch ( len )
        {
        case 0:
            return 42;
        case 1:
            return hashCode( (long) array[0] );
        case 2:
            return 31 * hashCode( (long) array[0] ) + hashCode( (long) array[1] );
        case 3:
            return (31 * hashCode( (long) array[0] ) + hashCode( (long) array[1] )) * 31  + hashCode( (long) array[2] );
        default:
            return len * (31 * hashCode( (long) array[0] ) + hashCode( (long) array[len / 2] ) * 31 +
                          hashCode( (long) array[len - 1] ));
        }
    }

    /**
     * Calculate hash code of a boolean[] value
     * @param array the value to compute hash code for
     * @return the hash code of the given value
     */
    public static int hashCode( boolean[] array )
    {
        int len = array.length;
        switch ( len )
        {
        case 0:
            return 42;
        case 1:
            return hashCode( array[0] );
        case 2:
            return 31 * hashCode( array[0] ) + hashCode( array[1] );
        case 3:
            return (31 * hashCode( array[0] ) + hashCode( array[1] )) * 31  + hashCode( array[2] );
        default:
            return len * (31 * hashCode( array[0] ) + hashCode( array[len / 2] ) * 31 + hashCode( array[len - 1] ));
        }
    }

    private static Boolean compareArrayAndList( Object array, List<?> list )
    {
        int length = Array.getLength( array );
        if ( length != list.size() )
        {
            return false;
        }

        int i = 0;
        for ( Object o : list )
        {
            if ( !equals( o, Array.get( array, i++ ) ) )
            {
                return false;
            }
        }
        return true;
    }

    private static boolean mixedFloatEquality( Float a, Double b )
    {
        return a.doubleValue() == b || (
                (long) Math.rint( a.doubleValue() ) == b.longValue() &&
                (long) Math.rint( b ) == a.longValue());
    }
}

