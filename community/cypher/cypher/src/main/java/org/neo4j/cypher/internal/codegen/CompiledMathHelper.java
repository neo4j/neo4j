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
package org.neo4j.cypher.internal.codegen;

import org.neo4j.cypher.internal.frontend.v2_3.ArithmeticException;
import org.neo4j.cypher.internal.frontend.v2_3.CypherTypeException;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

/**
 * This is a helper class used by compiled plans for doing basic math operations
 */
public final class CompiledMathHelper
{
    private static final double EPSILON = Math.pow( 1, -10 );

    /**
     * Do not instantiate this class
     */
    private CompiledMathHelper()
    {
    }

    /**
     * Utility function for doing addition
     */
    public static Object add( Object lhs, Object rhs )
    {
        if ( lhs == null || rhs == null )
        {
            return null;
        }

        if ( lhs instanceof String || rhs instanceof String )
        {
            return String.valueOf( lhs ) + String.valueOf( rhs );
        }

        //List addition
        if ( lhs instanceof List<?> && rhs instanceof List<?> )
        {
            List<?> lhsList = (List<?>) lhs;
            List<?> rhsList = (List<?>) rhs;
            List<Object> result = new ArrayList<>( lhsList.size() + rhsList.size() );
            result.addAll( lhsList );
            result.addAll( rhsList );
            return result;
        }
        else if ( lhs instanceof List<?> )
        {
            List<?> lhsList = (List<?>) lhs;
            List<Object> result = new ArrayList<>( lhsList.size() + 1 );
            result.addAll( lhsList );
            result.add( rhs );
            return result;
        }
        else if ( rhs instanceof List<?> )
        {
            List<?> rhsList = (List<?>) rhs;
            List<Object> result = new ArrayList<>( rhsList.size() + 1 );
            result.add( lhs );
            result.addAll( rhsList );
            return result;
        }

        // array addition
        Class<?> lhsClass = lhs.getClass();
        Class<?> rhsClass = rhs.getClass();
        if ( lhsClass.isArray() && rhsClass.isArray() )
        {
            return addArrays( lhs, rhs );
        }
        else if ( lhsClass.isArray() )
        {
            return addArrayWithObject( lhs, rhs );
        }
        else if ( rhsClass.isArray() )
        {
            return addObjectWithArray( lhs, rhs );
        }

        if ( lhs instanceof Number && rhs instanceof Number )
        {
            if ( lhs instanceof Double || rhs instanceof Double ||
                 lhs instanceof Float || rhs instanceof Float )
            {
                return ((Number) lhs).doubleValue() + ((Number) rhs).doubleValue();
            }
            if ( lhs instanceof Long || rhs instanceof Long ||
                 lhs instanceof Integer || rhs instanceof Integer ||
                 lhs instanceof Short || rhs instanceof Short ||
                 lhs instanceof Byte || rhs instanceof Byte )
            {
                return ((Number) lhs).longValue() + ((Number) rhs).longValue();
            }
            // other numbers we cannot add
        }

        throw new CypherTypeException( "Cannot add " + lhs.getClass().getSimpleName() +
                                       " and " + rhs.getClass().getSimpleName(), null );
    }

    public static Object subtract( Object lhs, Object rhs )
    {
        if ( lhs == null || rhs == null )
        {
            return null;
        }

        if ( lhs instanceof Number && rhs instanceof Number )
        {
            if ( lhs instanceof Double || rhs instanceof Double ||
                 lhs instanceof Float || rhs instanceof Float )
            {
                return ((Number) lhs).doubleValue() - ((Number) rhs).doubleValue();
            }
            if ( lhs instanceof Long || rhs instanceof Long ||
                 lhs instanceof Integer || rhs instanceof Integer ||
                 lhs instanceof Short || rhs instanceof Short ||
                 lhs instanceof Byte || rhs instanceof Byte )
            {
                return ((Number) lhs).longValue() - ((Number) rhs).longValue();
            }
            // other numbers we cannot subtract
        }

        throw new CypherTypeException( "Cannot add " + lhs.getClass().getSimpleName() +
                                       " and " + rhs.getClass().getSimpleName(), null );
    }

    public static Object multiply( Object lhs, Object rhs )
    {
        if ( lhs == null || rhs == null )
        {
            return null;
        }

        if ( lhs instanceof Number && rhs instanceof Number )
        {
            if ( lhs instanceof Double || rhs instanceof Double ||
                 lhs instanceof Float || rhs instanceof Float )
            {
                return ((Number) lhs).doubleValue() * ((Number) rhs).doubleValue();
            }
            if ( lhs instanceof Long || rhs instanceof Long ||
                 lhs instanceof Integer || rhs instanceof Integer ||
                 lhs instanceof Short || rhs instanceof Short ||
                 lhs instanceof Byte || rhs instanceof Byte )
            {
                return ((Number) lhs).longValue() * ((Number) rhs).longValue();
            }
            // other numbers we cannot multiply
        }

        throw new CypherTypeException( "Cannot multiply " + lhs.getClass().getSimpleName() +
                                       " and " + rhs.getClass().getSimpleName(), null );
    }

    public static Object divide( Object lhs, Object rhs )
    {
        if ( lhs == null || rhs == null )
        {
            return null;
        }

        if ( lhs instanceof Number && rhs instanceof Number )
        {
            if ( lhs instanceof Double || rhs instanceof Double ||
                 lhs instanceof Float || rhs instanceof Float )
            {
                double left = ((Number) lhs).doubleValue();
                double right = ((Number) rhs).doubleValue();
                if ( Math.abs( right ) < EPSILON )
                {
                    throw new ArithmeticException( "/ by zero", null );
                }
                return left / right;
            }
            if ( lhs instanceof Long || rhs instanceof Long ||
                 lhs instanceof Integer || rhs instanceof Integer ||
                 lhs instanceof Short || rhs instanceof Short ||
                 lhs instanceof Byte || rhs instanceof Byte )
            {
                long left = ((Number) lhs).longValue();
                long right = ((Number) rhs).longValue();
                if ( right == 0 )
                {
                    throw new ArithmeticException( "/ by zero", null );
                }
                return left / right;
            }
            // other numbers we cannot divide
        }

        throw new CypherTypeException( "Cannot divide " + lhs.getClass().getSimpleName() +
                                       " and " + rhs.getClass().getSimpleName(), null );
    }

    public static Object modulo( Object lhs, Object rhs )
    {
        if ( lhs == null || rhs == null )
        {
            return null;
        }

        if ( lhs instanceof Number && rhs instanceof Number )
        {
            return ((Number) lhs).doubleValue() % ((Number) rhs).doubleValue();
        }

        throw new CypherTypeException( "Cannot modulo " + lhs.getClass().getSimpleName() +
                                       " and " + rhs.getClass().getSimpleName(), null );
    }

    public static int transformToInt( Object value )
    {
        if ( value == null )
        {
            throw new CypherTypeException( "Expected a numeric value but got null", null );
        }
        if ( value instanceof Number )
        {
            Number number = (Number) value;
            if ( number.longValue() > Integer.MAX_VALUE )
            {
                throw new CypherTypeException( value.toString() + " is too large to cast to an int32", null );
            }
            return number.intValue();
        }
        throw new CypherTypeException( String.format( "Expected a numeric value but got %s", value.toString() ), null );
    }

    /**
     * Both a1 and a2 must be arrays
     */
    private static Object addArrays( Object a1, Object a2 )
    {
        int l1 = Array.getLength( a1 );
        int l2 = Array.getLength( a2 );
        Object[] ret = new Object[l1 + l2];
        for ( int i = 0; i < l1; i++ )
        {
            ret[i] = Array.get( a1, i );
        }
        for ( int i = 0; i < l2; i++ )
        {
            ret[l1 + i] = Array.get( a2, i );
        }
        return ret;
    }

    /**
     * array must be an array
     */
    private static Object addArrayWithObject( Object array, Object object )
    {
        int l = Array.getLength( array );
        Object[] ret = new Object[l + 1];
        int i = 0;
        for (; i < l; i++ )
        {
            ret[i] = Array.get( array, i );
        }
        ret[i] = object;

        return ret;
    }

    /**
     * array must be an array
     */
    private static Object addObjectWithArray( Object object, Object array )
    {
        int l = Array.getLength( array );
        Object[] ret = new Object[l + 1];
        ret[0] = object;
        for ( int i = 1; i < ret.length; i++ )
        {
            ret[i] = Array.get( array, i );
        }

        return ret;
    }
}
