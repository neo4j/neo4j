/*
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
package org.neo4j.cypher.internal.compiler.v2_3.birk;

import java.lang.reflect.Array;

import org.neo4j.cypher.internal.compiler.v2_3.CypherTypeException;

/**
 * This is a helper class used by compiled plans for doing basic math operations
 */
public final class CompiledMathHelper
{
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

        // array addition
        Class<?> op1Class = lhs.getClass();
        Class<?> op2Class = rhs.getClass();
        if ( op1Class.isArray() && op2Class.isArray())
        {
            return addArrays( lhs, rhs );
        }
        else if ( op1Class.isArray() )
        {
            return addArrayWithObject( lhs, rhs );
        }
        else if ( op2Class.isArray() )
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
        for (int i = 1; i < ret.length; i++ )
        {
            ret[i] = Array.get( array, i );
        }

        return ret;
    }
}
