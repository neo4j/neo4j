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
package org.neo4j.cypher.internal.compiler.v2_3.birk;

import org.neo4j.cypher.internal.compiler.v2_3.CypherTypeException;

import java.lang.reflect.Array;
import java.util.Arrays;

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
    public static Object add( Object op1, Object op2 )
    {
        if ( op1 == null || op2 == null )
        {
            return null;
        }

        if ( op1 instanceof String || op2 instanceof String )
        {
            return String.valueOf( op1 ) + String.valueOf( op2 );
        }

        //array multiplication
        Class<?> op1Class = op1.getClass();
        Class<?> op2Class = op2.getClass();
        if ( op1Class.isArray() && op2Class.isArray())
        {
            return addArrays( op1, op2 );
        }
        else if ( op1Class.isArray() )
        {
            return addArrayWithObject( op1, op2 );
        }
        else if ( op2Class.isArray() )
        {
            return addObjectWithArray( op1, op2 );
        }


        //From here down we assume we are dealing with numbers
        if( !(op1 instanceof Number) || !(op2 instanceof Number) ){
            throw new CypherTypeException( "Cannot add " + op1.getClass().getSimpleName() + " and " + op2.getClass()
                    .getSimpleName(), null );
        }

        if ( op1 instanceof Long || op2 instanceof Long )
        {
            return ((Number) op1).longValue() + ((Number) op2).longValue();
        }

        if ( op1 instanceof Double || op2 instanceof Double )
        {
            return ((Number) op1).doubleValue() + ((Number) op2).doubleValue();
        }

        if ( op1 instanceof Float || op2 instanceof Float )
        {
            return ((Number) op1).floatValue() + ((Number) op2).floatValue();
        }

        if ( op1 instanceof Integer || op2 instanceof Integer )
        {
            return ((Number) op1).intValue() + ((Number) op2).intValue();
        }


        throw new CypherTypeException( "Cannot add " + op1.getClass().getSimpleName() + " and " + op2.getClass()
            .getSimpleName(), null );
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
