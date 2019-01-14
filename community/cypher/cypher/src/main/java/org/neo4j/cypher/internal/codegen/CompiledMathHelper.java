/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.codegen;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.cypher.internal.util.v3_4.ArithmeticException;
import org.neo4j.cypher.internal.util.v3_4.CypherTypeException;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TemporalValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.VirtualValues;

/**
 * This is a helper class used by compiled plans for doing basic math operations
 */
@SuppressWarnings( "unused" )
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
        if ( lhs == null || rhs == null || lhs == Values.NO_VALUE || rhs == Values.NO_VALUE )
        {
            return null;
        }

        //List addition
        boolean lhsIsListValue = lhs instanceof ListValue;
        if ( lhsIsListValue && rhs instanceof ListValue )
        {
            return VirtualValues.concat( (ListValue) lhs, (ListValue) rhs );
        }
        else if ( lhsIsListValue )
        {
            if ( rhs instanceof List<?> )
            {
                return VirtualValues.concat( (ListValue) lhs, ValueUtils.asListValue( (List<?>) rhs ) );
            }
            else if ( rhs instanceof AnyValue )
            {
                return VirtualValues.appendToList( (ListValue) lhs, (AnyValue) rhs );
            }
            else
            {
                return VirtualValues.appendToList( (ListValue) lhs, ValueUtils.of( rhs ) );
            }
        }
        else if ( rhs instanceof ListValue )
        {
            if ( lhs instanceof List<?> )
            {
                return VirtualValues.concat( ValueUtils.asListValue( (List<?>) lhs ), (ListValue) rhs );
            }
            else if ( lhs instanceof AnyValue )
            {
                return VirtualValues.prependToList( (ListValue) rhs, (AnyValue) lhs );
            }
            else
            {
                return VirtualValues.prependToList( (ListValue) rhs, ValueUtils.of( lhs ) );
            }
        }
        else if ( lhs instanceof List<?> && rhs instanceof List<?> )
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

        // String addition
        if ( lhs instanceof TextValue )
        {
            lhs = ((TextValue) lhs).stringValue();
        }
        if ( rhs instanceof TextValue )
        {
            rhs = ((TextValue) rhs).stringValue();
        }
        if ( lhs instanceof String )
        {
            if ( rhs instanceof Value )
            {
                // Unfortunately string concatenation is not defined for temporal and spatial types, so we need to exclude them
                if ( !(rhs instanceof TemporalValue || rhs instanceof DurationValue || rhs instanceof PointValue) )
                {
                    return String.valueOf( lhs ) + ((Value) rhs).prettyPrint();
                }
            }
            else
            {
                return String.valueOf( lhs ) + String.valueOf( rhs );
            }
        }
        if ( rhs instanceof String )
        {
            if ( lhs instanceof Value )
            {
                // Unfortunately string concatenation is not defined for temporal and spatial types, so we need to exclude them
                if ( !(lhs instanceof TemporalValue || lhs instanceof DurationValue || lhs instanceof PointValue) )
                {
                    return ((Value) lhs).prettyPrint() + String.valueOf( rhs );
                }
            }
            else
            {
                return lhs.toString() + String.valueOf( rhs );
            }
        }

        // array addition

        // Extract arrays from ArrayValues
        if ( lhs instanceof ArrayValue )
        {
            lhs = ((ArrayValue) lhs).asObject();
        }
        if ( rhs instanceof ArrayValue )
        {
            rhs = ((ArrayValue) rhs).asObject();
        }

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

        // Handle NumberValues
        if ( lhs instanceof NumberValue && rhs instanceof NumberValue )
        {
            return ((NumberValue) lhs).plus( (NumberValue) rhs);
        }
        if ( lhs instanceof NumberValue )
        {
            lhs = ((NumberValue) lhs).asObject();
        }
        if ( rhs instanceof NumberValue )
        {
            rhs = ((NumberValue) rhs).asObject();
        }

        if ( lhs instanceof Number )
        {
            if ( rhs instanceof Number )
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
                    return Math.addExact( ((Number) lhs).longValue(), ((Number) rhs).longValue() );
                    // Remap java.lang.ArithmeticException later instead of:
                    //catch ( java.lang.ArithmeticException e )
                    //{
                    //    throw new ArithmeticException(
                    //            String.format( "result of %d + %d cannot be represented as an integer",
                    //                    ((Number) lhs).longValue(), ((Number) rhs).longValue() ), e );
                    //}
                }
            }
            // other numbers we cannot add
        }

        // Temporal values
        if ( lhs instanceof TemporalValue )
        {
            if ( rhs instanceof DurationValue )
            {
                return ((TemporalValue) lhs).plus( (DurationValue) rhs );
            }
        }
        if ( lhs instanceof DurationValue )
        {
            if ( rhs instanceof TemporalValue )
            {
                return ((TemporalValue) rhs).plus( (DurationValue) lhs );
            }
            if ( rhs instanceof DurationValue )
            {
                return ((DurationValue) lhs).add( (DurationValue) rhs );
            }
        }

        AnyValue lhsValue = lhs instanceof AnyValue ? (AnyValue) lhs : Values.of( lhs );
        AnyValue rhsValue = rhs instanceof AnyValue ? (AnyValue) rhs : Values.of( rhs );

        throw new CypherTypeException( String.format( "Cannot add `%s` and `%s`", lhsValue.getTypeName(), rhsValue.getTypeName() ), null );
    }

    public static Object subtract( Object lhs, Object rhs )
    {
        if ( lhs == null || rhs == null || lhs == Values.NO_VALUE || rhs == Values.NO_VALUE )
        {
            return null;
        }

        // Handle NumberValues
        if ( lhs instanceof NumberValue && rhs instanceof NumberValue )
        {
            return ((NumberValue) lhs).minus( (NumberValue) rhs );
        }
        if ( lhs instanceof NumberValue )
        {
            lhs = ((NumberValue) lhs).asObject();
        }
        if ( rhs instanceof NumberValue )
        {
            rhs = ((NumberValue) rhs).asObject();
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
                return Math.subtractExact( ((Number) lhs).longValue(), ((Number) rhs).longValue() );
                // Remap java.lang.ArithmeticException later instead of:
                //catch ( java.lang.ArithmeticException e )
                //{
                //    throw new ArithmeticException(
                //            String.format( "result of %d - %d cannot be represented as an integer",
                //                    ((Number) lhs).longValue(), ((Number) rhs).longValue() ), e );
                //}
            }
            // other numbers we cannot subtract
        }

        // Temporal values
        if ( lhs instanceof TemporalValue )
        {
            if ( rhs instanceof DurationValue )
            {
                return ((TemporalValue) lhs).minus( (DurationValue) rhs );
            }
        }
        if ( lhs instanceof DurationValue )
        {
            if ( rhs instanceof DurationValue )
            {
                return ((DurationValue) lhs).sub( (DurationValue) rhs );
            }
        }

        AnyValue lhsValue = lhs instanceof AnyValue ? (AnyValue) lhs : Values.of( lhs );
        AnyValue rhsValue = rhs instanceof AnyValue ? (AnyValue) rhs : Values.of( rhs );

        throw new CypherTypeException( String.format( "Cannot subtract `%s` from `%s`", rhsValue.getTypeName(), lhsValue.getTypeName() ), null );
    }

    public static Object multiply( Object lhs, Object rhs )
    {
        if ( lhs == null || rhs == null || lhs == Values.NO_VALUE || rhs == Values.NO_VALUE )
        {
            return null;
        }

        // Temporal values
        if ( lhs instanceof DurationValue )
        {
            if ( rhs instanceof NumberValue )
            {
                return ((DurationValue) lhs).mul( (NumberValue) rhs );
            }
            if ( rhs instanceof Number )
            {
                return ((DurationValue) lhs).mul( Values.numberValue( (Number) rhs ) );
            }
        }
        if ( rhs instanceof DurationValue )
        {
            if ( lhs instanceof NumberValue )
            {
                return ((DurationValue) rhs).mul( (NumberValue) lhs );
            }
            if ( lhs instanceof Number )
            {
                return ((DurationValue) rhs).mul( Values.numberValue( (Number) lhs ) );
            }
        }

        // Handle NumberValues
        if ( lhs instanceof NumberValue && rhs instanceof NumberValue )
        {
            return ((NumberValue) lhs).times( (NumberValue) rhs );
        }
        if ( lhs instanceof NumberValue )
        {
            lhs = ((NumberValue) lhs).asObject();
        }
        if ( rhs instanceof NumberValue )
        {
            rhs = ((NumberValue) rhs).asObject();
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
                return Math.multiplyExact( ((Number) lhs).longValue(), ((Number) rhs).longValue() );
                // Remap java.lang.ArithmeticException later instead of:
                //catch ( java.lang.ArithmeticException e )
                //{
                //    throw new ArithmeticException(
                //            String.format( "result of %d * %d cannot be represented as an integer",
                //                    ((Number) lhs).longValue(), ((Number) rhs).longValue() ), e );
                //}
            }
            // other numbers we cannot multiply
        }

        AnyValue lhsValue = lhs instanceof AnyValue ? (AnyValue) lhs : Values.of( lhs );
        AnyValue rhsValue = rhs instanceof AnyValue ? (AnyValue) rhs : Values.of( rhs );

        throw new CypherTypeException( String.format( "Cannot multiply `%s` and `%s`", lhsValue.getTypeName(), rhsValue.getTypeName() ), null );
    }

    public static Object divide( Object lhs, Object rhs )
    {
        if ( lhs == null || rhs == null || lhs == Values.NO_VALUE || rhs == Values.NO_VALUE )
        {
            return null;
        }

        // Temporal values
        if ( lhs instanceof DurationValue )
        {
            if ( rhs instanceof NumberValue )
            {
                return ((DurationValue) lhs).div( (NumberValue) rhs );
            }
            if ( rhs instanceof Number )
            {
                return ((DurationValue) lhs).div( Values.numberValue( (Number) rhs ) );
            }
        }

        // Handle NumberValues
        if ( lhs instanceof NumberValue && rhs instanceof NumberValue )
        {
            if ( rhs instanceof IntegralValue )
            {
                long right = ((IntegralValue) rhs).longValue();
                if ( right == 0 )
                {
                    throw new ArithmeticException( "/ by zero", null );
                }
            }
            return ((NumberValue) lhs).divideBy( (NumberValue) rhs );
        }
        if ( lhs instanceof NumberValue )
        {
            lhs = ((NumberValue) lhs).asObject();
        }
        if ( rhs instanceof NumberValue )
        {
            rhs = ((NumberValue) rhs).asObject();
        }

        if ( lhs instanceof Number && rhs instanceof Number )
        {
            if ( lhs instanceof Double || rhs instanceof Double ||
                 lhs instanceof Float || rhs instanceof Float )
            {
                double left = ((Number) lhs).doubleValue();
                double right = ((Number) rhs).doubleValue();
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

        AnyValue lhsValue = lhs instanceof AnyValue ? (AnyValue) lhs : Values.of( lhs );
        AnyValue rhsValue = rhs instanceof AnyValue ? (AnyValue) rhs : Values.of( rhs );

        throw new CypherTypeException( String.format( "Cannot divide `%s` by `%s`", lhsValue.getTypeName(), rhsValue.getTypeName() ), null );
    }

    public static Object modulo( Object lhs, Object rhs )
    {
        if ( lhs == null || rhs == null || lhs == Values.NO_VALUE || rhs == Values.NO_VALUE )
        {
            return null;
        }

        // Handle NumberValues
        if ( lhs instanceof NumberValue )
        {
            lhs = ((NumberValue) lhs).asObject();
        }
        if ( rhs instanceof NumberValue )
        {
            rhs = ((NumberValue) rhs).asObject();
        }

        if ( lhs instanceof Number && rhs instanceof Number )
        {
            if ( lhs instanceof Double || rhs instanceof Double ||
                    lhs instanceof Float || rhs instanceof Float )
            {
                double left = ((Number) lhs).doubleValue();
                double right = ((Number) rhs).doubleValue();
                return left % right;
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
                return left % right;
            }
            // other numbers we cannot divide
        }

        AnyValue lhsValue = lhs instanceof AnyValue ? (AnyValue) lhs : Values.of( lhs );
        AnyValue rhsValue = rhs instanceof AnyValue ? (AnyValue) rhs : Values.of( rhs );

        throw new CypherTypeException( String.format( "Cannot calculate modulus of `%s` and `%s`", lhsValue.getTypeName(), rhsValue.getTypeName() ), null );
    }

    public static int transformToInt( Object value )
    {
        if ( value == null )
        {
            throw new CypherTypeException( "Expected a numeric value but got null", null );
        }
        if ( value instanceof NumberValue )
        {
            value = ((NumberValue) value).asObject();
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

    public static long transformToLong( Object value )
    {
        if ( value == null )
        {
            throw new CypherTypeException( "Expected a numeric value but got null", null );
        }
        if ( value instanceof NumberValue )
        {
            NumberValue number = (NumberValue) value;
            return number.longValue();
        }
        if ( value instanceof Number )
        {
            Number number = (Number) value;
            return number.longValue();
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
