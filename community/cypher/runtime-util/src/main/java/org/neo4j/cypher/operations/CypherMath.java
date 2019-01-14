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
package org.neo4j.cypher.operations;

import org.neo4j.cypher.internal.v3_5.util.ArithmeticException;
import org.neo4j.cypher.internal.v3_5.util.CypherTypeException;

import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.FloatingPointValue;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TemporalValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.VirtualValues;

import static org.neo4j.values.storable.Values.ZERO_INT;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;

/**
 * This class contains static helper math methods used by the compiled expressions
 */
@SuppressWarnings( "unused" )
public final class CypherMath
{
    private CypherMath()
    {
        throw new UnsupportedOperationException( "Do not instantiate" );
    }

    //TODO this is horrible spaghetti code, we should push most of this down to AnyValue
    public static AnyValue add( AnyValue lhs, AnyValue rhs )
    {
        if ( lhs instanceof NumberValue && rhs instanceof NumberValue )
        {
            return ((NumberValue) lhs).plus( (NumberValue) rhs );
        }
        //List addition
        //arrays are same as lists when it comes to addition
        if ( lhs instanceof ArrayValue )
        {
            lhs = VirtualValues.fromArray( (ArrayValue) lhs );
        }
        if ( rhs instanceof ArrayValue )
        {
            rhs = VirtualValues.fromArray( (ArrayValue) rhs );
        }

        boolean lhsIsListValue = lhs instanceof ListValue;
        if ( lhsIsListValue && rhs instanceof ListValue )
        {
            return VirtualValues.concat( (ListValue) lhs, (ListValue) rhs );
        }
        else if ( lhsIsListValue )
        {
            return ((ListValue) lhs).append( rhs );
        }
        else if ( rhs instanceof ListValue )
        {
            return ((ListValue) rhs).prepend( lhs );
        }

        // String addition
        if ( lhs instanceof TextValue && rhs instanceof TextValue )
        {
            return ((TextValue) lhs).plus( (TextValue) rhs );
        }
        else if ( lhs instanceof TextValue )
        {
            if ( rhs instanceof Value )
            {
                // Unfortunately string concatenation is not defined for temporal and spatial types, so we need to
                // exclude them
                if ( !(rhs instanceof TemporalValue || rhs instanceof DurationValue || rhs instanceof PointValue) )
                {
                    return stringValue( ((TextValue) lhs).stringValue() + ((Value) rhs).prettyPrint() );
                }
                else
                {
                    return stringValue( ((TextValue) lhs).stringValue() + String.valueOf( rhs ) );
                }
            }
        }
        else if ( rhs instanceof TextValue )
        {
            if ( lhs instanceof Value )
            {
                // Unfortunately string concatenation is not defined for temporal and spatial types, so we need to
                // exclude them
                if ( !(lhs instanceof TemporalValue || lhs instanceof DurationValue || lhs instanceof PointValue) )
                {
                    return stringValue( ((Value) lhs).prettyPrint() + ((TextValue) rhs).stringValue() );
                }
                else
                {
                    return stringValue( String.valueOf( lhs ) + ((TextValue) rhs).stringValue() );
                }
            }
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

        throw new CypherTypeException(
                String.format( "Cannot add `%s` and `%s`", lhs.getTypeName(), rhs.getTypeName() ), null );
    }

    public static AnyValue subtract( AnyValue lhs, AnyValue rhs )
    {
        //numbers
        if ( lhs instanceof NumberValue && rhs instanceof NumberValue )
        {
            return ((NumberValue) lhs).minus( (NumberValue) rhs );
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
        throw new CypherTypeException(
                String.format( "Cannot subtract `%s` from `%s`", rhs.getTypeName(), lhs.getTypeName() ), null );
    }

    public static AnyValue multiply( AnyValue lhs, AnyValue rhs )
    {
        if ( lhs instanceof NumberValue && rhs instanceof NumberValue )
        {
            return ((NumberValue) lhs).times( (NumberValue) rhs );
        }
        // Temporal values
        if ( lhs instanceof DurationValue )
        {
            if ( rhs instanceof NumberValue )
            {
                return ((DurationValue) lhs).mul( (NumberValue) rhs );
            }
        }
        if ( rhs instanceof DurationValue )
        {
            if ( lhs instanceof NumberValue )
            {
                return ((DurationValue) rhs).mul( (NumberValue) lhs );
            }
        }
        throw new CypherTypeException(
                String.format( "Cannot multiply `%s` and `%s`", lhs.getTypeName(), rhs.getTypeName() ), null );
    }

    public static boolean divideCheckForNull( AnyValue lhs, AnyValue rhs )
    {
        if ( rhs instanceof IntegralValue && rhs.equals( ZERO_INT ) )
        {
            throw new ArithmeticException( "/ by zero", null );
        }
        else
        {
            return lhs == Values.NO_VALUE || rhs == Values.NO_VALUE;
        }
    }

    public static AnyValue divide( AnyValue lhs, AnyValue rhs )
    {
        if ( lhs instanceof NumberValue && rhs instanceof NumberValue )
        {
            return ((NumberValue) lhs).divideBy( (NumberValue) rhs );
        }
        // Temporal values
        if ( lhs instanceof DurationValue )
        {
            if ( rhs instanceof NumberValue )
            {
                return ((DurationValue) lhs).div( (NumberValue) rhs );
            }
        }
        throw new CypherTypeException(
                String.format( "Cannot divide `%s` by `%s`", lhs.getTypeName(), rhs.getTypeName() ), null );
    }

    public static AnyValue modulo( AnyValue lhs, AnyValue rhs )
    {
        if ( lhs instanceof NumberValue && rhs instanceof NumberValue )
        {
            if ( lhs instanceof FloatingPointValue || rhs instanceof FloatingPointValue )
            {
                return doubleValue( ((NumberValue) lhs).doubleValue() % ((NumberValue) rhs).doubleValue() );
            }
            else
            {
                return longValue( ((NumberValue) lhs).longValue() % ((NumberValue) rhs).longValue() );
            }
        }
        throw new CypherTypeException(
                String.format( "Cannot calculate modulus of `%s` and `%s`", lhs.getTypeName(), rhs.getTypeName() ),
                null );
    }

    public static AnyValue pow( AnyValue lhs, AnyValue rhs )
    {
        if ( lhs instanceof NumberValue && rhs instanceof NumberValue )
        {
            return doubleValue( Math.pow( ((NumberValue) lhs).doubleValue(), ((NumberValue) rhs).doubleValue() ) );
        }
        throw new CypherTypeException(
                String.format( "Cannot raise `%s` to the power of `%s`", lhs.getTypeName(), rhs.getTypeName() ), null );
    }
}
