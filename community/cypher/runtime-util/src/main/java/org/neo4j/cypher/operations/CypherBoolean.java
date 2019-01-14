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

import org.neo4j.cypher.internal.v3_5.util.CypherTypeException;
import org.neo4j.cypher.internal.v3_5.util.InternalException;
import org.neo4j.cypher.internal.v3_5.util.InvalidSemanticsException;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.neo4j.values.AnyValue;
import org.neo4j.values.AnyValues;
import org.neo4j.values.Comparison;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.FloatingPointValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;

import static org.neo4j.values.storable.Values.FALSE;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.TRUE;

/**
 * This class contains static helper boolean methods used by the compiled expressions
 */
@SuppressWarnings( "unused" )
public final class CypherBoolean
{
    private static final BooleanMapper BOOLEAN_MAPPER = new BooleanMapper();

    private CypherBoolean()
    {
        throw new UnsupportedOperationException( "Do not instantiate" );
    }

    public static Value xor( AnyValue lhs, AnyValue rhs )
    {
        return (lhs == TRUE) ^ (rhs == TRUE) ? TRUE : FALSE;
    }

    public static Value not( AnyValue in )
    {
        return in != TRUE ? TRUE : FALSE;
    }

    public static Value equals( AnyValue lhs, AnyValue rhs )
    {
        Boolean compare = lhs.ternaryEquals( rhs );
        if ( compare == null )
        {
            return NO_VALUE;
        }
        return compare ? TRUE : FALSE;
    }

    public static Value notEquals( AnyValue lhs, AnyValue rhs )
    {
        Boolean compare = lhs.ternaryEquals( rhs );
        if ( compare == null )
        {
            return NO_VALUE;
        }
        return compare ? FALSE : TRUE;
    }

    public static BooleanValue regex( TextValue lhs, TextValue rhs )
    {
        String regexString = rhs.stringValue();
        try
        {
            boolean matches = Pattern.compile( regexString ).matcher( lhs.stringValue() ).matches();
            return matches ? TRUE : FALSE;
        }
        catch ( PatternSyntaxException e )
        {
            throw new InvalidSemanticsException( "Invalid Regex: " + e.getMessage() );
        }
    }

    public static BooleanValue regex( TextValue text, Pattern pattern )
    {
        boolean matches = pattern.matcher( text.stringValue() ).matches();
        return matches ? TRUE : FALSE;
    }

    public static Value lessThan( AnyValue lhs, AnyValue rhs )
    {
        if ( isNan( lhs ) || isNan( rhs ) )
        {
            return NO_VALUE;
        }
        else if ( lhs instanceof NumberValue && rhs instanceof NumberValue )
        {
            return lessThan( AnyValues.TERNARY_COMPARATOR.ternaryCompare( lhs, rhs ) );
        }
        else if ( lhs instanceof TextValue && rhs instanceof TextValue )
        {
            return lessThan( AnyValues.TERNARY_COMPARATOR.ternaryCompare( lhs, rhs ) );
        }
        else if ( lhs instanceof BooleanValue && rhs instanceof BooleanValue )
        {
            return lessThan( AnyValues.TERNARY_COMPARATOR.ternaryCompare( lhs, rhs ) );
        }
        else if ( lhs instanceof PointValue && rhs instanceof PointValue )
        {
            return lessThan( AnyValues.TERNARY_COMPARATOR.ternaryCompare( lhs, rhs ) );
        }
        else if ( lhs instanceof DateValue && rhs instanceof DateValue )
        {
            return lessThan( AnyValues.TERNARY_COMPARATOR.ternaryCompare( lhs, rhs ) );
        }
        else if ( lhs instanceof LocalTimeValue && rhs instanceof LocalTimeValue )
        {
            return lessThan( AnyValues.TERNARY_COMPARATOR.ternaryCompare( lhs, rhs ) );
        }
        else if ( lhs instanceof TimeValue && rhs instanceof TimeValue )
        {
            return lessThan( AnyValues.TERNARY_COMPARATOR.ternaryCompare( lhs, rhs ) );
        }
        else if ( lhs instanceof LocalDateTimeValue && rhs instanceof LocalDateTimeValue )
        {
            return lessThan( AnyValues.TERNARY_COMPARATOR.ternaryCompare( lhs, rhs ) );
        }
        else if ( lhs instanceof DateTimeValue && rhs instanceof DateTimeValue )
        {
            return lessThan( AnyValues.TERNARY_COMPARATOR.ternaryCompare( lhs, rhs ) );
        }
        else
        {
            return NO_VALUE;
        }
    }

    public static Value lessThanOrEqual( AnyValue lhs, AnyValue rhs )
    {
        if ( isNan( lhs ) || isNan( rhs ) )
        {
            return NO_VALUE;
        }
        else if ( lhs instanceof NumberValue && rhs instanceof NumberValue )
        {
            return lessThanOrEqual( AnyValues.TERNARY_COMPARATOR.ternaryCompare( lhs, rhs ) );
        }
        else if ( lhs instanceof TextValue && rhs instanceof TextValue )
        {
            return lessThanOrEqual( AnyValues.TERNARY_COMPARATOR.ternaryCompare( lhs, rhs ) );
        }
        else if ( lhs instanceof BooleanValue && rhs instanceof BooleanValue )
        {
            return lessThanOrEqual( AnyValues.TERNARY_COMPARATOR.ternaryCompare( lhs, rhs ) );
        }
        else if ( lhs instanceof PointValue && rhs instanceof PointValue )
        {
            return lessThanOrEqual( AnyValues.TERNARY_COMPARATOR.ternaryCompare( lhs, rhs ) );
        }
        else if ( lhs instanceof DateValue && rhs instanceof DateValue )
        {
            return lessThanOrEqual( AnyValues.TERNARY_COMPARATOR.ternaryCompare( lhs, rhs ) );
        }
        else if ( lhs instanceof LocalTimeValue && rhs instanceof LocalTimeValue )
        {
            return lessThanOrEqual( AnyValues.TERNARY_COMPARATOR.ternaryCompare( lhs, rhs ) );
        }
        else if ( lhs instanceof TimeValue && rhs instanceof TimeValue )
        {
            return lessThanOrEqual( AnyValues.TERNARY_COMPARATOR.ternaryCompare( lhs, rhs ) );
        }
        else if ( lhs instanceof LocalDateTimeValue && rhs instanceof LocalDateTimeValue )
        {
            return lessThanOrEqual( AnyValues.TERNARY_COMPARATOR.ternaryCompare( lhs, rhs ) );
        }
        else if ( lhs instanceof DateTimeValue && rhs instanceof DateTimeValue )
        {
            return lessThanOrEqual( AnyValues.TERNARY_COMPARATOR.ternaryCompare( lhs, rhs ) );
        }
        else
        {
            return NO_VALUE;
        }
    }

    public static Value greaterThan( AnyValue lhs, AnyValue rhs )
    {
        if ( isNan( lhs ) || isNan( rhs ) )
        {
            return NO_VALUE;
        }
        else if ( lhs instanceof NumberValue && rhs instanceof NumberValue )
        {
            return greaterThan( AnyValues.TERNARY_COMPARATOR.ternaryCompare( lhs, rhs ) );
        }
        else if ( lhs instanceof TextValue && rhs instanceof TextValue )
        {
            return greaterThan( AnyValues.TERNARY_COMPARATOR.ternaryCompare( lhs, rhs ) );
        }
        else if ( lhs instanceof BooleanValue && rhs instanceof BooleanValue )
        {
            return greaterThan( AnyValues.TERNARY_COMPARATOR.ternaryCompare( lhs, rhs ) );
        }
        else if ( lhs instanceof PointValue && rhs instanceof PointValue )
        {
            return greaterThan( AnyValues.TERNARY_COMPARATOR.ternaryCompare( lhs, rhs ) );
        }
        else if ( lhs instanceof DateValue && rhs instanceof DateValue )
        {
            return greaterThan( AnyValues.TERNARY_COMPARATOR.ternaryCompare( lhs, rhs ) );
        }
        else if ( lhs instanceof LocalTimeValue && rhs instanceof LocalTimeValue )
        {
            return greaterThan( AnyValues.TERNARY_COMPARATOR.ternaryCompare( lhs, rhs ) );
        }
        else if ( lhs instanceof TimeValue && rhs instanceof TimeValue )
        {
            return greaterThan( AnyValues.TERNARY_COMPARATOR.ternaryCompare( lhs, rhs ) );
        }
        else if ( lhs instanceof LocalDateTimeValue && rhs instanceof LocalDateTimeValue )
        {
            return greaterThan( AnyValues.TERNARY_COMPARATOR.ternaryCompare( lhs, rhs ) );
        }
        else if ( lhs instanceof DateTimeValue && rhs instanceof DateTimeValue )
        {
            return greaterThan( AnyValues.TERNARY_COMPARATOR.ternaryCompare( lhs, rhs ) );
        }
        else
        {
            return NO_VALUE;
        }
    }

    public static Value greaterThanOrEqual( AnyValue lhs, AnyValue rhs )
    {
        if ( isNan( lhs ) || isNan( rhs ) )
        {
            return NO_VALUE;
        }
        else if ( lhs instanceof NumberValue && rhs instanceof NumberValue )
        {
            return greaterThanOrEqual( AnyValues.TERNARY_COMPARATOR.ternaryCompare( lhs, rhs ) );
        }
        else if ( lhs instanceof TextValue && rhs instanceof TextValue )
        {
            return greaterThanOrEqual( AnyValues.TERNARY_COMPARATOR.ternaryCompare( lhs, rhs ) );
        }
        else if ( lhs instanceof BooleanValue && rhs instanceof BooleanValue )
        {
            return greaterThanOrEqual( AnyValues.TERNARY_COMPARATOR.ternaryCompare( lhs, rhs ) );
        }
        else if ( lhs instanceof PointValue && rhs instanceof PointValue )
        {
            return greaterThanOrEqual( AnyValues.TERNARY_COMPARATOR.ternaryCompare( lhs, rhs ) );
        }
        else if ( lhs instanceof DateValue && rhs instanceof DateValue )
        {
            return greaterThanOrEqual( AnyValues.TERNARY_COMPARATOR.ternaryCompare( lhs, rhs ) );
        }
        else if ( lhs instanceof LocalTimeValue && rhs instanceof LocalTimeValue )
        {
            return greaterThanOrEqual( AnyValues.TERNARY_COMPARATOR.ternaryCompare( lhs, rhs ) );
        }
        else if ( lhs instanceof TimeValue && rhs instanceof TimeValue )
        {
            return greaterThanOrEqual( AnyValues.TERNARY_COMPARATOR.ternaryCompare( lhs, rhs ) );
        }
        else if ( lhs instanceof LocalDateTimeValue && rhs instanceof LocalDateTimeValue )
        {
            return greaterThanOrEqual( AnyValues.TERNARY_COMPARATOR.ternaryCompare( lhs, rhs ) );
        }
        else if ( lhs instanceof DateTimeValue && rhs instanceof DateTimeValue )
        {
            return greaterThanOrEqual( AnyValues.TERNARY_COMPARATOR.ternaryCompare( lhs, rhs ) );
        }
        else
        {
            return NO_VALUE;
        }
    }

    public static Value coerceToBoolean( AnyValue value )
    {
        return value.map( BOOLEAN_MAPPER );
    }

    private static Value lessThan( Comparison comparison )
    {
        switch ( comparison )
        {
        case GREATER_THAN_AND_EQUAL:
        case GREATER_THAN:
        case EQUAL:
        case SMALLER_THAN_AND_EQUAL:
            return FALSE;
        case SMALLER_THAN:
            return TRUE;
        case UNDEFINED:
            return NO_VALUE;
        default:
            throw new InternalException( comparison + " is not a known comparison", null );
        }
    }

    private static Value lessThanOrEqual( Comparison comparison )
    {
        switch ( comparison )
        {
        case GREATER_THAN_AND_EQUAL:
        case GREATER_THAN:
            return FALSE;
        case EQUAL:
        case SMALLER_THAN_AND_EQUAL:
        case SMALLER_THAN:
            return TRUE;
        case UNDEFINED:
            return NO_VALUE;
        default:
            throw new InternalException( comparison + " is not a known comparison", null );
        }
    }

    private static Value greaterThanOrEqual( Comparison comparison )
    {
        switch ( comparison )
        {
        case GREATER_THAN_AND_EQUAL:
        case GREATER_THAN:
        case EQUAL:
            return TRUE;
        case SMALLER_THAN_AND_EQUAL:
        case SMALLER_THAN:
            return FALSE;
        case UNDEFINED:
            return NO_VALUE;
        default:
            throw new InternalException( comparison + " is not a known comparison", null );
        }
    }

    private static Value greaterThan( Comparison comparison )
    {
        switch ( comparison )
        {
        case GREATER_THAN:
            return TRUE;
        case GREATER_THAN_AND_EQUAL:
        case EQUAL:
        case SMALLER_THAN_AND_EQUAL:
        case SMALLER_THAN:
            return FALSE;
        case UNDEFINED:
            return NO_VALUE;
        default:
            throw new InternalException( comparison + " is not a known comparison", null );
        }
    }

    private static boolean isNan( AnyValue value )
    {
        return value instanceof FloatingPointValue && ((FloatingPointValue) value).isNaN();
    }

    private static final class BooleanMapper implements ValueMapper<Value>
    {
        @Override
        public Value mapPath( PathValue value )
        {
            return value.size() > 0 ? TRUE : FALSE;
        }

        @Override
        public Value mapNode( VirtualNodeValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null );
        }

        @Override
        public Value mapRelationship( VirtualRelationshipValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null );
        }

        @Override
        public Value mapMap( MapValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null );
        }

        @Override
        public Value mapNoValue()
        {
            return NO_VALUE;
        }

        @Override
        public Value mapSequence( SequenceValue value )
        {
            return value.length() > 0 ? TRUE : FALSE;
        }

        @Override
        public Value mapText( TextValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null );
        }

        @Override
        public Value mapBoolean( BooleanValue value )
        {
            return value;
        }

        @Override
        public Value mapNumber( NumberValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null );
        }

        @Override
        public Value mapDateTime( DateTimeValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null );
        }

        @Override
        public Value mapLocalDateTime( LocalDateTimeValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null );
        }

        @Override
        public Value mapDate( DateValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null );
        }

        @Override
        public Value mapTime( TimeValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null );
        }

        @Override
        public Value mapLocalTime( LocalTimeValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null );
        }

        @Override
        public Value mapDuration( DurationValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null );
        }

        @Override
        public Value mapPoint( PointValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null );
        }
    }
}
