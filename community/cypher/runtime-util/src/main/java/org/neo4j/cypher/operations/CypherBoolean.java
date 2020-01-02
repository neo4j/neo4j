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
package org.neo4j.cypher.operations;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.neo4j.util.CalledFromGeneratedCode;
import org.neo4j.exceptions.CypherTypeException;
import org.neo4j.exceptions.InternalException;
import org.neo4j.exceptions.InvalidSemanticsException;
import org.neo4j.values.AnyValue;
import org.neo4j.values.AnyValues;
import org.neo4j.values.Comparison;
import org.neo4j.values.Equality;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;

import static java.lang.String.format;
import static org.neo4j.values.storable.Values.FALSE;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.TRUE;

/**
 * This class contains static helper boolean methods used by the compiled expressions
 */
@SuppressWarnings( {"ReferenceEquality"} )
public final class CypherBoolean
{
    private static final BooleanMapper BOOLEAN_MAPPER = new BooleanMapper();

    private CypherBoolean()
    {
        throw new UnsupportedOperationException( "Do not instantiate" );
    }

    @CalledFromGeneratedCode
    public static Value xor( AnyValue lhs, AnyValue rhs )
    {
        assert lhs != NO_VALUE && rhs != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        return (lhs == TRUE) ^ (rhs == TRUE) ? TRUE : FALSE;
    }

    @CalledFromGeneratedCode
    public static Value not( AnyValue in )
    {
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        return in != TRUE ? TRUE : FALSE;
    }

    @CalledFromGeneratedCode
    public static Value equals( AnyValue lhs, AnyValue rhs )
    {
        assert lhs != NO_VALUE && rhs != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        Equality compare = lhs.ternaryEquals( rhs );
        switch ( compare )
        {
        case TRUE:
            return Values.TRUE;
        case FALSE:
            return Values.FALSE;
        case UNDEFINED:
            return NO_VALUE;
        default:
            throw new IllegalArgumentException( format("%s is not a valid result for %s=%s", compare, lhs, rhs )  );
        }
    }

    @CalledFromGeneratedCode
    public static Value notEquals( AnyValue lhs, AnyValue rhs )
    {
        assert lhs != NO_VALUE && rhs != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        Equality compare = lhs.ternaryEquals( rhs );
        switch ( compare )
        {
        case TRUE:
            return Values.FALSE;
        case FALSE:
            return Values.TRUE;
        case UNDEFINED:
            return NO_VALUE;
        default:
            throw new IllegalArgumentException( format("%s is not a valid result for %s<>%s", compare, lhs, rhs )  );
        }
    }

    @CalledFromGeneratedCode
    public static BooleanValue regex( TextValue lhs, TextValue rhs )
    {
        assert lhs != NO_VALUE && rhs != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        String regexString = rhs.stringValue();
        try
        {
            boolean matches = Pattern.compile( regexString ).matcher( lhs.stringValue() ).matches();
            return matches ? TRUE : FALSE;
        }
        catch ( PatternSyntaxException e )
        {
            throw new InvalidSemanticsException( "Invalid Regex: " + e.getMessage(), null );
        }
    }

    @CalledFromGeneratedCode
    public static BooleanValue regex( TextValue text, Pattern pattern )
    {
        assert text != NO_VALUE : "NO_VALUE checks need to happen outside this call";

        boolean matches = pattern.matcher( text.stringValue() ).matches();
        return matches ? TRUE : FALSE;
    }

    @CalledFromGeneratedCode
    public static Value lessThan( AnyValue lhs, AnyValue rhs )
    {
        if ( AnyValue.isNanAndNumber(lhs, rhs) )
        {
            return FALSE;
        }
        Comparison comparison = AnyValues.TERNARY_COMPARATOR.ternaryCompare( lhs, rhs );
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

    @CalledFromGeneratedCode
    public static Value lessThanOrEqual( AnyValue lhs, AnyValue rhs )
    {
        if ( AnyValue.isNanAndNumber(lhs, rhs) )
        {
            return FALSE;
        }
        Comparison comparison = AnyValues.TERNARY_COMPARATOR.ternaryCompare( lhs, rhs );
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

    @CalledFromGeneratedCode
    public static Value greaterThan( AnyValue lhs, AnyValue rhs )
    {
        if ( AnyValue.isNanAndNumber(lhs, rhs) )
        {
            return FALSE;
        }
        Comparison comparison = AnyValues.TERNARY_COMPARATOR.ternaryCompare( lhs, rhs );
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

    @CalledFromGeneratedCode
    public static Value greaterThanOrEqual( AnyValue lhs, AnyValue rhs )
    {
        if ( AnyValue.isNanAndNumber(lhs, rhs) )
        {
            return FALSE;
        }
        Comparison comparison = AnyValues.TERNARY_COMPARATOR.ternaryCompare( lhs, rhs );
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

    @CalledFromGeneratedCode
    public static Value coerceToBoolean( AnyValue value )
    {
        return value.map( BOOLEAN_MAPPER );
    }

    @CalledFromGeneratedCode
    public static Value in( AnyValue lhs, AnyValue rhs )
    {
        assert rhs != NO_VALUE;

        ListValue anyValues = CypherFunctions.asList( rhs );

        boolean seenUndefined = false;
        for ( AnyValue value : anyValues )
        {
            switch ( lhs.ternaryEquals( value ) )
            {
            case TRUE:
                return Values.TRUE;
            case UNDEFINED:
                seenUndefined = true;
            case FALSE:
                break;
            default:
                throw new IllegalStateException( "Unknown state" );
            }
        }

        return seenUndefined ? NO_VALUE : Values.FALSE;
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
