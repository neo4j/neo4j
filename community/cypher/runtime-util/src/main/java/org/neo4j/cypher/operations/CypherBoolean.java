/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.opencypher.v9_0.util.CypherTypeException;
import org.opencypher.v9_0.util.InvalidSemanticsException;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.neo4j.values.AnyValue;
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
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;

import static org.neo4j.values.storable.Values.NO_VALUE;

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
        return (lhs == Values.TRUE) ^ (rhs == Values.TRUE) ? Values.TRUE : Values.FALSE;
    }

    public static Value not( AnyValue in )
    {
        return in != Values.TRUE ? Values.TRUE : Values.FALSE;
    }

    public static Value equals( AnyValue lhs, AnyValue rhs )
    {
        Boolean compare = lhs.ternaryEquals( rhs );
        if ( compare == null )
        {
            return NO_VALUE;
        }
        return compare ? Values.TRUE : Values.FALSE;
    }

    public static Value notEquals( AnyValue lhs, AnyValue rhs )
    {
        Boolean compare = lhs.ternaryEquals( rhs );
        if ( compare == null )
        {
            return NO_VALUE;
        }
        return compare ? Values.FALSE : Values.TRUE;
    }

    public static Value regex( AnyValue lhs, AnyValue rhs )
    {
        String regexString = CypherFunctions.asString( rhs );
        if ( lhs instanceof TextValue )
        {
            try
            {
                boolean matches = Pattern.compile( regexString ).matcher( ((TextValue) lhs).stringValue() ).matches();
                return matches ? Values.TRUE : Values.FALSE;
            }
            catch ( PatternSyntaxException e )
            {
                throw new InvalidSemanticsException( "Invalid Regex: " + e.getMessage() );
            }
        }
        else
        {
            return NO_VALUE;
        }
    }

    public static Value regex( AnyValue text, Pattern pattern )
    {
        if ( text instanceof TextValue )
        {
            boolean matches = pattern.matcher( ((TextValue) text).stringValue() ).matches();
            return matches ? Values.TRUE : Values.FALSE;
        }
        else
        {
            return NO_VALUE;
        }
    }

    public static Value startsWith( AnyValue lhs, AnyValue rhs )
    {
        if ( lhs instanceof TextValue && rhs instanceof TextValue )
        {
            return ((TextValue) lhs).stringValue().startsWith( ((TextValue) rhs).stringValue() ) ? Values.TRUE : Values.FALSE;
        }
        else
        {
            return NO_VALUE;
        }
    }

    public static Value endsWith( AnyValue lhs, AnyValue rhs )
    {
        if ( lhs instanceof TextValue && rhs instanceof TextValue )
        {
            return ((TextValue) lhs).stringValue().endsWith( ((TextValue) rhs).stringValue() ) ? Values.TRUE : Values.FALSE;
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

    private static final class BooleanMapper implements ValueMapper<Value>
    {
        @Override
        public Value mapPath( PathValue value )
        {
            return value.size() > 0 ? Values.TRUE : Values.FALSE;
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
            return value.length() > 0 ? Values.TRUE : Values.FALSE;
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
