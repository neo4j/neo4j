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

    public static Value or( AnyValue... args )
    {
        boolean seenNull = false;
        for ( AnyValue arg : args )
        {
            if ( arg == NO_VALUE )
            {
                seenNull = true;
                continue;
            }

            if ( arg.map( BOOLEAN_MAPPER ) )
            {
                return Values.TRUE;
            }
        }
        return seenNull ? NO_VALUE : Values.FALSE;
    }

    public static Value xor( AnyValue lhs, AnyValue rhs )
    {
        boolean seenNull = false;
        if ( lhs == NO_VALUE || rhs == NO_VALUE )
        {
            return NO_VALUE;
        }

        return lhs.map( BOOLEAN_MAPPER ) ^ rhs.map( BOOLEAN_MAPPER ) ? Values.TRUE : Values.FALSE;
    }

    public static Value and( AnyValue... args )
    {
        boolean seenNull = false;
        for ( AnyValue arg : args )
        {
            if ( arg == NO_VALUE )
            {
                seenNull = true;
                continue;
            }

            if ( !arg.map( BOOLEAN_MAPPER ) )
            {
                return Values.FALSE;
            }
        }
        return seenNull ? NO_VALUE : Values.TRUE;
    }

    public static Value not( AnyValue in )
    {
        if ( in == NO_VALUE )
        {
            return NO_VALUE;
        }

        return !in.map( BOOLEAN_MAPPER ) ? Values.TRUE : Values.FALSE;
    }

    public static Value equals( AnyValue lhs, AnyValue rhs )
    {
        Boolean equals = lhs.ternaryEquals( rhs );
        if ( equals == null )
        {
            return NO_VALUE;
        }
        else
        {
            return equals ? Values.TRUE : Values.FALSE;
        }
    }

    public static Value notEquals( AnyValue lhs, AnyValue rhs )
    {
        Boolean equals = lhs.ternaryEquals( rhs );
        if ( equals == null )
        {
            return NO_VALUE;
        }
        else
        {
            return equals ? Values.FALSE : Values.TRUE;
        }
    }

    private static final class BooleanMapper implements ValueMapper<Boolean>
    {
        @Override
        public Boolean mapPath( PathValue value )
        {
            return value.size() > 0;
        }

        @Override
        public Boolean mapNode( VirtualNodeValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null );
        }

        @Override
        public Boolean mapRelationship( VirtualRelationshipValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null );
        }

        @Override
        public Boolean mapMap( MapValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null );
        }

        @Override
        public Boolean mapNoValue()
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + NO_VALUE, null );
        }

        @Override
        public Boolean mapSequence( SequenceValue value )
        {
            return value.length() > 0;
        }

        @Override
        public Boolean mapText( TextValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null );
        }

        @Override
        public Boolean mapBoolean( BooleanValue value )
        {
            return value.booleanValue();
        }

        @Override
        public Boolean mapNumber( NumberValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null );
        }

        @Override
        public Boolean mapDateTime( DateTimeValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null );
        }

        @Override
        public Boolean mapLocalDateTime( LocalDateTimeValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null );
        }

        @Override
        public Boolean mapDate( DateValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null );
        }

        @Override
        public Boolean mapTime( TimeValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null );
        }

        @Override
        public Boolean mapLocalTime( LocalTimeValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null );
        }

        @Override
        public Boolean mapDuration( DurationValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null );
        }

        @Override
        public Boolean mapPoint( PointValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null );
        }
    }
}
