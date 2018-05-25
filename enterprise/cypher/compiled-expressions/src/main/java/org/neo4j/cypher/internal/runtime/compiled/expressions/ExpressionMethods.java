/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions;

import org.opencypher.v9_0.util.CypherTypeException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.AnyValue;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.StringValue;
import org.neo4j.values.storable.TemporalValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;
import org.neo4j.values.virtual.VirtualValues;

import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.stringValue;

/**
 * This class contains static helper methods used by the compiled expressions
 */
public final class ExpressionMethods
{
    private static final BooleanMapper BOOLEAN_MAPPER = new BooleanMapper();

    private ExpressionMethods()
    {
        throw new UnsupportedOperationException( "Do not instantiate" );
    }

    public static DoubleValue sin( AnyValue in )
    {
        if ( in instanceof NumberValue )
        {
            return doubleValue( Math.sin( ((NumberValue) in).doubleValue() ) );
        }
        else
        {
            throw new CypherTypeException( "sin requires numbers", null );
        }
    }

    public static DoubleValue round( AnyValue in )
    {
        if ( in instanceof NumberValue )
        {
            return doubleValue( Math.round( ((NumberValue) in).doubleValue() ) );
        }
        else
        {
            throw new CypherTypeException( "round requires numbers", null );
        }
    }

    public static DoubleValue rand()
    {
        return doubleValue( ThreadLocalRandom.current().nextDouble() );
    }

    //TODO this is horrible spaghetti code, we should push most of this down to AnyValue
    public static AnyValue add( AnyValue lhs, AnyValue rhs )
    {
        if ( lhs == NO_VALUE || rhs == NO_VALUE )
        {
            return NO_VALUE;
        }
        if ( lhs instanceof NumberValue && rhs instanceof NumberValue )
        {
            return ((NumberValue) lhs).plus( (NumberValue) rhs );
        }
        //List addition
        boolean lhsIsListValue = lhs instanceof ListValue;
        if ( lhsIsListValue && rhs instanceof ListValue )
        {
            return VirtualValues.concat( (ListValue) lhs, (ListValue) rhs );
        }
        else if ( lhsIsListValue )
        {
            return VirtualValues.appendToList( (ListValue) lhs, rhs );
        }
        else if ( rhs instanceof ListValue )
        {
            return VirtualValues.prependToList( (ListValue) rhs, lhs );
        }

        // String addition
        if ( lhs instanceof TextValue )
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
                    //TODO this seems wrong but it is what we currently do in compiled runtime
                    return stringValue( ((TextValue) lhs).stringValue() + String.valueOf( rhs ) );
                }
            }
        }
        if ( rhs instanceof TextValue )
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
                    //TODO this seems wrong but it is what we currently do in compiled runtime
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
                String.format( "Don't know how to add `%s` and `%s`", lhs, rhs ), null );
    }

    public static AnyValue subtract( AnyValue lhs, AnyValue rhs )
    {
        if ( lhs == NO_VALUE || rhs == NO_VALUE )
        {
            return NO_VALUE;
        }
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
                String.format( "Don't know how to subtract `%s` and `%s`", lhs, rhs ), null );
    }

    public static AnyValue multiply( AnyValue lhs, AnyValue rhs )
    {
        if ( lhs == NO_VALUE || rhs == NO_VALUE )
        {
            return NO_VALUE;
        }
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
                String.format( "Don't know how to subtract `%s` and `%s`", lhs, rhs ), null );
    }

    //data access
    public static Value nodeProperty( Transaction tx, long node, int property )
    {
        CursorFactory cursors = tx.cursors();
        try ( NodeCursor nodes = cursors.allocateNodeCursor();
              PropertyCursor properties = cursors.allocatePropertyCursor() )
        {
            tx.dataRead().singleNode( node, nodes );
            if ( nodes.next() )
            {
                nodes.properties( properties );
                return property( properties, property );
            }
            return Values.NO_VALUE;
        }
    }

    public static Value relationshipProperty( Transaction tx, long relationship, int property )
    {
        CursorFactory cursors = tx.cursors();
        try ( RelationshipScanCursor relationships = cursors.allocateRelationshipScanCursor();
              PropertyCursor properties = cursors.allocatePropertyCursor() )
        {
            tx.dataRead().singleRelationship( relationship, relationships );
            if ( relationships.next() )
            {
                relationships.properties( properties );
                return property( properties, property );
            }
            return Values.NO_VALUE;
        }
    }

    //boolean operations

    public static Value or( AnyValue... args )
    {
        for ( AnyValue arg : args )
        {
            if ( arg == NO_VALUE )
            {
                return NO_VALUE;
            }

            if ( arg.map( BOOLEAN_MAPPER ) )
            {
                return Values.TRUE;
            }
        }
        return Values.FALSE;
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
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null);
        }

        @Override
        public Boolean mapRelationship( VirtualRelationshipValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null);
        }

        @Override
        public Boolean mapMap( MapValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null);
        }

        @Override
        public Boolean mapNoValue()
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + NO_VALUE, null);
        }

        @Override
        public Boolean mapSequence( SequenceValue value )
        {
            return value.length() > 0;
        }

        @Override
        public Boolean mapText( TextValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null);
        }

        @Override
        public Boolean mapBoolean( BooleanValue value )
        {
            return value.booleanValue();
        }

        @Override
        public Boolean mapNumber( NumberValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null);

        }

        @Override
        public Boolean mapDateTime( DateTimeValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null);

        }

        @Override
        public Boolean mapLocalDateTime( LocalDateTimeValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null);

        }

        @Override
        public Boolean mapDate( DateValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null);
        }

        @Override
        public Boolean mapTime( TimeValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null);

        }

        @Override
        public Boolean mapLocalTime( LocalTimeValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null);

        }

        @Override
        public Boolean mapDuration( DurationValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null);

        }

        @Override
        public Boolean mapPoint( PointValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null);
        }
    }

    private static Value property( PropertyCursor properties, int property )
    {
        while ( properties.next() )
        {
            if ( properties.propertyKey() == property )
            {
                return properties.propertyValue();
            }
        }
        return Values.NO_VALUE;
    }
}
