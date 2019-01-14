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

import java.util.HashMap;
import java.util.Map;

import org.neo4j.cypher.internal.runtime.DbAccess;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.values.AnyValue;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.FloatingPointValue;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;
import org.neo4j.values.virtual.VirtualValues;

import static java.lang.String.format;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTAny;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTBoolean;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTDate;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTDateTime;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTDuration;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTFloat;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTGeometry;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTInteger;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTLocalDateTime;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTLocalTime;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTMap;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTNode;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTNumber;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTPath;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTPoint;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTRelationship;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTString;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTTime;
import static org.neo4j.values.SequenceValue.IterationPreference.RANDOM_ACCESS;
import static org.neo4j.values.storable.Values.NO_VALUE;

@SuppressWarnings( {"unused", "WeakerAccess"} )
public final class CypherCoercions
{
    private CypherCoercions()
    {
        throw new UnsupportedOperationException( "do not instantiate" );
    }

    public static TextValue asTextValue( AnyValue value )
    {
        if ( !(value instanceof TextValue) )
        {
            throw cantCoerce( value, "String" );
        }
        return (TextValue) value;
    }

    public static NodeValue asNodeValue( AnyValue value )
    {
        if ( !(value instanceof NodeValue) )
        {
            throw cantCoerce( value, "Node" );
        }
        return (NodeValue) value;
    }

    public static RelationshipValue asRelationshipValue( AnyValue value )
    {
        if ( !(value instanceof RelationshipValue) )
        {
            throw cantCoerce( value, "Relationship" );
        }
        return (RelationshipValue) value;
    }

    public static PathValue asPathValue( AnyValue value )
    {
        if ( !(value instanceof PathValue) )
        {
            throw cantCoerce( value, "Path" );
        }
        return (PathValue) value;
    }

    public static IntegralValue asIntegralValue( AnyValue value )
    {
        if ( !(value instanceof NumberValue) )
        {
            throw cantCoerce( value, "Integer" );
        }
        return Values.longValue( ((NumberValue) value).longValue() );
    }

    public static FloatingPointValue asFloatingPointValue( AnyValue value )
    {
        if ( !(value instanceof NumberValue) )
        {
            throw cantCoerce( value, "Float" );
        }
        return Values.doubleValue( ((NumberValue) value).doubleValue() );
    }

    public static BooleanValue asBooleanValue( AnyValue value )
    {
        if ( !(value instanceof BooleanValue) )
        {
            throw cantCoerce( value, "Boolean" );
        }
        return (BooleanValue) value;
    }

    public static NumberValue asNumberValue( AnyValue value )
    {
        if ( !(value instanceof NumberValue) )
        {
            throw cantCoerce( value, "Number" );
        }
        return (NumberValue) value;
    }

    public static PointValue asPointValue( AnyValue value )
    {
        if ( !(value instanceof PointValue) )
        {
            throw cantCoerce( value, "Point" );
        }
        return (PointValue) value;
    }

    public static DateValue asDateValue( AnyValue value )
    {
        if ( !(value instanceof DateValue) )
        {
            throw cantCoerce( value, "Date" );
        }
        return (DateValue) value;
    }

    public static TimeValue asTimeValue( AnyValue value )
    {
        if ( !(value instanceof TimeValue) )
        {
            throw cantCoerce( value, "Time" );
        }
        return (TimeValue) value;
    }

    public static LocalTimeValue asLocalTimeValue( AnyValue value )
    {
        if ( !(value instanceof LocalTimeValue) )
        {
            throw cantCoerce( value, "LocalTime" );
        }
        return (LocalTimeValue) value;
    }

    public static LocalDateTimeValue asLocalDateTimeValue( AnyValue value )
    {
        if ( !(value instanceof LocalDateTimeValue) )
        {
            throw cantCoerce( value, "LocalDateTime" );
        }
        return (LocalDateTimeValue) value;
    }

    public static DateTimeValue asDateTimeValue( AnyValue value )
    {
        if ( !(value instanceof DateTimeValue) )
        {
            throw cantCoerce( value, "DateTime" );
        }
        return (DateTimeValue) value;
    }

    public static DurationValue asDurationValue( AnyValue value )
    {
        if ( !(value instanceof DurationValue) )
        {
            throw cantCoerce( value, "Duration" );
        }
        return (DurationValue) value;
    }

    public static MapValue asMapValue( AnyValue value, DbAccess access )
    {
        if ( value instanceof MapValue )
        {
            return (MapValue) value;
        }
        else if ( value instanceof NodeValue )
        {
            return access.nodeAsMap( ((NodeValue) value).id() );
        }
        else if ( value instanceof RelationshipValue )
        {
            return access.relationshipAsMap( ((RelationshipValue) value).id() );
        }
        else
        {
            throw cantCoerce( value, "Map" );
        }
    }

    public static ListValue asList( AnyValue value, Neo4jTypes.AnyType innerType, DbAccess access )
    {
        return new ListCoercer().apply( value, innerType, access );
    }

    private static CypherTypeException cantCoerce( AnyValue value, String type )
    {
        return new CypherTypeException( format( "Can't coerce `%s` to %s", value, type ), null );
    }

    private static class ListMapper implements ValueMapper<ListValue>
    {

        @Override
        public ListValue mapPath( PathValue value )
        {
            return null;
        }

        @Override
        public ListValue mapNode( VirtualNodeValue value )
        {
            return null;
        }

        @Override
        public ListValue mapRelationship( VirtualRelationshipValue value )
        {
            return null;
        }

        @Override
        public ListValue mapMap( MapValue value )
        {
            return null;
        }

        @Override
        public ListValue mapNoValue()
        {
            return null;
        }

        @Override
        public ListValue mapSequence( SequenceValue value )
        {
            return null;
        }

        @Override
        public ListValue mapText( TextValue value )
        {
            return null;
        }

        @Override
        public ListValue mapBoolean( BooleanValue value )
        {
            return null;
        }

        @Override
        public ListValue mapNumber( NumberValue value )
        {
            return null;
        }

        @Override
        public ListValue mapDateTime( DateTimeValue value )
        {
            return null;
        }

        @Override
        public ListValue mapLocalDateTime( LocalDateTimeValue value )
        {
            return null;
        }

        @Override
        public ListValue mapDate( DateValue value )
        {
            return null;
        }

        @Override
        public ListValue mapTime( TimeValue value )
        {
            return null;
        }

        @Override
        public ListValue mapLocalTime( LocalTimeValue value )
        {
            return null;
        }

        @Override
        public ListValue mapDuration( DurationValue value )
        {
            return null;
        }

        @Override
        public ListValue mapPoint( PointValue value )
        {
            return null;
        }
    }

    @FunctionalInterface
    interface Coercer
    {
        AnyValue apply( AnyValue value, Neo4jTypes.AnyType coerceTo, DbAccess access );
    }

    private static final Map<Class<? extends Neo4jTypes.AnyType>,Coercer> CONVERTERS = new HashMap<>();

    private AnyValue coerceTo( AnyValue value, DbAccess access, Neo4jTypes.AnyType types )
    {
        Coercer function = CONVERTERS.get( types.getClass() );

        return function.apply( value, types, access );
    }

    private static class ListCoercer implements Coercer
    {
        @Override
        public ListValue apply( AnyValue value, Neo4jTypes.AnyType innerType, DbAccess access )
        {
            //Fast route
            if ( innerType == NTAny )
            {
                return fastListConversion( value );
            }

            //slow route, recursively convert the list
            if ( !(value instanceof SequenceValue) )
            {
                throw cantCoerce( value, "List" );
            }
            SequenceValue listValue = (SequenceValue) value;
            Coercer innerCoercer = CONVERTERS.get( innerType.getClass() );
            AnyValue[] coercedValues = new AnyValue[listValue.length()];
            Neo4jTypes.AnyType nextInner = nextInner( innerType );
            if ( listValue.iterationPreference() == RANDOM_ACCESS )
            {
                for ( int i = 0; i < coercedValues.length; i++ )
                {
                    AnyValue nextItem = listValue.value( i );
                    coercedValues[i] = nextItem == NO_VALUE ? NO_VALUE : innerCoercer.apply( nextItem, nextInner, access );
                }
            }
            else
            {
                int i = 0;
                for ( AnyValue anyValue : listValue )
                {
                    AnyValue nextItem = listValue.value( i );
                    coercedValues[i++] = nextItem == NO_VALUE ? NO_VALUE : innerCoercer.apply( anyValue, nextInner, access );
                }
            }
            return VirtualValues.list( coercedValues );
        }
    }

    private static Neo4jTypes.AnyType nextInner( Neo4jTypes.AnyType type )
    {
        if ( type instanceof Neo4jTypes.ListType )
        {
            return ((Neo4jTypes.ListType) type).innerType();
        }
        else
        {
            return type;
        }
    }

    private static ListValue fastListConversion( AnyValue value )
    {
        if ( value instanceof ListValue )
        {
            return (ListValue) value;
        }
        else if ( value instanceof ArrayValue )
        {
            return VirtualValues.fromArray( (ArrayValue) value );
        }
        else if ( value instanceof PathValue )
        {
            return ((PathValue) value).asList();
        }
        throw cantCoerce( value, "List" );
    }

    static
    {
        CONVERTERS.put( NTAny.getClass(), ( a, ignore1, ignore2 ) -> a );
        CONVERTERS.put( NTString.getClass(), ( a, ignore1, ignore2 ) -> asTextValue( a ) );
        CONVERTERS.put( NTNumber.getClass(), ( a, ignore1, ignore2 ) -> asNumberValue( a ) );
        CONVERTERS.put( NTInteger.getClass(), ( a, ignore1, ignore2 ) -> asIntegralValue( a ) );
        CONVERTERS.put( NTFloat.getClass(), ( a, ignore1, ignore2 ) -> asFloatingPointValue( a ) );
        CONVERTERS.put( NTBoolean.getClass(), ( a, ignore1, ignore2 ) -> asBooleanValue( a ) );
        CONVERTERS.put( NTMap.getClass(), ( a, ignore, c ) -> asMapValue( a, c ) );
        CONVERTERS.put( NTNode.getClass(), ( a, ignore1, ignore2 ) -> asNodeValue( a ) );
        CONVERTERS.put( NTRelationship.getClass(), ( a, ignore1, ignore2 ) -> asRelationshipValue( a ) );
        CONVERTERS.put( NTPath.getClass(), ( a, ignore1, ignore2 ) -> asPathValue( a ) );
        CONVERTERS.put( NTGeometry.getClass(), ( a, ignore1, ignore2 ) -> asPointValue( a ) );
        CONVERTERS.put( NTPoint.getClass(), ( a, ignore1, ignore2 ) -> asPointValue( a ) );
        CONVERTERS.put( NTDateTime.getClass(), ( a, ignore1, ignore2 ) -> asDateTimeValue( a ) );
        CONVERTERS.put( NTLocalDateTime.getClass(), ( a, ignore1, ignore2 ) -> asLocalDateTimeValue( a ) );
        CONVERTERS.put( NTDate.getClass(), ( a, ignore1, ignore2 ) -> asDateValue( a ) );
        CONVERTERS.put( NTTime.getClass(), ( a, ignore1, ignore2 ) -> asTimeValue( a ) );
        CONVERTERS.put( NTLocalTime.getClass(), ( a, ignore1, ignore2 ) -> asLocalTimeValue( a ) );
        CONVERTERS.put( NTDuration.getClass(), ( a, ignore1, ignore2 ) -> asDurationValue( a ) );
        CONVERTERS.put( Neo4jTypes.ListType.class, new ListCoercer() );
    }
}
