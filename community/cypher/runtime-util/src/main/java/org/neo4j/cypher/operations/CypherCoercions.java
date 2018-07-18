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

import org.neo4j.cypher.internal.runtime.DbAccess;
import org.neo4j.values.AnyValue;
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
import org.neo4j.values.virtual.VirtualValues;

import static java.lang.String.format;
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

    public static ListValue asListValueFailOnPaths( AnyValue value )
    {
        if ( value instanceof PathValue )
        {
            throw cantCoerce( value, "List" );
        }
        else
        {
            return asList( value );
        }
    }

    public static ListValue asListValueSupportPaths( AnyValue value )
    {
        if ( value instanceof PathValue )
        {
            return ((PathValue) value).asList();
        }
        else
        {
            return asList( value );
        }
    }

    private static CypherTypeException cantCoerce( AnyValue value, String type )
    {
        return new CypherTypeException( format( "Can't coerce `%s` to %s", value, type ), null );
    }

    private static ListValue asList( AnyValue value )
    {
        if ( value instanceof ListValue )
        {
            return (ListValue) value;
        }
        else if ( value instanceof ArrayValue )
        {
            return VirtualValues.fromArray( (ArrayValue) value );
        }
        else if ( value == NO_VALUE )
        {
            return VirtualValues.EMPTY_LIST;
        }
        else
        {
            return VirtualValues.list( value );
        }
    }
}
