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
package org.neo4j.kernel.impl.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.spatial.Geometry;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;
import org.neo4j.values.virtual.VirtualValues;

import static org.neo4j.values.virtual.VirtualValues.map;

public final class ValueUtils
{
    private ValueUtils()
    {
        throw new UnsupportedOperationException( "do not instantiate" );
    }

    /**
     * Creates an AnyValue by doing type inspection. Do not use in production code where performance is important.
     *
     * @param object the object to turned into a AnyValue
     * @return the AnyValue corresponding to object.
     */
    @SuppressWarnings( "unchecked" )
    public static AnyValue of( Object object )
    {
        Value value = Values.unsafeOf( object, true );
        if ( value != null )
        {
            return value;
        }
        else
        {
            if ( object instanceof Entity )
            {
                if ( object instanceof Node )
                {
                    return fromNodeProxy( (Node) object );
                }
                else if ( object instanceof Relationship )
                {
                    return fromRelationshipProxy( (Relationship) object );
                }
                else
                {
                    throw new IllegalArgumentException( "Unknown entity + " + object.getClass().getName() );
                }
            }
            else if ( object instanceof Iterable<?> )
            {
                if ( object instanceof Path )
                {
                    return fromPath( (Path) object );
                }
                else if ( object instanceof List<?> )
                {
                    return asListValue( (List<?>) object );
                }
                else
                {
                    return asListValue( (Iterable<?>) object );
                }
            }
            else if ( object instanceof Map<?,?> )
            {
                return asMapValue( (Map<String,Object>) object );
            }
            else if ( object instanceof Iterator<?> )
            {
                ArrayList<Object> objects = new ArrayList<>();
                Iterator<?> iterator = (Iterator<?>) object;
                while ( iterator.hasNext() )
                {
                    objects.add( iterator.next() );
                }
                return asListValue( objects );
            }
            else if ( object instanceof Object[] )
            {
                Object[] array = (Object[]) object;
                AnyValue[] anyValues = new AnyValue[array.length];
                for ( int i = 0; i < array.length; i++ )
                {
                    anyValues[i] = ValueUtils.of( array[i] );
                }
                return VirtualValues.list( anyValues );
            }
            else if ( object instanceof Stream<?> )
            {
                return asListValue( ((Stream<Object>) object).collect( Collectors.toList() ) );
            }
            else if ( object instanceof Geometry )
            {
                return asGeometryValue( (Geometry) object );
            }
            else if ( object instanceof VirtualNodeValue || object instanceof VirtualRelationshipValue )
            {
                return (AnyValue) object;
            }
            else
            {
                throw new IllegalArgumentException(
                        String.format( "Cannot convert %s to AnyValue", object.getClass().getName() ) );
            }
        }
    }

    public static PointValue asPointValue( Point point )
    {
        return toPoint( point );
    }

    public static PointValue asGeometryValue( Geometry geometry )
    {
        if ( !geometry.getGeometryType().equals( "Point" ) )
        {
            throw new IllegalArgumentException( "Cannot handle geometry type: " + geometry.getCRS().getType() );
        }
        return toPoint( geometry );
    }

    private static PointValue toPoint( Geometry geometry )
    {
        List<Double> coordinate = geometry.getCoordinates().get( 0 ).getCoordinate();
        double[] primitiveCoordinate = new double[coordinate.size()];
        for ( int i = 0; i < coordinate.size(); i++ )
        {
            primitiveCoordinate[i] = coordinate.get( i );
        }

        return Values.pointValue( CoordinateReferenceSystem.get( geometry.getCRS() ), primitiveCoordinate );
    }

    public static ListValue asListValue( List<?> collection )
    {
        ArrayList<AnyValue> values = new ArrayList<>( collection.size() );
        for ( Object o : collection )
        {
            values.add( ValueUtils.of( o ) );
        }
        return VirtualValues.fromList( values );
    }

    public static ListValue asListValue( Iterable<?> collection )
    {
        ArrayList<AnyValue> values = new ArrayList<>();
        for ( Object o : collection )
        {
            values.add( ValueUtils.of( o ) );
        }
        return VirtualValues.fromList( values );
    }

    public static AnyValue asNodeOrEdgeValue( PropertyContainer container )
    {
        if ( container instanceof Node )
        {
            return fromNodeProxy( (Node) container );
        }
        else if ( container instanceof Relationship )
        {
            return fromRelationshipProxy( (Relationship) container );
        }
        else
        {
            throw new IllegalArgumentException(
                    "Cannot produce a node or edge from " + container.getClass().getName() );
        }
    }

    public static ListValue asListOfEdges( Iterable<Relationship> rels )
    {
        return VirtualValues.list( StreamSupport.stream( rels.spliterator(), false )
                .map( ValueUtils::fromRelationshipProxy ).toArray( RelationshipValue[]::new ) );
    }

    public static ListValue asListOfEdges( Relationship[] rels )
    {
        RelationshipValue[] relValues = new RelationshipValue[rels.length];
        for ( int i = 0; i < relValues.length; i++ )
        {
            relValues[i] = fromRelationshipProxy( rels[i] );
        }
        return VirtualValues.list( relValues );
    }

    public static MapValue asMapValue( Map<String,Object> map )
    {
        HashMap<String,AnyValue> newMap = new HashMap<>( map.size() );
        for ( Map.Entry<String,Object> entry : map.entrySet() )
        {
            newMap.put( entry.getKey(), ValueUtils.of( entry.getValue() ) );
        }

        return map( newMap );
    }

    public static MapValue asParameterMapValue( Map<String,Object> map )
    {
        HashMap<String,AnyValue> newMap = new HashMap<>( map.size() );
        for ( Map.Entry<String,Object> entry : map.entrySet() )
        {
            try
            {
                newMap.put( entry.getKey(), ValueUtils.of( entry.getValue() ) );
            }
            catch ( IllegalArgumentException e )
            {
                newMap.put( entry.getKey(), VirtualValues.error( e ) );
            }
        }

        return map( newMap );
    }

    public static NodeValue fromNodeProxy( Node node )
    {
        return new NodeProxyWrappingNodeValue( node );
    }

    public static RelationshipValue fromRelationshipProxy( Relationship relationship )
    {
        return new RelationshipProxyWrappingValue( relationship );
    }

    public static PathValue fromPath( Path path )
    {
        return new PathWrappingPathValue( path );
    }

    /**
     * Creates a {@link Value} from the given object, or if it is already a Value it is returned as it is.
     * <p>
     * This is different from {@link Values#of} which often explicitly fails or creates a new copy
     * if given a Value.
     */
    public static Value asValue( Object value )
    {
        if ( value instanceof Value )
        {
            return (Value) value;
        }
        return Values.of( value );
    }

    /**
     * Creates an {@link AnyValue} from the given object, or if it is already an AnyValue it is returned as it is.
     * <p>
     * This is different from {@link ValueUtils#of} which often explicitly fails or creates a new copy
     * if given an AnyValue.
     */
    public static AnyValue asAnyValue( Object value )
    {
        if ( value instanceof AnyValue )
        {
            return (AnyValue) value;
        }
        return ValueUtils.of( value );
    }

    public static NodeValue asNodeValue( Object object )
    {
        if ( object instanceof NodeValue )
        {
            return (NodeValue) object;
        }
        if ( object instanceof Node )
        {
            return fromNodeProxy( (Node) object );
        }
        throw new IllegalArgumentException(
                "Cannot produce a node from " + object.getClass().getName() );
    }

    public static RelationshipValue asRelationshipValue( Object object )
    {
        if ( object instanceof RelationshipValue )
        {
            return (RelationshipValue) object;
        }
        if ( object instanceof Relationship )
        {
            return fromRelationshipProxy( (Relationship) object );
        }
        throw new IllegalArgumentException(
                "Cannot produce a relationship from " + object.getClass().getName() );
    }

    public static LongValue asLongValue( Object object )
    {
        if ( object instanceof LongValue )
        {
            return (LongValue) object;
        }
        if ( object instanceof Long )
        {
            return Values.longValue( (long) object );
        }
        throw new IllegalArgumentException(
                "Cannot produce a long from " + object.getClass().getName() );
    }

    public static DoubleValue asDoubleValue( Object object )
    {
        if ( object instanceof DoubleValue )
        {
            return (DoubleValue) object;
        }
        if ( object instanceof Double )
        {
            return Values.doubleValue( (double) object );
        }
        throw new IllegalArgumentException(
                "Cannot produce a double from " + object.getClass().getName() );
    }

    public static BooleanValue asBooleanValue( Object object )
    {
        if ( object instanceof BooleanValue )
        {
            return (BooleanValue) object;
        }
        if ( object instanceof Boolean )
        {
            return Values.booleanValue( (boolean) object );
        }
        throw new IllegalArgumentException(
                "Cannot produce a boolean from " + object.getClass().getName() );
    }

    public static TextValue asTextValue( Object object )
    {
        if ( object instanceof TextValue )
        {
            return (TextValue) object;
        }
        if ( object instanceof String )
        {
            return Values.stringValue( (String) object );
        }
        throw new IllegalArgumentException(
                "Cannot produce a string from " + object.getClass().getName() );
    }

}

