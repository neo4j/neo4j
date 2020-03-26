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
package org.neo4j.kernel.impl.util;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.spatial.Geometry;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.util.CalledFromGeneratedCode;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.IntValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualValues;

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
        if ( object instanceof AnyValue )
        {
            return (AnyValue) object;
        }
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
                    return fromNodeEntity( (Node) object );
                }
                else if ( object instanceof Relationship )
                {
                    return fromRelationshipEntity( (Relationship) object );
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
                    return asListValue( (List<Object>) object );
                }
                else
                {
                    return asListValue( (Iterable<Object>) object );
                }
            }
            else if ( object instanceof Map<?,?> )
            {
                return asMapValue( (Map<String,Object>) object );
            }
            else if ( object instanceof Iterator<?> )
            {
                ListValueBuilder builder = ListValueBuilder.newListBuilder();
                Iterator<?> iterator = (Iterator<?>) object;
                while ( iterator.hasNext() )
                {
                    builder.add( ValueUtils.of( iterator.next() ) );
                }
                return builder.build();
            }
            else if ( object instanceof Object[] )
            {
                Object[] array = (Object[]) object;
                if ( array.length == 0 )
                {
                    return VirtualValues.EMPTY_LIST;
                }

                ListValueBuilder builder = ListValueBuilder.newListBuilder( array.length );
                for ( Object o : array )
                {
                    builder.add( ValueUtils.of( o ) );
                }
                return builder.build();
            }
            else if ( object instanceof Stream<?> )
            {
                return asListValue( ((Stream<Object>) object).collect( Collectors.toList() ) );
            }
            else if ( object instanceof Geometry )
            {
                return asGeometryValue( (Geometry) object );
            }
            else
            {
                ClassLoader classLoader = object.getClass().getClassLoader();
                throw new IllegalArgumentException(
                        String.format( "Cannot convert %s of type %s to AnyValue, classloader=%s, classloader-name=%s",
                                object,
                                object.getClass().getName(),
                                classLoader != null ? classLoader.toString() : "null",
                                classLoader != null ? classLoader.getName() : "null" )
                );
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
        int size = collection.size();
        if ( size == 0 )
        {
            return VirtualValues.EMPTY_LIST;
        }

        ListValueBuilder values = ListValueBuilder.newListBuilder( size );
        for ( Object o : collection )
        {
            values.add( ValueUtils.of( o ) );
        }
        return values.build();
    }

    public static ListValue asListValue( Iterable<?> collection )
    {
        ListValueBuilder values = ListValueBuilder.newListBuilder();
        for ( Object o : collection )
        {
            values.add( ValueUtils.of( o ) );
        }
        return values.build();
    }

    public static AnyValue asNodeOrEdgeValue( Entity container )
    {
        if ( container instanceof Node )
        {
            return fromNodeEntity( (Node) container );
        }
        else if ( container instanceof Relationship )
        {
            return fromRelationshipEntity( (Relationship) container );
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
                .map( ValueUtils::fromRelationshipEntity ).toArray( RelationshipValue[]::new ) );
    }

    public static ListValue asListOfEdges( Relationship[] rels )
    {
        if ( rels.length == 0 )
        {
            return VirtualValues.EMPTY_LIST;
        }

        ListValueBuilder relValues = ListValueBuilder.newListBuilder( rels.length );
        for ( Relationship rel : rels )
        {
            relValues.add( fromRelationshipEntity( rel ) );
        }
        return relValues.build();
    }

    public static MapValue asMapValue( Map<String,?> map )
    {
        int size = map.size();
        if ( size == 0 )
        {
            return VirtualValues.EMPTY_MAP;
        }

        MapValueBuilder builder = new MapValueBuilder( size );
        for ( Map.Entry<String,?> entry : map.entrySet() )
        {
            builder.add( entry.getKey(), ValueUtils.of( entry.getValue() ) );
        }
        return builder.build();
    }

    public static MapValue asParameterMapValue( Map<String,Object> map )
    {
        int size = map.size();
        if ( size == 0 )
        {
            return VirtualValues.EMPTY_MAP;
        }

        MapValueBuilder builder = new MapValueBuilder( size );
        for ( Map.Entry<String,Object> entry : map.entrySet() )
        {
            try
            {
                builder.add( entry.getKey(), ValueUtils.of( entry.getValue() ) );
            }
            catch ( IllegalArgumentException e )
            {
                builder.add( entry.getKey(), VirtualValues.error( e ) );
            }
        }

        return builder.build();
    }

    public static NodeValue fromNodeEntity( Node node )
    {
        return new NodeEntityWrappingNodeValue( node );
    }

    public static RelationshipValue fromRelationshipEntity( Relationship relationship )
    {
        return new RelationshipEntityWrappingValue( relationship );
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

    @CalledFromGeneratedCode
    public static NodeValue asNodeValue( Object object )
    {
        if ( object instanceof NodeValue )
        {
            return (NodeValue) object;
        }
        if ( object instanceof Node )
        {
            return fromNodeEntity( (Node) object );
        }
        throw new IllegalArgumentException(
                "Cannot produce a node from " + object.getClass().getName() );
    }

    @CalledFromGeneratedCode
    public static RelationshipValue asRelationshipValue( Object object )
    {
        if ( object instanceof RelationshipValue )
        {
            return (RelationshipValue) object;
        }
        if ( object instanceof Relationship )
        {
            return fromRelationshipEntity( (Relationship) object );
        }
        throw new IllegalArgumentException(
                "Cannot produce a relationship from " + object.getClass().getName() );
    }

    @CalledFromGeneratedCode
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
        if ( object instanceof IntValue )
        {
            return Values.longValue(((IntValue) object).longValue());
        }
        if ( object instanceof Integer )
        {
            return Values.longValue( (int) object );
        }

        throw new IllegalArgumentException(
                "Cannot produce a long from " + object.getClass().getName() );
    }

    @CalledFromGeneratedCode
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

    @CalledFromGeneratedCode
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

    @CalledFromGeneratedCode
    public static TextValue asTextValue( Object object )
    {
        if ( object instanceof TextValue )
        {
            return (TextValue) object;
        }
        if ( object instanceof String )
        {
            return Values.utf8Value( (String) object );
        }
        throw new IllegalArgumentException(
                "Cannot produce a string from " + object.getClass().getName() );
    }

}

