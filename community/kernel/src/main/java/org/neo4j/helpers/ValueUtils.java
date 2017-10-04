/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.helpers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.spatial.Geometry;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.CoordinateReferenceSystem;
import org.neo4j.values.virtual.EdgeValue;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.PointValue;
import org.neo4j.values.virtual.VirtualValues;

import static java.util.stream.StreamSupport.stream;
import static org.neo4j.values.virtual.VirtualValues.list;
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
        try
        {
            return Values.of( object );
        }
        catch ( IllegalArgumentException e )
        {
            if ( object instanceof Node )
            {
                return fromNodeProxy( (Node) object );
            }
            else if ( object instanceof Relationship )
            {
                return fromRelationshipProxy( (Relationship) object );
            }
            else if ( object instanceof Path )
            {
                return asPathValue( (Path) object );
            }
            else if ( object instanceof Map<?,?> )
            {
                return asMapValue( (Map<String,Object>) object );
            }
            else if ( object instanceof Iterable<?> )
            {
                return asListValue( (Iterable<?>) object );
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
            else if ( object instanceof Stream<?> )
            {
                return asListValue( ((Stream<Object>) object).collect( Collectors.toList() ) );
            }
            else if ( object instanceof Point )
            {
                return asPointValue( (Point) object );
            }
            else if ( object instanceof Geometry )
            {
                return asPointValue( (Geometry) object );
            }
            else if ( object instanceof Object[] )
            {
                Object[] array = (Object[]) object;
                AnyValue[] anyValues = new AnyValue[array.length];
                for ( int i = 0; i < array.length; i++ )
                {
                    anyValues[i] = of( array[i] );
                }
                return VirtualValues.list( anyValues );
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

    public static PointValue asPointValue( Geometry geometry )
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
        if ( geometry.getCRS().getCode() == CoordinateReferenceSystem.Cartesian.code )
        {
            return VirtualValues.pointCartesian( coordinate.get( 0 ), coordinate.get( 1 ) );
        }
        else if ( geometry.getCRS().getCode() == CoordinateReferenceSystem.WGS84.code )
        {
            return VirtualValues.pointGeographic( coordinate.get( 0 ), coordinate.get( 1 ) );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown coordinate reference system " + geometry.getCRS() );
        }
    }

    public static ListValue asListValue( Iterable<?> collection )
    {
        AnyValue[] anyValues =
                Iterables.stream( collection ).map( ValueUtils::of ).toArray( AnyValue[]::new );
        return list( anyValues );
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

    public static PathValue asPathValue( Path path )
    {
        NodeValue[] nodes = stream( path.nodes().spliterator(), false )
                .map( ValueUtils::fromNodeProxy ).toArray( NodeValue[]::new );
        EdgeValue[] edges = stream( path.relationships().spliterator(), false )
                .map( ValueUtils::fromRelationshipProxy ).toArray( EdgeValue[]::new );

        return VirtualValues.path( nodes, edges );
    }

    public static ListValue asListOfEdges( Iterable<Relationship> rels )
    {
        return VirtualValues.list( StreamSupport.stream( rels.spliterator(), false )
                .map( ValueUtils::fromRelationshipProxy ).toArray( EdgeValue[]::new ) );
    }

    public static ListValue asListOfEdges( Relationship[] rels )
    {
        EdgeValue[] edgeValues = new EdgeValue[rels.length];
        for ( int i = 0; i < edgeValues.length; i++ )
        {
            edgeValues[i] = fromRelationshipProxy( rels[i] );
        }
        return VirtualValues.list( edgeValues );
    }

    public static MapValue asMapValue( Map<String,Object> map )
    {
        return map( mapValues( map ) );
    }

    public static PointValue fromMap( MapValue map )
    {
        if ( map.containsKey( "x" ) && map.containsKey( "y" ) )
        {
            double x = ((NumberValue) map.get( "x" )).doubleValue();
            double y = ((NumberValue) map.get( "y" )).doubleValue();
            if ( !map.containsKey( "crs" ) )
            {
                return VirtualValues.pointCartesian( x, y );
            }

            TextValue crs = (TextValue) map.get( "crs" );
            if ( crs.stringValue().equals( CoordinateReferenceSystem.Cartesian.type() ) )
            {
                return VirtualValues.pointCartesian( x, y );
            }
            else if ( crs.stringValue().equals( CoordinateReferenceSystem.WGS84.type() ) )
            {
                return VirtualValues.pointGeographic( x, y );
            }
            else
            {
                throw new IllegalArgumentException( "Unknown coordinate reference system: " + crs.stringValue() );
            }
        }
        else if ( map.containsKey( "latitude" ) && map.containsKey( "longitude" ) )
        {
            double latitude = ((NumberValue) map.get( "latitude" )).doubleValue();
            double longitude = ((NumberValue) map.get( "longitude" )).doubleValue();
            if ( !map.containsKey( "crs" ) )
            {
                return VirtualValues.pointGeographic( longitude, latitude );
            }

            TextValue crs = (TextValue) map.get( "crs" );
            if ( crs.stringValue().equals( CoordinateReferenceSystem.WGS84.type() ) )
            {
                return VirtualValues.pointGeographic( longitude, latitude );
            }
            else
            {
                throw new IllegalArgumentException(
                        "Geographic points does not support coordinate reference system: " + crs.stringValue() );
            }
        }
        else
        {
            throw new IllegalArgumentException(
                    "A point must contain either 'x' and 'y' or 'latitude' and 'longitude'" );
        }
    }

    private static Map<String,AnyValue> mapValues( Map<String,Object> map )
    {
        HashMap<String,AnyValue> newMap = new HashMap<>( map.size() );
        for ( Map.Entry<String,Object> entry : map.entrySet() )
        {
            newMap.put( entry.getKey(), of( entry.getValue() ) );
        }

        return newMap;
    }

    public static NodeValue fromNodeProxy( Node node )
    {
        return new NodeProxyWrappingNodeValue( node );
    }

    public static EdgeValue fromRelationshipProxy( Relationship relationship )
    {
        return new RelationshipProxyWrappingEdgeValue( relationship );
    }
}
