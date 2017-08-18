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
package org.neo4j.values.virtual;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;

/**
 * Entry point to the virtual values library.
 */
@SuppressWarnings( "WeakerAccess" )
public final class VirtualValues
{
    public static final MapValue EMPTY_MAP = new MapValue( Collections.emptyMap() );
    public static final ListValue EMPTY_LIST = new ListValue.ArrayListValue( new AnyValue[0] );

    private VirtualValues()
    {
    }

    // DIRECT FACTORY METHODS

    public static ListValue list( AnyValue... values )
    {
        return new ListValue.ArrayListValue( values );
    }

    public static ListValue fromList( List<AnyValue> values )
    {
        return new ListValue.JavaListListValue( values );
    }

    public static ListValue range( long start, long end, long step )
    {
        return new ListValue.IntegralRangeListValue( start, end, step );
    }

    public static ListValue fromArray( ArrayValue arrayValue )
    {
        return new ListValue.ArrayValueListValue( arrayValue );
    }

    public static ListValue filter( ListValue list, Function<AnyValue,Boolean> filter )
    {
        return new ListValue.FilteredListValue( list, filter );
    }

    public static ListValue slice( ListValue list, int from, int to )
    {
        int f = Math.max( from, 0 );
        int t = Math.min( to, list.size() );
        if ( f > t )
        {
            return EMPTY_LIST;
        }
        else
        {
            return new ListValue.ListSlice( list, f, t );
        }
    }

    public static ListValue drop( ListValue list, int n )
    {
        int start = Math.max( 0, Math.min( n, list.size() ) );
        return new ListValue.ListSlice( list, start, list.size() );
    }

    public static ListValue take( ListValue list, int n )
    {
        int end = Math.max( 0, Math.min( n, list.size() ) );
        return new ListValue.ListSlice( list, 0, end );
    }

    public static ListValue transform( ListValue list, Function<AnyValue,AnyValue> transForm )
    {
        return new ListValue.TransformedListValue( list, transForm );
    }

    public static ListValue reverse( ListValue list )
    {
        return new ListValue.ReversedList( list );
    }

    public static ListValue concat( ListValue... lists )
    {
        return new ListValue.ConcatList( lists );
    }

    public static ListValue appendToList( ListValue list, AnyValue value )
    {
        AnyValue[] newValues = new AnyValue[list.size() + 1];
        System.arraycopy( list.asArray(), 0, newValues, 0, list.size() );
        newValues[list.size()] = value;
        return VirtualValues.list( newValues );
    }

    public static ListValue prependToList( ListValue list, AnyValue value )
    {
        AnyValue[] newValues = new AnyValue[list.size() + 1];
        newValues[0] = value;
        System.arraycopy( list.asArray(), 0, newValues, 1, list.size() );
        return VirtualValues.list( newValues );
    }

    public static MapValue emptyMap()
    {
        return EMPTY_MAP;
    }

    public static MapValue map( String[] keys, AnyValue[] values )
    {
        assert keys.length == values.length;
        HashMap<String,AnyValue> map = new HashMap<>( keys.length );
        for ( int i = 0; i < keys.length; i++ )
        {
            map.put( keys[i], values[i] );
        }
        return new MapValue( map );
    }

    public static MapValue combine( MapValue a, MapValue b )
    {
        HashMap<String,AnyValue> map = new HashMap<>( a.size() + b.size() );
        a.foreach( map::put );
        b.foreach( map::put );
        return VirtualValues.map( map );
    }

    public static MapValue map( Map<String,AnyValue> map )
    {
        return new MapValue( map );
    }

    public static NodeReference node( long id )
    {
        return new NodeReference( id );
    }

    public static EdgeReference edge( long id )
    {
        return new EdgeReference( id );
    }

    public static PathValue path( NodeValue[] nodes, EdgeValue[] edges )
    {
        assert nodes != null;
        assert edges != null;
        if ( (nodes.length + edges.length) % 2 == 0 )
        {
            throw new IllegalArgumentException(
                    "Tried to construct a path that is not built like a path: even number of elements" );
        }
        return new PathValue( nodes, edges );
    }

    public static PointValue pointCartesian( double x, double y )
    {
        return new PointValue.CartesianPointValue( x, y );
    }

    public static PointValue pointGeographic( double longitude, double latitude )
    {
        return new PointValue.GeographicPointValue( longitude, latitude );
    }

    public static NodeValue nodeValue( long id, TextArray labels, MapValue properties )
    {
        return new NodeValue.DirectNodeValue( id, labels, properties );
    }

    public static NodeValue fromNodeProxy( Node node )
    {
        return new NodeValue.NodeProxyWrappingNodeValue( node );
    }

    public static EdgeValue edgeValue( long id, NodeValue startNode, NodeValue endNode, TextValue type,
            MapValue properties )
    {
        return new EdgeValue.DirectEdgeValue( id, startNode, endNode, type, properties );
    }

    public static EdgeValue fromRelationshipProxy( Relationship relationship )
    {
        return new EdgeValue.RelationshipProxyWrappingEdgeValue( relationship );
    }
}
