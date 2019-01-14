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
package org.neo4j.values.virtual;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.helpers.collection.Pair;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.PathValue.DirectPathValue;

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

    public static ListValue dropNoValues( ListValue list )
    {
        return new ListValue.DropNoValuesListValue( list );
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

    /*
    TOMBSTONE: TransformedListValue & FilteredListValue

    This list value variant would lazily apply a transform/filter on a inner list. The lazy behavior made it hard
    to guarantee that the transform/filter was still evaluable and correct on reading the transformed list, so
    this was removed. If we want lazy values again, remember the problems of

       - returning results out of Cypher combined with auto-closing iterators
       - reading modified tx-state which was not visible at TransformedListValue creation

    */

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

    public static ErrorValue error( Exception e )
    {
        return new ErrorValue( e );
    }

    @SafeVarargs
    public static MapValue copy( MapValue map, Pair<String,AnyValue>... moreEntries )
    {
        HashMap<String,AnyValue> hashMap = new HashMap<>( map.size() );
        for ( Map.Entry<String,AnyValue> entry : map.entrySet() )
        {
            hashMap.put( entry.getKey(), entry.getValue() );
        }
        for ( Pair<String,AnyValue> entry : moreEntries )
        {
            hashMap.put( entry.first(), entry.other() );
        }
        return new MapValue( hashMap );
    }

    public static NodeReference node( long id )
    {
        return new NodeReference( id );
    }

    public static RelationshipReference relationship( long id )
    {
        return new RelationshipReference( id );
    }

    public static PathValue path( NodeValue[] nodes, RelationshipValue[] relationships )
    {
        assert nodes != null;
        assert relationships != null;
        if ( (nodes.length + relationships.length) % 2 == 0 )
        {
            throw new IllegalArgumentException(
                    "Tried to construct a path that is not built like a path: even number of elements" );
        }
        return new DirectPathValue( nodes, relationships );
    }

    public static NodeValue nodeValue( long id, TextArray labels, MapValue properties )
    {
        return new NodeValue.DirectNodeValue( id, labels, properties );
    }

    public static RelationshipValue relationshipValue( long id, NodeValue startNode, NodeValue endNode, TextValue type,
            MapValue properties )
    {
        return new RelationshipValue.DirectRelationshipValue( id, startNode, endNode, type, properties );
    }
}
