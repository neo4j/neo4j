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
package org.neo4j.values.virtual;

import java.util.HashMap;
import java.util.List;

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
    public static final MapValue EMPTY_MAP = MapValue.EMPTY;
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

    /*
    TOMBSTONE: TransformedListValue & FilteredListValue

    This list value variant would lazily apply a transform/filter on a inner list. The lazy behavior made it hard
    to guarantee that the transform/filter was still evaluable and correct on reading the transformed list, so
    this was removed. If we want lazy values again, remember the problems of

       - returning results out of Cypher combined with auto-closing iterators
       - reading modified tx-state which was not visible at TransformedListValue creation

    */

    public static ListValue concat( ListValue... lists )
    {
        return new ListValue.ConcatList( lists );
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
        return new MapValue.MapWrappingMapValue( map );
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
