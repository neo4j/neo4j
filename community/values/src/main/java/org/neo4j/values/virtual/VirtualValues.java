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

import java.util.HashMap;

import org.neo4j.values.AnyValue;
import org.neo4j.values.TextValue;
import org.neo4j.values.VirtualValue;

/**
 * Entry point to the virtual values library.
 */
@SuppressWarnings( "WeakerAccess" )
public final class VirtualValues
{
    private VirtualValues()
    {
    }

    // DIRECT FACTORY METHODS

    public static ListValue list( AnyValue... values )
    {
        return new ListValue( values );
    }

    public static MapValue emptyMap()
    {
        return new MapValue( new HashMap<>() );
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

    public static MapValue map( HashMap<String,AnyValue> map )
    {
        return new MapValue( map );
    }

    public static LabelSet labels( TextValue... labels )
    {
        return new LabelSet.ArrayBasedLabelSet( labels );
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
        return new PathValue( nodes, edges );
    }

    public static VirtualValue pointCartesian( double x, double y )
    {
        return new PointValue.CarthesianPointValue( x, y );
    }

    public static VirtualValue pointGeographic( double latitude, double longitude )
    {
        return new PointValue.GeographicPointValue( latitude, longitude );
    }

    public static NodeValue nodeValue( long id, LabelSet labels, MapValue properties )
    {
        return new NodeValue( id, labels, properties );
    }

    public static EdgeValue edgeValue( long id, long startNodeId, long endNodeId, TextValue type, MapValue properties )
    {
        return new EdgeValue( id, startNodeId, endNodeId, type, properties );
    }
}
