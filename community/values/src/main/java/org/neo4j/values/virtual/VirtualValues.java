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

    private static final int[] EMPTY_INT_ARRAY = new int[0];
    private static final TextValue[] EMPTY_TEXT_ARRAY = new TextValue[0];

    // DIRECT FACTORY METHODS

    public static ListValue list( AnyValue... values )
    {
        return new ListValue( values );
    }

    public static MapValue emptyMap()
    {
        return new MapValue( EMPTY_INT_ARRAY, EMPTY_TEXT_ARRAY );
    }

    public static MapValue map( int[] keys, AnyValue[] values )
    {
        return new MapValue( keys, values );
    }

    public static LabelValue label( int id, TextValue value )
    {
        return new LabelValue( id, value );
    }

    public static LabelSet labels( LabelValue... labelIds )
    {
        return new LabelSet.ArrayBasedLabelSet( labelIds );
    }

    public static NodeReference node( long id )
    {
        return new NodeReference( id );
    }

    public static EdgeReference edge( long id )
    {
        return new EdgeReference( id );
    }

    public static PathValue path( NodeReference[] nodes, EdgeReference[] edges )
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
