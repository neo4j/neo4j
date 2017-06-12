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

/**
 * Entry point to the virtual values library.
 */
@SuppressWarnings( "WeakerAccess" )
public class VirtualValues
{
    private VirtualValues()
    {
    }

    // DIRECT FACTORY METHODS

    public static ListValue list( AnyValue... values )
    {
        return new ListValue( values );
    }

    public static MapValue map( int[] keys, AnyValue[] values )
    {
        return new MapValue( keys, values );
    }

    public static LabelSet labels( int... labelIds )
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
}
