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

import java.util.Arrays;

import org.neo4j.values.AnyValue;
import org.neo4j.values.Values;
import org.neo4j.values.VirtualValue;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@SuppressWarnings( "WeakerAccess" )
public class VirtualValueTestUtil
{
    public static AnyValue toAnyValue( Object o )
    {
        if ( o instanceof AnyValue )
        {
            return (AnyValue) o;
        }
        else
        {
            return Values.of( o );
        }
    }

    public static VirtualValue path( VirtualValue... pathElements )
    {
        assert pathElements.length % 2 == 1;
        NodeReference[] nodes = new NodeReference[pathElements.length / 2 + 1];
        EdgeReference[] edges = new EdgeReference[pathElements.length / 2];
        nodes[0] = (NodeReference) pathElements[0];
        for ( int i = 1; i < pathElements.length; i += 2 )
        {
            edges[i / 2] = (EdgeReference) pathElements[i];
            nodes[i / 2 + 1] = (NodeReference) pathElements[i + 1];
        }
        return VirtualValues.path( nodes, edges );
    }

    public static VirtualValue list( Object... objects )
    {
        AnyValue[] values = new AnyValue[objects.length];
        for ( int i = 0; i < objects.length; i++ )
        {
            values[i] = toAnyValue( objects[i] );
        }
        return VirtualValues.list( values );
    }

    public static VirtualValue map( Object... keyOrVal )
    {
        assert keyOrVal.length % 2 == 0;
        String[] keys = new String[keyOrVal.length / 2];
        AnyValue[] values = new AnyValue[keyOrVal.length / 2];
        for ( int i = 0; i < keyOrVal.length; i += 2 )
        {
            keys[i / 2] = (String) keyOrVal[i];
            values[i / 2] = toAnyValue( keyOrVal[i + 1] );
        }
        return VirtualValues.map( keys, values );
    }

    public static void assertEqual( VirtualValue a, VirtualValue b )
    {
        assertTrue( "should be equal", a.equals( b ) );
        assertTrue( "should be equal", b.equals( a ) );
        assertTrue( "should have same has", a.hashCode() == b.hashCode() );
    }

    public static void assertNotEqual( VirtualValue a, VirtualValue b )
    {
        assertFalse( "should not equal", a.equals( b ) );
        assertFalse( "should not equal", b.equals( a ) );
    }

    public static NodeReference[] nodes( long... ids )
    {
        return Arrays.stream( ids )
                .mapToObj( VirtualValues::node )
                .toArray( NodeReference[]::new );
    }

    public static EdgeReference[] edges( long... ids )
    {
        return Arrays.stream( ids )
                .mapToObj( VirtualValues::edge )
                .toArray( EdgeReference[]::new );
    }
}
