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
import java.util.Comparator;

import org.neo4j.values.AnyValue;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.VirtualValue;

public final class PathValue extends VirtualValue
{
    private final NodeValue[] nodes;
    private final EdgeValue[] edges;

    PathValue( NodeValue[] nodes, EdgeValue[] edges )
    {
        assert nodes != null;
        assert edges != null;
        assert nodes.length == edges.length + 1;

        this.nodes = nodes;
        this.edges = edges;
    }

    public NodeValue startNode()
    {
        return nodes[0];
    }

    public NodeValue endNode()
    {
        return nodes[nodes.length - 1];
    }

    public EdgeValue lastEdge()
    {
        assert edges.length > 0;
        return edges[edges.length - 1];
    }

    @Override
    public boolean equals( VirtualValue other )
    {
        if ( other == null || other.getClass() != PathValue.class )
        {
            return false;
        }
        PathValue that = (PathValue) other;
        return size() == that.size() &&
               Arrays.equals( nodes, that.nodes ) &&
               Arrays.equals( edges, that.edges );
    }

    @Override
    public int computeHash()
    {
        int result = nodes[0].hashCode();
        for ( int i = 1; i < nodes.length; i++ )
        {
            result += 31 * (result + edges[i - 1].hashCode());
            result += 31 * (result + nodes[i].hashCode());
        }
        return result;
    }

    @Override
    public <E extends Exception> void writeTo( AnyValueWriter<E> writer ) throws E
    {
        writer.writePath( nodes, edges );
    }

    @Override
    public VirtualValueGroup valueGroup()
    {
        return VirtualValueGroup.PATH;
    }

    @Override
    public int compareTo( VirtualValue other, Comparator<AnyValue> comparator )
    {
        if ( other == null || other.getClass() != PathValue.class )
        {
            throw new IllegalArgumentException( "Cannot compare different virtual values" );
        }

        PathValue otherPath = (PathValue) other;

        int x = nodes[0].compareTo( otherPath.nodes[0], comparator );
        if ( x == 0 )
        {
            int i = 0;
            int length = Math.min( edges.length, otherPath.edges.length );

            while ( x == 0 && i < length )
            {
                x = edges[i].compareTo( otherPath.edges[i], comparator );
                ++i;
            }

            if ( x == 0 )
            {
                x = Integer.compare( edges.length, otherPath.edges.length );
            }
        }

        return x;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder( "Path{" );
        int i = 0;
        for ( ; i < edges.length; i++ )
        {
            sb.append( nodes[i] );
            sb.append( edges[i] );
        }
        sb.append( nodes[i] );
        sb.append( '}' );
        return sb.toString();
    }

    public ListValue asList()
    {
        int size = nodes.length + edges.length;
        AnyValue[] anyValues = new AnyValue[size];
        for ( int i = 0; i < size; i++ )
        {
            if ( i % 2 == 0 )
            {
                anyValues[i] = nodes[i / 2];
            }
            else
            {
                anyValues[i] = edges[i / 2];
            }
        }
        return VirtualValues.list( anyValues );
    }

    public int size()
    {
        return edges.length;
    }

    public NodeValue[] nodes()
    {
        return nodes;
    }

    public EdgeValue[] edges()
    {
        return edges;
    }
}
