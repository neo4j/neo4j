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

final class PathValue extends VirtualValue
{
    private final NodeReference[] nodes;
    private final EdgeReference[] edges;

    PathValue( NodeReference[] nodes, EdgeReference[] edges )
    {
        assert nodes != null;
        assert edges != null;
        assert nodes.length == edges.length + 1;

        this.nodes = nodes;
        this.edges = edges;
    }

    @Override
    public boolean equals( VirtualValue other )
    {
        if ( other == null || !(other instanceof PathValue) )
        {
            return false;
        }
        PathValue that = (PathValue) other;
        return size() == that.size() &&
                Arrays.equals( nodes, that.nodes ) &&
                Arrays.equals( edges, that.edges );
    }

    @Override
    public int hash()
    {
        int result = nodes[0].hashCode();
        for ( int i = 1; i < nodes.length; i++ )
        {
            result += 31 * ( result + edges[i - 1].hashCode() );
            result += 31 * ( result + nodes[i].hashCode() );
        }
        return result;
    }

    @Override
    public void writeTo( AnyValueWriter writer )
    {
        writer.beginPath( edges.length );
        for ( NodeReference node : nodes )
        {
            node.writeTo( writer );
        }
        for ( EdgeReference edge : edges )
        {
            edge.writeTo( writer );
        }
        writer.endPath();
    }

    @Override
    public VirtualValueGroup valueGroup()
    {
        return VirtualValueGroup.PATH;
    }

    @Override
    public int compareTo( VirtualValue other, Comparator<AnyValue> comparator )
    {
        if ( !(other instanceof PathValue) )
        {
            throw new IllegalArgumentException( "Cannot compare different virtual values" );
        }

        PathValue otherPath = (PathValue) other;

        int x = Integer.compare( edges.length, otherPath.edges.length );
        if ( x == 0 )
        {
            for ( int i = 0; i < edges.length; i++ )
            {
                x = edges[i].compareTo( otherPath.edges[i], comparator );
                if ( x != 0 )
                {
                    return x;
                }
            }
            x = nodes[0].compareTo( otherPath.nodes[0], comparator );
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

    public int size()
    {
        return edges.length;
    }
}
