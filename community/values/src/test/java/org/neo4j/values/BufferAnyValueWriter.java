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
package org.neo4j.values;

import java.util.Arrays;

import org.neo4j.values.storable.BufferValueWriter;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.EdgeValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;

import static java.lang.String.format;

public class BufferAnyValueWriter extends BufferValueWriter implements AnyValueWriter<RuntimeException>
{

    enum SpecialKind
    {
        WriteNode,
        WriteNodeReference,
        EndNode,
        BeginLabels,
        EndLabels,
        WriteEdge,
        WriteEdgeReference,
        EndEdge,
        WritePath,
        BeginMap,
        WriteKeyId,
        EndMap,
        BeginList,
        EndList,
    }

    public static class Special
    {
        final SpecialKind kind;
        final String key;

        @Override
        public boolean equals( Object o )
        {
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }

            Special special = (Special) o;
            return kind == special.kind && key.equals( special.key );
        }

        @Override
        public int hashCode()
        {
            return 31 * kind.hashCode() + key.hashCode();
        }

        Special( SpecialKind kind, String key )
        {
            this.kind = kind;
            this.key = key;
        }

        Special( SpecialKind kind, int key )
        {
            this.kind = kind;
            this.key = Integer.toString( key );
        }

        @Override
        public String toString()
        {
            return format( "Special(%s)", key );
        }
    }

    @Override
    public void writeNodeReference( long nodeId )
    {
        buffer.add( Specials.writeNodeReference( nodeId ) );
    }

    @Override
    public void writeNode( long nodeId, TextArray labels, MapValue properties ) throws RuntimeException
    {
        buffer.add( Specials.writeNode( nodeId, labels, properties ) );
    }

    @Override
    public void writeEdgeReference( long edgeId )
    {
        buffer.add( Specials.writeEdgeReference( edgeId ) );
    }

    @Override
    public void writeEdge( long edgeId, long startNodeId, long endNodeId, TextValue type, MapValue properties )
            throws RuntimeException
    {
        buffer.add( Specials.writeEdge( edgeId, startNodeId, endNodeId, type, properties ) );
    }

    @Override
    public void beginMap( int size )
    {
        buffer.add( Specials.beginMap( size ) );
    }

    @Override
    public void endMap()
    {
        buffer.add( Specials.endMap() );
    }

    @Override
    public void beginList( int size )
    {
        buffer.add( Specials.beginList( size ) );
    }

    @Override
    public void endList()
    {
        buffer.add( Specials.endList() );
    }

    @Override
    public void writePath( NodeValue[] nodes, EdgeValue[] edges ) throws RuntimeException
    {
        buffer.add( Specials.writePath( nodes, edges ) );
    }

    @SuppressWarnings( "WeakerAccess" )
    public static class Specials
    {

        public static Special writeNode( long nodeId, TextArray labels, MapValue properties )
        {
            return new Special( SpecialKind.WriteNode, Arrays.hashCode( new Object[]{nodeId, properties} ) +
                                                       31 * labels.hashCode() );
        }

        public static Special writeEdge( long edgeId, long startNodeId, long endNodeId, TextValue type,
                MapValue properties )
        {
            return new Special( SpecialKind.WriteEdge,
                    Arrays.hashCode( new Object[]{edgeId, startNodeId, endNodeId, type, properties} ) );
        }

        public static Special writePath( NodeValue[] nodes, EdgeValue[] edges )
        {
            return new Special( SpecialKind.WritePath, Arrays.hashCode( nodes ) + 31 * Arrays.hashCode( edges ) );
        }

        public static Special writeNodeReference( long nodeId )
        {
            return new Special( SpecialKind.WriteNodeReference, (int) nodeId );
        }

        public static Special writeEdgeReference( long edgeId )
        {
            return new Special( SpecialKind.WriteEdgeReference, (int) edgeId );
        }

        public static Special beginMap( int size )
        {
            return new Special( SpecialKind.BeginMap, size );
        }

        public static Special endMap()
        {
            return new Special( SpecialKind.EndMap, 0 );
        }

        public static Special beginList( int size )
        {
            return new Special( SpecialKind.BeginList, size );
        }

        public static Special endList()
        {
            return new Special( SpecialKind.EndList, 0 );
        }
    }
}
