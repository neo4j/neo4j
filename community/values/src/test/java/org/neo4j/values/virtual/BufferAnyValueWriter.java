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

import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.BufferValueWriter;

import static java.lang.String.format;

public class BufferAnyValueWriter extends BufferValueWriter implements AnyValueWriter<RuntimeException>
{

    enum SpecialKind
    {
        WriteNodeReference,
        EndNode,
        BeginLabels,
        WriteLabel,
        EndLabels,
        WriteEdgeReference,
        EndEdge,
        BeginMap,
        WriteKeyId,
        EndMap,
        BeginList,
        EndList,
        BeginPath,
        EndPath,
        BeginPoint,
        EndPoint
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
    public void beginLabels( int numberOfLabels )
    {
        buffer.add( Specials.beginLabels( numberOfLabels ) );
    }

    @Override
    public void writeLabel( int labelId )
    {
        buffer.add( Specials.writeLabel( labelId ) );
    }

    @Override
    public void endLabels()
    {
        buffer.add( Specials.endLabels() );
    }

    @Override
    public void writeEdgeReference( long edgeId )
    {
        buffer.add( Specials.writeEdgeReference( edgeId ) );
    }

    @Override
    public void beginMap( int size )
    {
        buffer.add( Specials.beginMap( size ) );
    }

    @Override
    public void writeKeyId( int keyId )
    {
        buffer.add( Specials.writeKeyId( keyId ) );
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
    public void beginPath( int length )
    {
        buffer.add( Specials.beginPath( length ) );
    }

    @Override
    public void endPath()
    {
        buffer.add( Specials.endPath() );
    }

    @Override
    public void beginPoint( CoordinateReferenceSystem coordinateReferenceSystem )
    {
        buffer.add( Specials.beginPoint( coordinateReferenceSystem ) );
    }

    @Override
    public void endPoint()
    {
        buffer.add( Specials.endPoint() );
    }

    @SuppressWarnings( "WeakerAccess" )
    public static class Specials
    {

        public static Special writeNodeReference( long nodeId )
        {
            return new Special( SpecialKind.WriteNodeReference, (int)nodeId );
        }

        public static Special beginLabels( int numberOfLabels )
        {
            return new Special( SpecialKind.BeginLabels, numberOfLabels );
        }

        public static Special writeLabel( int labelId )
        {
            return new Special( SpecialKind.WriteLabel, labelId );
        }

        public static Special endLabels()
        {
            return new Special( SpecialKind.EndLabels, 0 );
        }

        public static Special writeEdgeReference( long edgeId )
        {
            return new Special( SpecialKind.WriteEdgeReference, (int)edgeId );
        }

        public static Special beginMap( int size )
        {
            return new Special( SpecialKind.BeginMap, size );
        }

        public static Special writeKeyId( int keyId )
        {
            return new Special( SpecialKind.WriteKeyId, keyId );
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

        public static Special beginPath( int length )
        {
            return new Special( SpecialKind.BeginPath, length );
        }

        public static Special endPath()
        {
            return new Special( SpecialKind.EndPath, 0 );
        }

        public static Special beginPoint( CoordinateReferenceSystem crs )
        {
            return new Special( SpecialKind.BeginPoint, crs.code );
        }

        public static Special endPoint()
        {
            return new Special( SpecialKind.EndPoint, 0 );
        }
    }
}
