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

public class BufferAnyValueWriter extends BufferValueWriter implements AnyValueWriter
{

    enum SpecialKind
    {
        BeginNode,
        EndNode,
        BeginLabels,
        WriteLabel,
        EndLabels,
        BeginProperties,
        WritePropertyKeyId,
        EndProperties,
        BeginEdge,
        EndEdge,
        BeginMap,
        WriteKeyId,
        EndMap,
        BeginList,
        EndList,
        BeginPath,
        EndPath
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
    public void beginNode( long nodeId )
    {
        buffer.add( Specials.beginNode( nodeId ) );
    }

    @Override
    public void endNode()
    {
        buffer.add( Specials.endNode() );
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
    public void beginProperties( int numberOfProperties )
    {
        buffer.add( Specials.beginProperties( numberOfProperties ) );
    }

    @Override
    public void writePropertyKeyId( int propertyKeyId )
    {
        buffer.add( Specials.writePropertyKeyId( propertyKeyId ) );
    }

    @Override
    public void endProperties()
    {
        buffer.add( Specials.endProperties() );
    }

    @Override
    public void beginEdge( long edgeId )
    {
        buffer.add( Specials.beginEdge( edgeId ) );
    }

    @Override
    public void endEdge()
    {
        buffer.add( Specials.endEdge() );
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

    @SuppressWarnings( "WeakerAccess" )
    public static class Specials
    {

        public static Special beginNode( long nodeId )
        {
            return new Special( SpecialKind.BeginNode, (int)nodeId );
        }

        public static Special endNode()
        {
            return new Special( SpecialKind.EndNode, 0 );
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

        public static Special beginProperties( int numberOfProperties )
        {
            return new Special( SpecialKind.BeginProperties, numberOfProperties );
        }

        public static Special writePropertyKeyId( int propertyKeyId )
        {
            return new Special( SpecialKind.WritePropertyKeyId, propertyKeyId );
        }

        public static Special endProperties()
        {
            return new Special( SpecialKind.EndProperties, 0 );
        }

        public static Special beginEdge( long edgeId )
        {
            return new Special( SpecialKind.BeginEdge, (int)edgeId );
        }

        public static Special endEdge()
        {
            return new Special( SpecialKind.EndEdge, 0 );
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
    }
}
