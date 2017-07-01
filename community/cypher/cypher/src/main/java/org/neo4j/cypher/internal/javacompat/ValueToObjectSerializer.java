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
package org.neo4j.cypher.internal.javacompat;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;

import org.neo4j.graphalgo.impl.util.PathImpl;
import org.neo4j.graphdb.spatial.CRS;
import org.neo4j.graphdb.spatial.Coordinate;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.CoordinateReferenceSystem;
import org.neo4j.values.virtual.EdgeValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;

public class ValueToObjectSerializer implements AnyValueWriter<RuntimeException>
{
    private final NodeManager nodeManager;
    private final Deque<Writer> stack = new ArrayDeque<>();

    public ValueToObjectSerializer( NodeManager nodeManager )
    {
        this.nodeManager = nodeManager;
        stack.push( new ObjectWriter() );
    }

    public Object value()
    {
        assert stack.size() == 1;
        return stack.getLast().value();
    }

    private void writeValue( Object value )
    {
        assert !stack.isEmpty();
        Writer head = stack.peek();
        head.write( value );
    }

    @Override
    public void writeNodeReference( long nodeId ) throws RuntimeException
    {
        writeValue( nodeManager.newNodeProxyById( nodeId ) );
    }

    @Override
    public void writeNode( long nodeId, TextArray ignore, MapValue properties ) throws RuntimeException
    {
        writeValue( nodeManager.newNodeProxyById( nodeId ) );
    }

    @Override
    public void writeEdgeReference( long edgeId ) throws RuntimeException
    {
        writeValue( nodeManager.newRelationshipProxyById( edgeId ) );
    }

    @Override
    public void writeEdge( long edgeId, long startNodeId, long endNodeId, TextValue type, MapValue properties )
            throws RuntimeException
    {
        writeValue( nodeManager.newRelationshipProxyById( edgeId ) );
    }

    @Override
    public void beginMap( int size ) throws RuntimeException
    {
        stack.push( new MapWriter( size ) );
    }

    @Override
    public void endMap() throws RuntimeException
    {
        assert !stack.isEmpty();
        writeValue( stack.pop().value() );
    }

    @Override
    public void beginList( int size ) throws RuntimeException
    {
        stack.push( new ListWriter( size ) );
    }

    @Override
    public void endList() throws RuntimeException
    {
        assert !stack.isEmpty();
        writeValue( stack.pop().value() );
    }

    @Override
    public void writePath( NodeValue[] nodes, EdgeValue[] edges ) throws RuntimeException
    {
        PathImpl.Builder builder = new PathImpl.Builder( nodeManager.newNodeProxyById( nodes[0].id() ) );
        for ( EdgeValue edge : edges )
        {
            builder.push( nodeManager.newRelationshipProxyById( edge.id() ) );
        }

        writeValue( builder.build() );
    }

    @Override
    public void beginPoint( CoordinateReferenceSystem coordinateReferenceSystem ) throws RuntimeException
    {
        stack.push( new PointWriter( coordinateReferenceSystem ) );
    }

    @Override
    public void endPoint() throws RuntimeException
    {
        assert !stack.isEmpty();
        writeValue( stack.pop().value() );
    }

    @Override
    public void writeNull() throws RuntimeException
    {
        writeValue( null );
    }

    @Override
    public void writeBoolean( boolean value ) throws RuntimeException
    {
        writeValue( value );
    }

    @Override
    public void writeInteger( byte value ) throws RuntimeException
    {
        writeValue( value );
    }

    @Override
    public void writeInteger( short value ) throws RuntimeException
    {
        writeValue( value );
    }

    @Override
    public void writeInteger( int value ) throws RuntimeException
    {
        writeValue( value );
    }

    @Override
    public void writeInteger( long value ) throws RuntimeException
    {
        writeValue( value );
    }

    @Override
    public void writeFloatingPoint( float value ) throws RuntimeException
    {
        writeValue( value );
    }

    @Override
    public void writeFloatingPoint( double value ) throws RuntimeException
    {
        writeValue( value );
    }

    @Override
    public void writeString( String value ) throws RuntimeException
    {
        writeValue( value );
    }

    @Override
    public void writeString( char value ) throws RuntimeException
    {
        writeValue( value );
    }

    @Override
    public void writeString( char[] value, int offset, int length ) throws RuntimeException
    {
        writeValue( new String( value, offset, length ) );
    }

    @Override
    public void beginUTF8( int size ) throws RuntimeException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void copyUTF8( long fromAddress, int length ) throws RuntimeException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void endUTF8() throws RuntimeException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void beginArray( int size, ArrayType arrayType ) throws RuntimeException
    {
        stack.push( new ArrayWriter( size ) );
    }

    @Override
    public void endArray() throws RuntimeException
    {
        assert !stack.isEmpty();
        writeValue( stack.pop().value() );
    }

    @Override
    public void writeByteArray( byte[] value ) throws RuntimeException
    {
        writeValue( value );
    }

    interface Writer
    {
        void write( Object value );

        Object value();
    }

    class ObjectWriter implements Writer
    {
        private Object value;

        @Override
        public void write( Object value )
        {
            this.value = value;
        }

        @Override
        public Object value()
        {
            return value;
        }
    }

    class MapWriter implements Writer
    {
        private String key;
        private boolean isKey = true;
        private final HashMap<String,Object> map;

        MapWriter( int size )
        {
            this.map = new HashMap<>( size );
        }

        @Override
        public void write( Object value )
        {
            if ( isKey )
            {
                key = (String) value;
                isKey = false;
            }
            else
            {
                map.put( key, value );
                isKey = true;
            }
        }

        @Override
        public Object value()
        {
            return map;
        }
    }

    class ArrayWriter implements Writer
    {
        protected final Object[] array;
        private int index;

        ArrayWriter( int size )
        {
            this.array = new Object[size];
        }

        @Override
        public void write( Object value )
        {
            array[index++] = value;
        }

        @Override
        public Object value()
        {
            return array;
        }
    }

    class ListWriter extends ArrayWriter
    {
        ListWriter( int size )
        {
            super( size );
        }

        @Override
        public Object value()
        {
            return Arrays.asList( array );
        }
    }

    class PointWriter implements Writer
    {
        //TODO it is quite silly that the point writer doesn't give me the whole thing at once
        private final double[] coordinates = new double[2];
        private int index;
        private final CoordinateReferenceSystem crs;

        PointWriter( CoordinateReferenceSystem crs )
        {
            this.crs = crs;
        }

        @Override
        public void write( Object value )
        {
            coordinates[index++] = (double) value;
        }

        @Override
        public Object value()
        {
            return new Point()
            {
                @Override
                public String getGeometryType()
                {
                    return "Point";
                }

                @Override
                public List<Coordinate> getCoordinates()
                {
                    return Collections.singletonList( new Coordinate( coordinates ) );
                }

                @Override
                public CRS getCRS()
                {
                    return new CRS()
                    {
                        @Override
                        public int getCode()
                        {
                            return crs.code();
                        }

                        @Override
                        public String getType()
                        {
                            return crs.type();
                        }

                        @Override
                        public String getHref()
                        {
                            return crs.href();
                        }
                    };
                }
            };
        }
    }
}
