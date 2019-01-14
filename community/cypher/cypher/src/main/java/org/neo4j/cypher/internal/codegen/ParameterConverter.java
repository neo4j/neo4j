/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.codegen;

import java.lang.reflect.Array;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.collection.ReverseArrayIterator;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualValues;

import static org.neo4j.helpers.collection.Iterators.iteratorsEqual;

/**
 * Used for turning parameters into appropriate types in the compiled runtime
 */
class ParameterConverter implements AnyValueWriter<RuntimeException>
{
    private final Deque<Writer> stack = new ArrayDeque<>();
    private final EmbeddedProxySPI proxySpi;

    ParameterConverter( EmbeddedProxySPI proxySpi )
    {
        this.proxySpi = proxySpi;
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
    public void writeNodeReference( long nodeId )
    {
        writeValue( VirtualValues.node( nodeId ) );
    }

    @Override
    public void writeNode( long nodeId, TextArray ignore, MapValue properties )
    {
        writeValue( VirtualValues.node( nodeId ) );
    }

    @Override
    public void writeRelationshipReference( long relId )
    {
        writeValue( VirtualValues.relationship( relId ) );
    }

    @Override
    public void writeRelationship( long relId, long startNodeId, long endNodeId, TextValue type, MapValue properties )
    {
        writeValue( VirtualValues.relationship( relId ) );
    }

    @Override
    public void beginMap( int size )
    {
        stack.push( new MapWriter( size ) );
    }

    @Override
    public void endMap()
    {
        assert !stack.isEmpty();
        writeValue( stack.pop().value() );
    }

    @Override
    public void beginList( int size )
    {
        stack.push( new ListWriter( size ) );
    }

    @Override
    public void endList()
    {
        assert !stack.isEmpty();
        writeValue( stack.pop().value() );
    }

    @Override
    public void writePath( NodeValue[] nodes, RelationshipValue[] relationships )
    {
        assert nodes != null;
        assert nodes.length > 0;
        assert relationships != null;
        assert nodes.length == relationships.length + 1;

        Node[] nodeProxies = new Node[nodes.length];
        for ( int i = 0; i < nodes.length; i++ )
        {
            nodeProxies[i] = proxySpi.newNodeProxy( nodes[i].id() );
        }
        Relationship[] relationship = new Relationship[relationships.length];
        for ( int i = 0; i < relationships.length; i++ )
        {
            relationship[i] = proxySpi.newRelationshipProxy( relationships[i].id() );
        }
        writeValue( new Path()
        {
            @Override
            public Node startNode()
            {
                return nodeProxies[0];
            }

            @Override
            public Node endNode()
            {
                return nodeProxies[nodeProxies.length - 1];
            }

            @Override
            public Relationship lastRelationship()
            {
                return relationship[relationship.length - 1];
            }

            @Override
            public Iterable<Relationship> relationships()
            {
                return Arrays.asList( relationship );
            }

            @Override
            public Iterable<Relationship> reverseRelationships()
            {
                return () -> new ReverseArrayIterator<>( relationship );
            }

            @Override
            public Iterable<Node> nodes()
            {
                return Arrays.asList( nodeProxies );
            }

            @Override
            public Iterable<Node> reverseNodes()
            {
                return () -> new ReverseArrayIterator<>( nodeProxies );
            }

            @Override
            public int length()
            {
                return relationship.length;
            }

            @Override
            public int hashCode()
            {
                if ( relationship.length == 0 )
                {
                    return startNode().hashCode();
                }
                else
                {
                    return Arrays.hashCode( relationship );
                }
            }

            @Override
            public boolean equals( Object obj )
            {
                if ( this == obj )
                {
                    return true;
                }
                else if ( obj instanceof Path )
                {
                    Path other = (Path) obj;
                    return startNode().equals( other.startNode() ) &&
                           iteratorsEqual( this.relationships().iterator(), other.relationships().iterator() );

                }
                else
                {
                    return false;
                }
            }

            @Override
            public Iterator<PropertyContainer> iterator()
            {
                return new Iterator<PropertyContainer>()
                {
                    Iterator<? extends PropertyContainer> current = nodes().iterator();
                    Iterator<? extends PropertyContainer> next = relationships().iterator();

                    @Override
                    public boolean hasNext()
                    {
                        return current.hasNext();
                    }

                    @Override
                    public PropertyContainer next()
                    {
                        try
                        {
                            return current.next();
                        }
                        finally
                        {
                            Iterator<? extends PropertyContainer> temp = current;
                            current = next;
                            next = temp;
                        }
                    }

                    @Override
                    public void remove()
                    {
                        next.remove();
                    }
                };
            }
        } );
    }

    @Override
    public void writeNull()
    {
        writeValue( null );
    }

    @Override
    public void writeBoolean( boolean value )
    {
        writeValue( value );
    }

    @Override
    public void writeInteger( byte value )
    {
        writeValue( (long) value );
    }

    @Override
    public void writeInteger( short value )
    {
        writeValue( (long) value );
    }

    @Override
    public void writeInteger( int value )
    {
        writeValue( (long) value );
    }

    @Override
    public void writeInteger( long value )
    {
        writeValue( value );
    }

    @Override
    public void writeFloatingPoint( float value )
    {
        writeValue( (double) value );
    }

    @Override
    public void writeFloatingPoint( double value )
    {
        writeValue( value );
    }

    @Override
    public void writeString( String value )
    {
        writeValue( value );
    }

    @Override
    public void writeString( char value )
    {
        writeValue( value );
    }

    @Override
    public void beginArray( int size, ArrayType arrayType )
    {
        stack.push( new ArrayWriter( size, arrayType ) );
    }

    @Override
    public void endArray()
    {
        assert !stack.isEmpty();
        writeValue( stack.pop().value() );
    }

    @Override
    public void writeByteArray( byte[] value )
    {
        writeValue( value );
    }

    @Override
    public void writePoint( CoordinateReferenceSystem crs, double[] coordinate )
    {
        writeValue( Values.pointValue( crs, coordinate ) );
    }

    @Override
    public void writeDuration( long months, long days, long seconds, int nanos )
    {
        writeValue( DurationValue.duration( months, days, seconds, nanos ) );
    }

    @Override
    public void writeDate( LocalDate localDate )
    {
        writeValue( localDate );
    }

    @Override
    public void writeLocalTime( LocalTime localTime )
    {
        writeValue( localTime );
    }

    @Override
    public void writeTime( OffsetTime offsetTime )
    {
        writeValue( offsetTime );
    }

    @Override
    public void writeLocalDateTime( LocalDateTime localDateTime )
    {
        writeValue( localDateTime );
    }

    @Override
    public void writeDateTime( ZonedDateTime zonedDateTime )
    {
        writeValue( zonedDateTime );
    }

    private interface Writer
    {
        void write( Object value );

        Object value();
    }

    private static class ObjectWriter implements Writer
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

    private static class MapWriter implements Writer
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

    private static class ArrayWriter implements Writer
    {
        protected final Object array;
        private int index;

        ArrayWriter( int size, ArrayType arrayType )
        {
            switch ( arrayType )
            {
            case SHORT:
                this.array = Array.newInstance( short.class, size );
                break;
            case INT:
                this.array = Array.newInstance( int.class, size );
                break;
            case BYTE:
                this.array = Array.newInstance( byte.class, size );
                break;
            case LONG:
                this.array = Array.newInstance( long.class, size );
                break;
            case FLOAT:
                this.array = Array.newInstance( float.class, size );
                break;
            case DOUBLE:
                this.array = Array.newInstance( double.class, size );
                break;
            case BOOLEAN:
                this.array = Array.newInstance( boolean.class, size );
                break;
            case STRING:
                this.array = Array.newInstance( String.class, size );
                break;
            case CHAR:
                this.array = Array.newInstance( char.class, size );
                break;
            default:
                this.array = new Object[size];
            }
        }

        @Override
        public void write( Object value )
        {
            Array.set( array, index, value );
            index++;
        }

        @Override
        public Object value()
        {
            return array;
        }
    }

    private static class ListWriter implements Writer
    {
        private final List<Object> list;

        ListWriter( int size )
        {
            this.list = new ArrayList<>( size );
        }

        @Override
        public void write( Object value )
        {
            list.add( value );
        }

        @Override
        public Object value()
        {
            return list;
        }
    }
}
