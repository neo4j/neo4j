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

import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.AnyValues;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;

public abstract class EdgeValue extends VirtualEdgeValue
{
    private final long id;

    protected EdgeValue( long id )
    {
        this.id = id;
    }

    @Override
    public <E extends Exception> void writeTo( AnyValueWriter<E> writer ) throws E
    {
        writer.writeEdge( id, startNode().id(), endNode().id(), type(), properties() );
    }

    @Override
    public String toString()
    {
        return format( "-[%d]-", id );
    }

    public abstract NodeValue startNode();

    public abstract NodeValue endNode();

    @Override
    public long id()
    {
        return id;
    }

    public abstract TextValue type();

    public abstract MapValue properties();

    public NodeValue otherNode( NodeValue node )
    {
        return node.equals( startNode() ) ? endNode() : startNode();
    }

    static class DirectEdgeValue extends EdgeValue
    {
        private final NodeValue startNode;
        private final NodeValue endNode;
        private final TextValue type;
        private final MapValue properties;

        DirectEdgeValue( long id, NodeValue startNode, NodeValue endNode, TextValue type, MapValue properties )
        {
            super( id );
            assert properties != null;

            this.startNode = startNode;
            this.endNode = endNode;
            this.type = type;
            this.properties = properties;
        }

        @Override
        public NodeValue startNode()
        {
            return startNode;
        }

        @Override
        public NodeValue endNode()
        {
            return endNode;
        }

        @Override
        public TextValue type()
        {
            return type;
        }

        @Override
        public MapValue properties()
        {
            return properties;
        }
    }

    public static class RelationshipProxyWrappingEdgeValue extends EdgeValue
    {
        private final Relationship relationship;
        private volatile TextValue type;
        private volatile MapValue properties;
        private volatile NodeValue startNode;
        private volatile NodeValue endNode;

        RelationshipProxyWrappingEdgeValue( Relationship relationship )
        {
            super( relationship.getId() );
            this.relationship = relationship;
        }

        public Relationship relationshipProxy()
        {
            return relationship;
        }

        @Override
        public <E extends Exception> void writeTo( AnyValueWriter<E> writer ) throws E
        {
            MapValue p;
            try
            {
                p = properties();
            }
            catch ( NotFoundException e )
            {
                p = VirtualValues.EMPTY_MAP;

            }
            writer.writeEdge( id(), startNode().id(), endNode().id(), type(), p);
        }

        @Override
        public NodeValue startNode()
        {
            NodeValue start = startNode;
            if ( start == null )
            {
                synchronized ( this )
                {
                    start = startNode;
                    if ( start == null )
                    {
                        start = startNode = VirtualValues.fromNodeProxy( relationship.getStartNode() );
                    }
                }
            }
            return start;
        }

        @Override
        public NodeValue endNode()
        {
            NodeValue end = endNode;
            if ( end == null )
            {
                synchronized ( this )
                {
                    end = endNode;
                    if ( end == null )
                    {
                        end = endNode = VirtualValues.fromNodeProxy( relationship.getEndNode() );
                    }
                }
            }
            return end;
        }

        @Override
        public TextValue type()
        {
            TextValue t = type;
            if ( t == null )
            {
                synchronized ( this )
                {
                    t = type;
                    if ( t == null )
                    {
                        t = type = Values.stringValue( relationship.getType().name() );
                    }
                }
            }
            return t;
        }

        @Override
        public MapValue properties()
        {
            MapValue m = properties;
            if ( m == null )
            {
                synchronized ( this )
                {
                    m = properties;
                    if ( m == null )
                    {
                        m = properties = AnyValues.asMapValue( relationship.getAllProperties() );
                    }
                }
            }
            return m;
        }
    }
}
