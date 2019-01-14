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
package org.neo4j.values.virtual;

import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.storable.TextValue;

import static java.lang.String.format;

public abstract class RelationshipValue extends VirtualRelationshipValue
{
    private final long id;

    protected RelationshipValue( long id )
    {
        this.id = id;
    }

    @Override
    public <E extends Exception> void writeTo( AnyValueWriter<E> writer ) throws E
    {
        writer.writeRelationship( id, startNode().id(), endNode().id(), type(), properties() );
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

    public NodeValue otherNode( VirtualNodeValue node )
    {
        return node.equals( startNode() ) ? endNode() : startNode();
    }

    public long otherNodeId( long node )
    {
        return node == startNode().id() ? endNode().id() : startNode().id();
    }

    @Override
    public String getTypeName()
    {
        return "Relationship";
    }

    static class DirectRelationshipValue extends RelationshipValue
    {
        private final NodeValue startNode;
        private final NodeValue endNode;
        private final TextValue type;
        private final MapValue properties;

        DirectRelationshipValue( long id, NodeValue startNode, NodeValue endNode, TextValue type, MapValue properties )
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
}
