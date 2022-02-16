/*
 * Copyright (c) "Neo4j"
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

import java.util.function.Consumer;

import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.ElementIdMapper;
import org.neo4j.values.storable.TextValue;

import static java.lang.String.format;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.values.AnyValueWriter.EntityMode.REFERENCE;

public abstract class RelationshipValue extends VirtualRelationshipValue implements RelationshipVisitor
{
    private final long id;
    private final ElementIdMapper elementIdMapper;
    private final long startNodeId;
    private final ElementIdMapper startNodeElementIdMapper;
    private final long endNodeId;
    private final ElementIdMapper endNodeElementIdMapper;
    private int type = RelationshipReference.NO_TYPE;

    protected RelationshipValue( long id, ElementIdMapper elementIdMapper, long startNodeId, ElementIdMapper startNodeElementIdMapper, long endNodeId,
            ElementIdMapper endNodeElementIdMapper )
    {
        this.id = id;
        this.elementIdMapper = elementIdMapper;
        this.startNodeId = startNodeId;
        this.startNodeElementIdMapper = startNodeElementIdMapper;
        this.endNodeId = endNodeId;
        this.endNodeElementIdMapper = endNodeElementIdMapper;
    }

    @Override
    public <E extends Exception> void writeTo( AnyValueWriter<E> writer ) throws E
    {
        if ( writer.entityMode() == REFERENCE )
        {
            writer.writeRelationshipReference( id );
        }
        else
        {
            writer.writeRelationship( id, startNode().id(), endNode().id(), type(), properties(), isDeleted() );
        }
    }

    @Override
    public String toString()
    {
        return format( "-[%d]-", id );
    }

    public long startNodeId()
    {
        return startNodeId;
    }

    @Override
    public long startNodeId( Consumer<RelationshipVisitor> consumer )
    {
        return startNodeId;
    }

    @Override
    public String startNodeElementId( Consumer<RelationshipVisitor> consumer )
    {
        return startNodeElementIdMapper.nodeElementId( startNodeId );
    }

    public String startNodeElementId()
    {
        return startNodeElementIdMapper.nodeElementId( startNodeId );
    }

    public long endNodeId()
    {
        return endNodeId;
    }

    @Override
    public long endNodeId( Consumer<RelationshipVisitor> consumer )
    {
        return endNodeId;
    }

    public String endNodeElementId()
    {
        return endNodeElementIdMapper.nodeElementId( endNodeId );
    }

    @Override
    public String endNodeElementId( Consumer<RelationshipVisitor> consumer )
    {
        return endNodeElementIdMapper.nodeElementId( endNodeId );
    }

    public abstract VirtualNodeValue startNode();

    public abstract VirtualNodeValue endNode();

    @Override
    public long id()
    {
        return id;
    }

    @Override
    public String elementId()
    {
        return elementIdMapper.relationshipElementId( id );
    }

    @Override
    public int relationshipTypeId( Consumer<RelationshipVisitor> consumer )
    {
        if ( type == RelationshipReference.NO_TYPE )
        {
            consumer.accept( this );
        }
        return type;
    }

    public abstract TextValue type();

    public abstract MapValue properties();

    public VirtualNodeValue otherNode( VirtualNodeValue node )
    {
        return node.equals( startNode() ) ? endNode() : startNode();
    }

    public long otherNodeId( long node )
    {
        return node == startNodeId() ? endNodeId() : startNodeId();
    }

    @Override
    public String getTypeName()
    {
        return "Relationship";
    }

    @Override
    public void visit( long startNode, long endNode, int type )
    {
        this.type = type;
    }

    private static final long DIRECT_RELATIONSHIP_VALUE_SHALLOW_SIZE = shallowSizeOfInstance( DirectRelationshipValue.class );
    static class DirectRelationshipValue extends RelationshipValue
    {
        private final VirtualNodeValue startNode;
        private final VirtualNodeValue endNode;
        private final TextValue type;
        private final MapValue properties;
        private final boolean isDeleted;
        private final String elementId;

        /**
         * @param id the id of the relationship.
         * @param elementId element id of the relationship, or {@code null} which means it will be created from the {@code elementIdMapper} only when requested.
         * @param elementIdMapper mapping internal id to element id.
         * @param startNode start node of this relationship.
         * @param endNode end node of this relationship.
         * @param type type name of this relationship.
         * @param properties properties of this relationship
         * @param isDeleted whether this node is deleted.
         */
        DirectRelationshipValue( long id, String elementId, ElementIdMapper elementIdMapper, VirtualNodeValue startNode, VirtualNodeValue endNode,
                TextValue type, MapValue properties, boolean isDeleted )
        {
            super( id, elementIdMapper, startNode.id(), startNode.elementIdMapper(), endNode.id(), endNode.elementIdMapper() );
            assert properties != null;

            this.elementId = elementId;
            this.startNode = startNode;
            this.endNode = endNode;
            this.type = type;
            this.properties = properties;
            this.isDeleted = isDeleted;
        }

        @Override
        public VirtualNodeValue startNode()
        {
            return startNode;
        }

        @Override
        public VirtualNodeValue endNode()
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

        @Override
        public long estimatedHeapUsage()
        {
            return DIRECT_RELATIONSHIP_VALUE_SHALLOW_SIZE +
                    startNode.estimatedHeapUsage() +
                    endNode.estimatedHeapUsage() +
                    type.estimatedHeapUsage() +
                    properties.estimatedHeapUsage();
        }

        @Override
        public boolean isDeleted()
        {
            return isDeleted;
        }

        @Override
        public String elementId()
        {
            return elementId != null ? elementId : super.elementId();
        }
    }
}
