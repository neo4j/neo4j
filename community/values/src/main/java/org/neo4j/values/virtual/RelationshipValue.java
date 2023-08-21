/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.values.virtual;

import static java.lang.String.format;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.values.AnyValueWriter.EntityMode.REFERENCE;

import java.util.function.Consumer;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.storable.TextValue;

public abstract class RelationshipValue extends VirtualRelationshipValue implements RelationshipVisitor {
    private final long id;
    private long startNodeId;
    private long endNodeId;
    private int type = RelationshipReference.NO_TYPE;

    protected RelationshipValue(long id, long startNodeId, long endNodeId) {
        this.id = id;
        this.startNodeId = startNodeId;
        this.endNodeId = endNodeId;
    }

    @Override
    public <E extends Exception> void writeTo(AnyValueWriter<E> writer) throws E {
        if (writer.entityMode() == REFERENCE) {
            writer.writeRelationshipReference(id);
        } else {
            writer.writeRelationship(
                    elementId(),
                    id,
                    startNodeElementId(),
                    startNode().id(),
                    endNodeElementId(),
                    endNode().id(),
                    type(),
                    properties(),
                    isDeleted());
        }
    }

    @Override
    public String toString() {
        return format("-[%d]-", id);
    }

    public long startNodeId() {
        return startNodeId;
    }

    public String startNodeElementId() {
        return startNode().elementId();
    }

    @Override
    public long startNodeId(Consumer<RelationshipVisitor> consumer) {
        if (startNodeId == RelationshipReference.NO_NODE) {
            consumer.accept(this);
        }
        return startNodeId;
    }

    public long endNodeId() {
        return endNodeId;
    }

    public String endNodeElementId() {
        return endNode().elementId();
    }

    @Override
    public long endNodeId(Consumer<RelationshipVisitor> consumer) {
        if (endNodeId == RelationshipReference.NO_NODE) {
            consumer.accept(this);
        }
        return endNodeId;
    }

    public abstract VirtualNodeReference startNode();

    public abstract VirtualNodeReference endNode();

    @Override
    public long id() {
        return id;
    }

    @Override
    public int relationshipTypeId(Consumer<RelationshipVisitor> consumer) {
        if (type == RelationshipReference.NO_TYPE) {
            consumer.accept(this);
        }
        return type;
    }

    public abstract TextValue type();

    public abstract MapValue properties();

    public abstract String elementId();

    public VirtualNodeValue otherNode(VirtualNodeValue node) {
        return node.equals(startNode()) ? endNode() : startNode();
    }

    public long otherNodeId(long node) {
        return node == startNodeId() ? endNodeId() : startNodeId();
    }

    @Override
    public String getTypeName() {
        return "Relationship";
    }

    @Override
    public void visit(long startNode, long endNode, int type) {
        this.type = type;
        this.startNodeId = startNode;
        this.endNodeId = endNode;
    }

    private static final long DIRECT_RELATIONSHIP_VALUE_SHALLOW_SIZE =
            shallowSizeOfInstance(DirectRelationshipValue.class);

    public static class DirectRelationshipValue extends RelationshipValue {
        private final String elementId;
        private final VirtualNodeReference startNode;
        private final VirtualNodeReference endNode;
        private final TextValue type;
        private final MapValue properties;
        private final boolean isDeleted;

        /**
         * @param id the id of the relationship.
         * @param startNode start node of this relationship.
         * @param endNode end node of this relationship.
         * @param type type name of this relationship.
         * @param properties properties of this relationship
         * @param isDeleted whether this node is deleted.
         */
        DirectRelationshipValue(
                long id,
                String elementId,
                VirtualNodeReference startNode,
                VirtualNodeReference endNode,
                TextValue type,
                MapValue properties,
                boolean isDeleted) {
            super(id, startNode.id(), endNode.id());
            assert properties != null;
            assert elementId != null;

            this.elementId = elementId;
            this.startNode = startNode;
            this.endNode = endNode;
            this.type = type;
            this.properties = properties;
            this.isDeleted = isDeleted;
        }

        @Override
        public VirtualNodeReference startNode() {
            return startNode;
        }

        @Override
        public VirtualNodeReference endNode() {
            return endNode;
        }

        @Override
        public TextValue type() {
            return type;
        }

        @Override
        public MapValue properties() {
            return properties;
        }

        @Override
        public long estimatedHeapUsage() {
            return DIRECT_RELATIONSHIP_VALUE_SHALLOW_SIZE
                    + startNode.estimatedHeapUsage()
                    + endNode.estimatedHeapUsage()
                    + type.estimatedHeapUsage()
                    + properties.estimatedHeapUsage();
        }

        @Override
        public boolean isDeleted() {
            return isDeleted;
        }

        @Override
        public String elementId() {
            return elementId;
        }
    }
}
