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

import org.neo4j.exceptions.IncomparableValuesException;
import org.neo4j.values.VirtualValue;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;

public interface CompositeDatabaseValue {
    long sourceId();

    class CompositeGraphDirectNodeValue extends NodeValue.DirectNodeValue implements CompositeDatabaseValue {
        private final long sourceId;

        /**
         * @param id internal id of the node.
         * @param sourceId a unique id in the current transaction, of the graph this node belongs to.
         * @param labels label names of this node.
         * @param properties properties of this node.
         * @param isDeleted whether this node is deleted.
         */
        CompositeGraphDirectNodeValue(
                long id, String elementId, long sourceId, TextArray labels, MapValue properties, boolean isDeleted) {
            super(id, elementId, labels, properties, isDeleted);
            this.sourceId = sourceId;
        }

        @Override
        public long sourceId() {
            return sourceId;
        }

        @Override
        public boolean equals(VirtualValue other) {
            if (!super.equals(other)) {
                return false;
            }

            if (other instanceof CompositeDatabaseValue compositeValue) {
                return sourceId == compositeValue.sourceId();
            } else {
                throw new IncomparableValuesException(
                        this.getClass().getSimpleName(), other.getClass().getSimpleName());
            }
        }

        @Override
        protected int computeHashToMemoize() {
            return 31 * Long.hashCode(id()) + Long.hashCode(sourceId);
        }
    }

    class CompositeDirectRelationshipValue extends RelationshipValue.DirectRelationshipValue
            implements CompositeDatabaseValue {
        private final long sourceId;

        /**
         * @param id the id of the relationship.
         * @param startNode start node of this relationship.
         * @param sourceId a unique id in the current transaction, of the graph this relationship belongs to.
         * @param endNode end node of this relationship.
         * @param type type name of this relationship.
         * @param properties properties of this relationship
         * @param isDeleted whether this node is deleted.
         */
        CompositeDirectRelationshipValue(
                long id,
                String elementId,
                long sourceId,
                VirtualNodeReference startNode,
                VirtualNodeReference endNode,
                TextValue type,
                MapValue properties,
                boolean isDeleted) {
            super(id, elementId, startNode, endNode, type, properties, isDeleted);
            this.sourceId = sourceId;
        }

        @Override
        public long sourceId() {
            return sourceId;
        }

        @Override
        public boolean equals(VirtualValue other) {
            if (!super.equals(other)) {
                return false;
            }

            if (other instanceof CompositeDatabaseValue compositeValue) {
                return sourceId == compositeValue.sourceId();
            } else {
                throw new IncomparableValuesException(
                        this.getClass().getSimpleName(), other.getClass().getSimpleName());
            }
        }

        @Override
        protected int computeHashToMemoize() {
            return 31 * Long.hashCode(id()) + Long.hashCode(sourceId);
        }
    }

    class CompositeFullNodeReference extends FullNodeReference implements CompositeDatabaseValue {

        private final long sourceId;

        public CompositeFullNodeReference(long id, String elementId, long sourceId) {
            super(id, elementId);
            this.sourceId = sourceId;
        }

        @Override
        public boolean equals(VirtualValue other) {
            if (!super.equals(other)) {
                return false;
            }

            if (other instanceof CompositeDatabaseValue compositeValue) {
                return sourceId == compositeValue.sourceId();
            } else {
                throw new IncomparableValuesException(
                        this.getClass().getSimpleName(), other.getClass().getSimpleName());
            }
        }

        @Override
        public long sourceId() {
            return sourceId;
        }

        @Override
        protected int computeHashToMemoize() {
            return 31 * Long.hashCode(id()) + Long.hashCode(sourceId);
        }
    }
}
