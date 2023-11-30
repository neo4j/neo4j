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

import java.util.Comparator;
import org.neo4j.exceptions.IncomparableValuesException;
import org.neo4j.values.AnyValue;
import org.neo4j.values.VirtualValue;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;

public interface CompositeDatabaseValue {
    long sourceId();

    final class CompositeGraphDirectNodeValue extends NodeValue.DirectNodeValue implements CompositeDatabaseValue {
        private final long sourceId;

        /**
         * @param id internal id of the node.
         * @param elementId the element id of the node.
         * @param sourceId a unique id of the constituent graph in the current transaction.
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

        /**
         * @param other
         * @return true if the entity is a node which has the same id and sourceId as this object, otherwise false.
         */
        @Override
        protected boolean equals(VirtualNodeValue other) {
            if (!super.equals(other)) {
                return false;
            }

            if (other instanceof CompositeDatabaseValue compositeValue) {
                return sourceId == compositeValue.sourceId();
            } else if (other instanceof VirtualNodeReference nodeReference) {
                return elementId().equals(nodeReference.elementId());
            } else {
                /*
                 * If we get here, it means we try to compare a composite node with a non-composite node, which has the same id.
                 * It is not possible to compare those values as they might OR might not refer to the same element.
                 *
                 * We should never get here, as we always work with either only composite or only non-composite values.
                 */
                throw new IncomparableValuesException(
                        this.getClass().getSimpleName(), other.getClass().getSimpleName());
            }
        }

        @Override
        public int unsafeCompareTo(VirtualValue other, Comparator<AnyValue> comparator) {
            var idComparison = Long.compare(id(), ((VirtualNodeValue) other).id());

            return idComparison != 0
                    ? idComparison
                    : Long.compare(sourceId(), ((CompositeDatabaseValue) other).sourceId());
        }

        @Override
        protected int computeHashToMemoize() {
            return 31 * Long.hashCode(id()) + Long.hashCode(sourceId);
        }
    }

    final class CompositeDirectRelationshipValue extends RelationshipValue.DirectRelationshipValue
            implements CompositeDatabaseValue {
        private final long sourceId;
        /**
         * @param id the id of the relationship.
         * @param elementId the element id of the relationship.
         * @param sourceId a unique id of the constituent graph in the current transaction.
         * @param startNode start node of this relationship.
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

        /**
         * @param other
         * @return true if the entity is a relationship which has the same id and sourceId as this object, otherwise false.
         */
        @Override
        public boolean equals(VirtualRelationshipValue other) {
            if (!super.equals(other)) {
                return false;
            }

            if (other instanceof CompositeDatabaseValue compositeValue) {
                return sourceId == compositeValue.sourceId();
            } else if (other instanceof RelationshipValue relationshipValue) {
                return elementId().equals(relationshipValue.elementId());
            } else {
                /*
                 * If we get here, it means we try to compare a composite relationship with a non-composite relationship, which has the same id.
                 * It is not possible to compare those values as they might OR might not refer to the same element.
                 *
                 * We should never get here, as we always work with either only composite or only non-composite values.
                 */
                throw new IncomparableValuesException(
                        this.getClass().getSimpleName(), other.getClass().getSimpleName());
            }
        }

        @Override
        public int unsafeCompareTo(VirtualValue other, Comparator<AnyValue> comparator) {
            var idComparison = Long.compare(id(), ((VirtualNodeValue) other).id());

            return idComparison != 0
                    ? idComparison
                    : Long.compare(sourceId(), ((CompositeDatabaseValue) other).sourceId());
        }

        @Override
        protected int computeHashToMemoize() {
            return 31 * Long.hashCode(id()) + Long.hashCode(sourceId);
        }
    }

    final class CompositeFullNodeReference extends FullNodeReference implements CompositeDatabaseValue {

        private final long sourceId;

        /**
         * @param id the id of the node.
         * @param elementId the element id of the node.
         * @param sourceId a unique id of the constituent graph in the current transaction.
         */
        public CompositeFullNodeReference(long id, String elementId, long sourceId) {
            super(id, elementId);
            this.sourceId = sourceId;
        }
        /**
         * @param other
         * @return true if the entity is a node which has the same id and sourceId as this object, otherwise false.
         */
        @Override
        protected boolean equals(VirtualNodeValue other) {
            if (!super.equals(other)) {
                return false;
            }

            if (other instanceof CompositeDatabaseValue compositeValue) {
                return sourceId == compositeValue.sourceId();
            } else if (other instanceof VirtualNodeReference nodeReference) {
                return elementId().equals(nodeReference.elementId());
            } else {
                /*
                 * If we get here, it means we try to compare a composite node with a non-composite node, which has the same id.
                 * It is not possible to compare those values as they might OR might not refer to the same element.
                 *
                 * We should never get here, as we always work with either only composite or only non-composite values.
                 */
                throw new IncomparableValuesException(
                        this.getClass().getSimpleName(), other.getClass().getSimpleName());
            }
        }

        @Override
        public int unsafeCompareTo(VirtualValue other, Comparator<AnyValue> comparator) {
            var idComparison = Long.compare(id(), ((VirtualNodeValue) other).id());

            return idComparison != 0
                    ? idComparison
                    : Long.compare(sourceId(), ((CompositeDatabaseValue) other).sourceId());
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
