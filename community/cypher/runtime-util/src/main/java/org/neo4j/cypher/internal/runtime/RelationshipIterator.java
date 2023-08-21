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
package org.neo4j.cypher.internal.runtime;

import org.eclipse.collections.api.iterator.LongIterator;
import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.storageengine.api.RelationshipVisitor;

public interface RelationshipIterator extends LongIterator {
    /**
     * Can be called to visit the data about the most recent id returned from {@link #next()}.
     */
    <EXCEPTION extends Exception> boolean relationshipVisit(long relationshipId, RelationshipVisitor<EXCEPTION> visitor)
            throws EXCEPTION;

    long startNodeId();

    long endNodeId();

    default long otherNodeId(long node) {
        return node == startNodeId() ? endNodeId() : startNodeId();
    }

    int typeId();

    class Empty extends PrimitiveLongCollections.AbstractPrimitiveLongBaseIterator implements RelationshipIterator {
        @Override
        public <EXCEPTION extends Exception> boolean relationshipVisit(
                long relationshipId, RelationshipVisitor<EXCEPTION> visitor) { // Nothing to visit
            return false;
        }

        @Override
        protected boolean fetchNext() {
            return false;
        }

        @Override
        public long startNodeId() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long endNodeId() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int typeId() {
            throw new UnsupportedOperationException();
        }
    }

    RelationshipIterator EMPTY = new Empty();
}
