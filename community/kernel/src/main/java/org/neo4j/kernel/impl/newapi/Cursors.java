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
package org.neo4j.kernel.impl.newapi;

import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_NODE;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_RELATIONSHIP;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_RELATIONSHIP_TYPE;
import static org.neo4j.storageengine.api.LongReference.NULL_REFERENCE;

import org.neo4j.internal.kernel.api.CloseListener;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.Reference;

public final class Cursors {
    private Cursors() {
        throw new UnsupportedOperationException("Don't instantiate");
    }

    /**
     * Returns an empty relationship traversal cursor.
     *
     * Note, the Read is required for situations where we for example try to
     * read properties from this cursor. If we did nothing in that case the
     * property cursor could end up in an undefined state that lead to errors
     * in some cases.
     */
    public static RelationshipTraversalCursor emptyTraversalCursor(org.neo4j.internal.kernel.api.Read read) {
        return new EmptyTraversalCursor(read);
    }

    public static class EmptyTraversalCursor implements RelationshipTraversalCursor {
        private final org.neo4j.internal.kernel.api.Read read;

        private EmptyTraversalCursor(org.neo4j.internal.kernel.api.Read read) {
            this.read = read;
        }

        @Override
        public void otherNode(NodeCursor cursor) {
            read.singleNode(NO_SUCH_NODE, cursor);
        }

        @Override
        public long otherNodeReference() {
            return NO_SUCH_NODE;
        }

        @Override
        public long originNodeReference() {
            return NO_SUCH_NODE;
        }

        @Override
        public boolean next() {
            return false;
        }

        @Override
        public void setTracer(KernelReadTracer tracer) {}

        @Override
        public void removeTracer() {}

        @Override
        public void close() {}

        @Override
        public void closeInternal() {}

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void setCloseListener(CloseListener closeListener) {}

        @Override
        public void setToken(int token) {}

        @Override
        public int getToken() {
            return -1;
        }

        @Override
        public long relationshipReference() {
            return NO_SUCH_RELATIONSHIP;
        }

        @Override
        public int type() {
            return NO_SUCH_RELATIONSHIP_TYPE;
        }

        @Override
        public void source(NodeCursor cursor) {
            read.singleNode(NO_SUCH_NODE, cursor);
        }

        @Override
        public void target(NodeCursor cursor) {
            read.singleNode(NO_SUCH_NODE, cursor);
        }

        @Override
        public void properties(PropertyCursor cursor, PropertySelection selection) {
            ((DefaultPropertyCursor) cursor).initEmptyRelationship();
        }

        @Override
        public long sourceNodeReference() {
            return NO_SUCH_NODE;
        }

        @Override
        public long targetNodeReference() {
            return NO_SUCH_NODE;
        }

        @Override
        public Reference propertiesReference() {
            return NULL_REFERENCE;
        }
    }
}
