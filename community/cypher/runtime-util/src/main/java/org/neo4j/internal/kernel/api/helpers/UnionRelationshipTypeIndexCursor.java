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
package org.neo4j.internal.kernel.api.helpers;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.kernel.api.TokenReadSession;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.Reference;

public abstract class UnionRelationshipTypeIndexCursor extends UnionTokenIndexCursor<RelationshipTypeIndexCursor>
        implements RelationshipIndexCursor {

    public static UnionRelationshipTypeIndexCursor ascendingUnionRelationshipTypeIndexCursor(
            Read read,
            TokenReadSession tokenReadSession,
            CursorContext cursorContext,
            int[] types,
            RelationshipTypeIndexCursor[] cursors)
            throws KernelException {
        assert types.length == cursors.length;
        for (int i = 0; i < types.length; i++) {
            read.relationshipTypeScan(
                    tokenReadSession,
                    cursors[i],
                    IndexQueryConstraints.ordered(IndexOrder.ASCENDING),
                    new TokenPredicate(types[i]),
                    cursorContext);
        }
        return new AscendingUnionRelationshipTypeIndexCursor(cursors);
    }

    public static UnionRelationshipTypeIndexCursor descendingUnionRelationshipTypeIndexCursor(
            Read read,
            TokenReadSession tokenReadSession,
            CursorContext cursorContext,
            int[] types,
            RelationshipTypeIndexCursor[] cursors)
            throws KernelException {
        assert types.length == cursors.length;
        for (int i = 0; i < types.length; i++) {
            read.relationshipTypeScan(
                    tokenReadSession,
                    cursors[i],
                    IndexQueryConstraints.ordered(IndexOrder.DESCENDING),
                    new TokenPredicate(types[i]),
                    cursorContext);
        }
        return new DescendingUnionRelationshipTypeIndexCursor(cursors);
    }

    public static UnionRelationshipTypeIndexCursor unionRelationshipTypeIndexCursor(
            RelationshipTypeIndexCursor[] cursors) {
        return new AscendingUnionRelationshipTypeIndexCursor(cursors);
    }

    UnionRelationshipTypeIndexCursor(RelationshipTypeIndexCursor[] cursors) {
        super(cursors);
    }

    @Override
    public boolean readFromStore() {
        return current().readFromStore();
    }

    @Override
    public long sourceNodeReference() {
        return current().sourceNodeReference();
    }

    @Override
    public long targetNodeReference() {
        return current().targetNodeReference();
    }

    @Override
    public int type() {
        return current().type();
    }

    @Override
    public void properties(PropertyCursor cursor, PropertySelection selection) {
        current().properties(cursor, selection);
    }

    @Override
    public Reference propertiesReference() {
        return current().propertiesReference();
    }

    @Override
    public long relationshipReference() {
        return reference();
    }

    @Override
    public void source(NodeCursor cursor) {
        current().source(cursor);
    }

    @Override
    public void target(NodeCursor cursor) {
        current().target(cursor);
    }

    @Override
    public float score() {
        return Float.NaN;
    }

    private static final class AscendingUnionRelationshipTypeIndexCursor extends UnionRelationshipTypeIndexCursor {
        AscendingUnionRelationshipTypeIndexCursor(RelationshipTypeIndexCursor[] cursors) {
            super(cursors);
        }

        @Override
        int compare(long current, long other) {
            return Long.compare(current, other);
        }

        @Override
        long extremeValue() {
            return Long.MAX_VALUE;
        }
    }

    private static final class DescendingUnionRelationshipTypeIndexCursor extends UnionRelationshipTypeIndexCursor {
        DescendingUnionRelationshipTypeIndexCursor(RelationshipTypeIndexCursor[] cursors) {
            super(cursors);
        }

        @Override
        int compare(long current, long other) {
            return -Long.compare(current, other);
        }

        @Override
        long extremeValue() {
            return Long.MIN_VALUE;
        }
    }
}
