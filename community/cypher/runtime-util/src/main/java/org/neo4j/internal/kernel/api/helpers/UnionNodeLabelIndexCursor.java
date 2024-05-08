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
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.SkippableCursor;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.kernel.api.TokenReadSession;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.io.pagecache.context.CursorContext;

public abstract class UnionNodeLabelIndexCursor extends UnionTokenIndexCursor<SkippableCursor>
        implements SkippableCursor {

    public static UnionNodeLabelIndexCursor ascendingUnionNodeLabelIndexCursor(
            Read read,
            TokenReadSession tokenReadSession,
            CursorContext cursorContext,
            int[] labels,
            NodeLabelIndexCursor[] cursors)
            throws KernelException {
        assert labels.length == cursors.length;
        for (int i = 0; i < labels.length; i++) {
            read.nodeLabelScan(
                    tokenReadSession,
                    cursors[i],
                    IndexQueryConstraints.ordered(IndexOrder.ASCENDING),
                    new TokenPredicate(labels[i]),
                    cursorContext);
        }
        return new AscendingUnionLabelIndexCursor(cursors);
    }

    public static UnionNodeLabelIndexCursor ascendingUnionNodeLabelIndexCursor(SkippableCursor[] cursors) {
        return new AscendingUnionLabelIndexCursor(cursors);
    }

    public static UnionNodeLabelIndexCursor descendingUnionNodeLabelIndexCursor(
            Read read,
            TokenReadSession tokenReadSession,
            CursorContext cursorContext,
            int[] labels,
            NodeLabelIndexCursor[] cursors)
            throws KernelException {
        assert labels.length == cursors.length;
        for (int i = 0; i < labels.length; i++) {
            read.nodeLabelScan(
                    tokenReadSession,
                    cursors[i],
                    IndexQueryConstraints.ordered(IndexOrder.DESCENDING),
                    new TokenPredicate(labels[i]),
                    cursorContext);
        }
        return new DescendingUnionLabelIndexCursor(cursors);
    }

    public static UnionNodeLabelIndexCursor descendingUnionNodeLabelIndexCursor(SkippableCursor[] cursors) {
        return new DescendingUnionLabelIndexCursor(cursors);
    }

    public static UnionNodeLabelIndexCursor unionNodeLabelIndexCursor(NodeLabelIndexCursor[] cursors) {
        return new AscendingUnionLabelIndexCursor(cursors);
    }

    UnionNodeLabelIndexCursor(SkippableCursor[] cursors) {
        super(cursors);
    }

    @Override
    public void skipUntil(long id) {
        for (SkippableCursor cursor : cursors) {
            if (cursor != null) {
                cursor.skipUntil(id);
            }
        }
    }

    private static final class AscendingUnionLabelIndexCursor extends UnionNodeLabelIndexCursor {
        AscendingUnionLabelIndexCursor(SkippableCursor[] cursors) {
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

    private static final class DescendingUnionLabelIndexCursor extends UnionNodeLabelIndexCursor {
        DescendingUnionLabelIndexCursor(SkippableCursor[] cursors) {
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
