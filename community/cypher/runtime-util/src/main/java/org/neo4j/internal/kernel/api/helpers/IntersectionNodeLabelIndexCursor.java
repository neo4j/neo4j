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
import org.neo4j.internal.kernel.api.DefaultCloseListenable;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.kernel.api.TokenReadSession;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.io.pagecache.context.CursorContext;

public abstract class IntersectionNodeLabelIndexCursor extends DefaultCloseListenable
        implements SkippableCompositeCursor {

    public static IntersectionNodeLabelIndexCursor ascendingIntersectionNodeLabelIndexCursor(
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
        return new AscendingIntersectionLabelIndexCursor(cursors);
    }

    public static IntersectionNodeLabelIndexCursor descendingIntersectionNodeLabelIndexCursor(
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
        return new DescendingIntersectionLabelIndexCursor(cursors);
    }

    public static IntersectionNodeLabelIndexCursor intersectionNodeLabelIndexCursor(NodeLabelIndexCursor[] cursors) {
        return new AscendingIntersectionLabelIndexCursor(cursors);
    }

    private final NodeLabelIndexCursor[] cursors;

    IntersectionNodeLabelIndexCursor(NodeLabelIndexCursor[] cursors) {
        assert cursors != null && cursors.length > 0;
        this.cursors = cursors;
    }

    abstract int compare(long current, long other);

    @Override
    public boolean next() {

        // advance all cursors once
        for (NodeLabelIndexCursor cursor : cursors) {
            if (!cursor.next()) {
                return false;
            }
        }

        if (cursors.length == 1) {
            return true;
        }

        // start from 0:th cursor and try to advance until all cursors point at the same node
        for (int i = 0; ; ) {
            var first = cursors[i];
            var second = cursors[i + 1];
            long firstReference = first.nodeReference();
            long secondReference = second.nodeReference();
            int compare = compare(firstReference, secondReference);
            if (compare == 0) {
                // we found a match, advance
                i++;
                if (i == cursors.length - 1) {
                    return true;
                }
            } else if (compare < 0) {
                // advance all cursors up to first and retry
                for (int j = 0; j <= i; j++) {
                    var cursor = cursors[j];
                    cursor.skipUntil(secondReference);
                    if (!cursor.next()) {
                        return false;
                    }
                }
                i = 0;
            } else {
                // advance second, and retry
                second.skipUntil(firstReference);
                if (!second.next()) {
                    return false;
                }
            }
        }
    }

    @Override
    public void setTracer(KernelReadTracer tracer) {
        for (NodeLabelIndexCursor cursor : cursors) {
            if (cursor != null) {
                cursor.setTracer(tracer);
            }
        }
    }

    @Override
    public void removeTracer() {
        for (NodeLabelIndexCursor cursor : cursors) {
            if (cursor != null) {
                cursor.removeTracer();
            }
        }
    }

    @Override
    public long reference() {
        return cursors[0].nodeReference();
    }

    @Override
    public void closeInternal() {
        // do nothing for
    }

    @Override
    public void skipUntil(long id) {
        for (NodeLabelIndexCursor cursor : cursors) {
            cursor.skipUntil(id);
        }
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    private static final class AscendingIntersectionLabelIndexCursor extends IntersectionNodeLabelIndexCursor {
        AscendingIntersectionLabelIndexCursor(NodeLabelIndexCursor[] cursors) {
            super(cursors);
        }

        @Override
        int compare(long current, long other) {
            return Long.compare(current, other);
        }
    }

    private static final class DescendingIntersectionLabelIndexCursor extends IntersectionNodeLabelIndexCursor {
        DescendingIntersectionLabelIndexCursor(NodeLabelIndexCursor[] cursors) {
            super(cursors);
        }

        @Override
        int compare(long current, long other) {
            return -Long.compare(current, other);
        }
    }
}
