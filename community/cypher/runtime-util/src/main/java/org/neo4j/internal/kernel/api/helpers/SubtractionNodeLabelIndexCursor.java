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

import static org.neo4j.internal.kernel.api.helpers.IntersectionNodeLabelIndexCursor.ascendingIntersectionNodeLabelIndexCursor;
import static org.neo4j.internal.kernel.api.helpers.IntersectionNodeLabelIndexCursor.descendingIntersectionNodeLabelIndexCursor;
import static org.neo4j.internal.kernel.api.helpers.UnionNodeLabelIndexCursor.ascendingUnionNodeLabelIndexCursor;
import static org.neo4j.internal.kernel.api.helpers.UnionNodeLabelIndexCursor.descendingUnionNodeLabelIndexCursor;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.DefaultCloseListenable;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.SkippableCursor;
import org.neo4j.internal.kernel.api.TokenReadSession;
import org.neo4j.io.pagecache.context.CursorContext;

public abstract class SubtractionNodeLabelIndexCursor extends DefaultCloseListenable implements SkippableCursor {

    private final SkippableCursor positiveCursor;
    private final SkippableCursor negativeCursor;
    private boolean negativeCursorHasData;
    private boolean first = true;

    public static SubtractionNodeLabelIndexCursor ascendingSubtractionNodeLabelIndexCursor(
            Read read,
            TokenReadSession tokenReadSession,
            CursorContext cursorContext,
            int[] positiveLabels,
            int[] negativeLabels,
            NodeLabelIndexCursor[] positiveCursors,
            NodeLabelIndexCursor[] negativeCursors)
            throws KernelException {
        return new AscendingSubtractionLabelIndexCursor(
                ascendingIntersectionNodeLabelIndexCursor(
                        read, tokenReadSession, cursorContext, positiveLabels, positiveCursors),
                ascendingUnionNodeLabelIndexCursor(
                        read, tokenReadSession, cursorContext, negativeLabels, negativeCursors));
    }

    public static SubtractionNodeLabelIndexCursor descendingSubtractionNodeLabelIndexCursor(
            Read read,
            TokenReadSession tokenReadSession,
            CursorContext cursorContext,
            int[] positiveLabels,
            int[] negativeLabels,
            NodeLabelIndexCursor[] positiveCursors,
            NodeLabelIndexCursor[] negativeCursors)
            throws KernelException {
        return new DescendingSubtractionLabelIndexCursor(
                descendingIntersectionNodeLabelIndexCursor(
                        read, tokenReadSession, cursorContext, positiveLabels, positiveCursors),
                descendingUnionNodeLabelIndexCursor(
                        read, tokenReadSession, cursorContext, negativeLabels, negativeCursors));
    }

    public static SubtractionNodeLabelIndexCursor subtractionNodeLabelIndexCursor(
            NodeLabelIndexCursor[] positiveCursors, NodeLabelIndexCursor[] negativeCursor) {
        return new AscendingSubtractionLabelIndexCursor(
                IntersectionNodeLabelIndexCursor.intersectionNodeLabelIndexCursor(positiveCursors),
                UnionNodeLabelIndexCursor.unionNodeLabelIndexCursor(negativeCursor));
    }

    SubtractionNodeLabelIndexCursor(SkippableCursor positiveCursor, SkippableCursor negativeCursor) {
        this.positiveCursor = positiveCursor;
        this.negativeCursor = negativeCursor;
    }

    @Override
    public long reference() {
        return positiveCursor.reference();
    }

    @Override
    public void closeInternal() {
        // do nothing
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void skipUntil(long id) {
        positiveCursor.skipUntil(id);
        negativeCursor.skipUntil(id);
    }

    abstract int compare(long a, long b);

    @Override
    public boolean next() {
        if (first) {
            negativeCursorHasData = negativeCursor.next();
            first = false;
        }
        boolean shouldContinue = positiveCursor.next();
        boolean localNegativeCursorHasData = negativeCursorHasData;
        while (shouldContinue) {
            if (!localNegativeCursorHasData) {
                return true;
            }
            long positiveId = positiveCursor.reference();
            int compare = compare(positiveId, negativeCursor.reference());
            if (compare < 0) {
                return true;
            } else if (compare > 0) {
                negativeCursor.skipUntil(positiveId);
                localNegativeCursorHasData = negativeCursor.next();
            } else {
                shouldContinue = positiveCursor.next();
                if (shouldContinue) {
                    negativeCursor.skipUntil(positiveId);
                    localNegativeCursorHasData = negativeCursor.next();
                }
            }
            negativeCursorHasData = localNegativeCursorHasData;
        }
        return false;
    }

    @Override
    public void setTracer(KernelReadTracer tracer) {
        positiveCursor.setTracer(tracer);
        negativeCursor.setTracer(tracer);
    }

    @Override
    public void removeTracer() {
        positiveCursor.removeTracer();
        negativeCursor.removeTracer();
    }

    private static final class AscendingSubtractionLabelIndexCursor extends SubtractionNodeLabelIndexCursor {
        AscendingSubtractionLabelIndexCursor(SkippableCursor positiveCursor, SkippableCursor negativeCursor) {
            super(positiveCursor, negativeCursor);
        }

        @Override
        int compare(long current, long other) {
            return Long.compare(current, other);
        }
    }

    private static final class DescendingSubtractionLabelIndexCursor extends SubtractionNodeLabelIndexCursor {
        DescendingSubtractionLabelIndexCursor(SkippableCursor positiveCursor, SkippableCursor negativeCursor) {
            super(positiveCursor, negativeCursor);
        }

        @Override
        int compare(long current, long other) {
            return -Long.compare(current, other);
        }
    }
}
