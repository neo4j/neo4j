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

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.DefaultCloseListenable;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.TokenReadSession;
import org.neo4j.io.pagecache.context.CursorContext;

public abstract class SubtractionNodeLabelIndexCursor extends DefaultCloseListenable implements CompositeCursor {

    private final CompositeCursor positiveCursor;
    private final CompositeCursor negativeCursor;
    private boolean negativeCursorIsExhausted;

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
                ascendingIntersectionNodeLabelIndexCursor(
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
                descendingIntersectionNodeLabelIndexCursor(
                        read, tokenReadSession, cursorContext, negativeLabels, negativeCursors));
    }

    SubtractionNodeLabelIndexCursor(CompositeCursor positiveCursor, CompositeCursor negativeCursor) {
        this.positiveCursor = positiveCursor;
        this.negativeCursor = negativeCursor;
        this.negativeCursorIsExhausted = !negativeCursor.next();
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

    abstract int compare(long a, long b);

    @Override
    public boolean next() {
        boolean shouldContinue = positiveCursor.next();
        boolean localNegativeExhausted = negativeCursorIsExhausted;
        while (shouldContinue) {
            if (localNegativeExhausted) {
                return true;
            }
            long positiveId = positiveCursor.reference();
            int compare = compare(positiveId, negativeCursor.reference());
            if (compare < 0) {
                return true;
            } else if (compare > 0) {
                while (!localNegativeExhausted && compare(positiveId, negativeCursor.reference()) > 0) {
                    localNegativeExhausted = !negativeCursor.next();
                }
            } else {
                shouldContinue = positiveCursor.next();
                localNegativeExhausted = !negativeCursor.next();
            }
            negativeCursorIsExhausted = localNegativeExhausted;
        }
        return shouldContinue;
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
        AscendingSubtractionLabelIndexCursor(CompositeCursor positiveCursor, CompositeCursor negativeCursor) {
            super(positiveCursor, negativeCursor);
        }

        @Override
        int compare(long current, long other) {
            return Long.compare(current, other);
        }
    }

    private static final class DescendingSubtractionLabelIndexCursor extends SubtractionNodeLabelIndexCursor {
        DescendingSubtractionLabelIndexCursor(CompositeCursor positiveCursor, CompositeCursor negativeCursor) {
            super(positiveCursor, negativeCursor);
        }

        @Override
        int compare(long current, long other) {
            return -Long.compare(current, other);
        }
    }
}
