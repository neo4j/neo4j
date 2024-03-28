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

import java.util.Arrays;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.DefaultCloseListenable;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.util.Preconditions;

public abstract class UnionTokenIndexCursor<CURSOR extends Cursor> extends DefaultCloseListenable
        implements CompositeCursor {
    private static final int UNINITIALIZED = -1;
    private final CURSOR[] cursors;
    private int currentCursorIndex = UNINITIALIZED;

    UnionTokenIndexCursor(CURSOR[] cursors) {
        assert cursors != null && cursors.length > 0;
        // we need will make a defensive copy here so that we can
        // null elements when done.
        this.cursors = Arrays.copyOf(cursors, cursors.length);
    }

    abstract int compare(long current, long other);

    abstract long reference(CURSOR cursor);

    abstract long extremeValue();

    @Override
    public final boolean next() {
        if (currentCursorIndex == UNINITIALIZED) {
            return initialize();
        } else {
            return internalNext();
        }
    }

    private boolean internalNext() {
        if (cursors[currentCursorIndex].next()) {
            findNext(reference(cursors[currentCursorIndex]));
            return true;
        } else {
            int oldCursorIndex = currentCursorIndex;
            cursors[oldCursorIndex] = null;
            findNext(extremeValue());
            return currentCursorIndex != oldCursorIndex;
        }
    }

    private void findNext(long currentReference) {
        for (int i = 0; i < cursors.length; i++) {
            if (i != currentCursorIndex) {
                var cursor = cursors[i];
                if (cursor != null) {
                    long otherReference = reference(cursor);
                    if (otherReference != StatementConstants.NO_SUCH_NODE) {
                        int compare = compare(currentReference, otherReference);
                        if (compare > 0) {
                            currentReference = otherReference;
                            currentCursorIndex = i;
                        } else if (compare == 0) {
                            if (!cursor.next()) {
                                cursors[i] = null;
                            }
                        }
                    }
                }
            }
        }
    }

    private boolean initialize() {
        long currentReference = extremeValue();
        for (int i = 0; i < cursors.length; i++) {
            final var cursor = cursors[i];
            if (cursor != null && cursor.next()) {
                long otherReference = reference(cursor);
                int compare = compare(currentReference, otherReference);
                if (compare > 0) {
                    currentReference = otherReference;
                    currentCursorIndex = i;
                } else if (compare == 0) {
                    if (!cursor.next()) {
                        cursors[i] = null;
                    }
                }
            } else {
                cursors[i] = null;
            }
        }
        return currentReference != extremeValue();
    }

    @Override
    public void setTracer(KernelReadTracer tracer) {
        for (CURSOR cursor : cursors) {
            if (cursor != null) {
                cursor.setTracer(tracer);
            }
        }
    }

    @Override
    public void removeTracer() {
        for (CURSOR cursor : cursors) {
            if (cursor != null) {
                cursor.removeTracer();
            }
        }
    }

    @Override
    public long reference() {
        Preconditions.checkArgument(
                cursors[currentCursorIndex] != null,
                "Calling `reference` after `next` has returned `false` is not allowed");
        return reference(cursors[currentCursorIndex]);
    }

    protected CURSOR current() {
        return cursors[currentCursorIndex];
    }

    @Override
    public void closeInternal() {
        // do nothing for
    }

    @Override
    public boolean isClosed() {
        return false;
    }
}
