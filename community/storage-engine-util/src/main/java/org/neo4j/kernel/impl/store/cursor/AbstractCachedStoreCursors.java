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
package org.neo4j.kernel.impl.store.cursor;

import static org.neo4j.internal.helpers.Numbers.safeCastIntToShort;
import static org.neo4j.util.FeatureToggles.flag;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.storageengine.api.cursor.CursorType;
import org.neo4j.storageengine.api.cursor.StoreCursors;

public abstract class AbstractCachedStoreCursors implements StoreCursors {
    private static final boolean CHECK_READ_CURSORS =
            flag(AbstractCachedStoreCursors.class, "CHECK_READ_CURSORS", false);
    protected CursorContext cursorContext;
    private final int numTypes;

    protected PageCursor[] cursorsByType;

    public AbstractCachedStoreCursors(CursorContext cursorContext, int numTypes) {
        this.cursorContext = cursorContext;
        this.numTypes = numTypes;
        this.cursorsByType = createEmptyCursorArray();
    }

    @Override
    public void reset(CursorContext cursorContext) {
        this.cursorContext = cursorContext;
        resetCursors();
    }

    private void resetCursors() {
        for (int i = 0; i < cursorsByType.length; i++) {
            PageCursor pageCursor = cursorsByType[i];
            if (pageCursor != null) {
                if (CHECK_READ_CURSORS) {
                    checkReadCursor(pageCursor, safeCastIntToShort(i));
                }
                pageCursor.close();
            }
        }
        cursorsByType = createEmptyCursorArray();
    }

    @Override
    public PageCursor readCursor(CursorType type) {
        short value = type.value();
        var cursor = cursorsByType[value];
        if (cursor == null) {
            cursor = createReadCursor(type);
            cursorsByType[value] = cursor;
        }
        return cursor;
    }

    protected abstract PageCursor createReadCursor(CursorType type);

    @Override
    public void close() {
        resetCursors();
    }

    private PageCursor[] createEmptyCursorArray() {
        return new PageCursor[numTypes];
    }

    private static void checkReadCursor(PageCursor pageCursor, short type) {
        if (pageCursor.getRawCurrentFile() == null) {
            throw new IllegalStateException("Read cursor " + ReflectionToStringBuilder.toString(pageCursor)
                    + " with type: " + type + " is closed outside of owning store cursors.");
        }
    }
}
