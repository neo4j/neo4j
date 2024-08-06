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
package org.neo4j.storageengine.api.cursor;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;

/**
 * Accessor for all underlying store cursors.
 * Provides requested store cursor that is using configuration from cursor context provided in {@link StoreCursors#reset(CursorContext)}.
 *
 * Store cursors provide read and write cursors.
 * We keep read cursors for as long as possible and close them only when we close instance of {@link StoreCursors}.
 * Write cursors still should be closed when they acquired.
 */
public interface StoreCursors extends AutoCloseable {
    StoreCursors NULL = new StoreCursorsAdapter();

    void reset(CursorContext cursorContext);

    PageCursor readCursor(CursorType type);

    PageCursor writeCursor(CursorType type);

    @Override
    void close();
}
