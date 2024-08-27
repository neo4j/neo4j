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
package org.neo4j.kernel.impl.transaction.state.storeview;

import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.StorageEntityScanCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.cursor.StoreCursors;

/**
 * Injectable logic to a {@link StoreScan} to accommodate for different types of scans.
 */
interface EntityScanCursorBehaviour<CURSOR extends StorageEntityScanCursor<?>> {
    CURSOR allocateEntityScanCursor(
            CursorContext cursorContext, StoreCursors storeCursors, MemoryTracker memoryTracker);

    int[] readTokens(CURSOR cursor);

    default int[] readTokensAndProperties(
            CURSOR cursor, StoragePropertyCursor propertyCursor, PropertySelection selection) {
        var tokens = readTokens(cursor);
        cursor.properties(propertyCursor, selection);
        return tokens;
    }
}
