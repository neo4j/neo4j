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

import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;

public final class Cursors {

    private Cursors() {
        throw new UnsupportedOperationException("Do not instantiate");
    }

    /**
     * This is a stand in for a future kernel improvement where we hopefully can push this seek to the index.
     */
    public static boolean seekAscending(CompositeCursor cursor, long seek) {
        boolean hasMore = true;
        while (hasMore && cursor.reference() < seek) {
            hasMore = cursor.next();
        }
        return hasMore;
    }

    /**
     * This is a stand in for a future kernel improvement where we hopefully can push this seek to the index.
     */
    public static boolean seekAscending(NodeLabelIndexCursor cursor, long seek) {
        boolean hasMore = true;
        while (hasMore && cursor.nodeReference() < seek) {
            hasMore = cursor.next();
        }
        return hasMore;
    }

    /**
     * This is a stand in for a future kernel improvement where we hopefully can push this seek to the index.
     */
    public static boolean seekDescending(CompositeCursor cursor, long seek) {
        boolean hasMore = true;
        while (hasMore && cursor.reference() > seek) {
            hasMore = cursor.next();
        }
        return hasMore;
    }

    /**
     * This is a stand in for a future kernel improvement where we hopefully can push this seek to the index.
     */
    public static boolean seekDescending(NodeLabelIndexCursor cursor, long seek) {
        boolean hasMore = true;
        while (hasMore && cursor.nodeReference() > seek) {
            hasMore = cursor.next();
        }
        return hasMore;
    }
}
