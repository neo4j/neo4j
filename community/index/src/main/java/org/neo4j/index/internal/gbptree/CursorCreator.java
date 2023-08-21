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
package org.neo4j.index.internal.gbptree;

import java.io.IOException;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.context.CursorContext;

@FunctionalInterface
public interface CursorCreator {
    PageCursor create() throws IOException;

    /**
     * Wraps {@link PageCursor#openLinkedCursor(long)} of provided cursor as {@link CursorCreator}
     */
    static CursorCreator bind(PageCursor cursor) {
        return () -> cursor.openLinkedCursor(0);
    }

    /**
     * Wraps {@link PagedFile#io(long, int, CursorContext)} of provided file as {@link CursorCreator}
     */
    static CursorCreator bind(PagedFile pagedFile, int pfFlags, CursorContext context) {
        return () -> pagedFile.io(0, pfFlags, context);
    }

    /**
     * Wraps {@link OffloadPageCursorFactory#create(long, int, CursorContext)} as {@link CursorCreator}
     */
    static CursorCreator bind(OffloadPageCursorFactory pcFactory, int pfFlags, CursorContext context) {
        return () -> pcFactory.create(0, pfFlags, context);
    }

    /**
     * Wraps {@link RootLayerSupport#openCursor(int, CursorContext)} as {@link CursorCreator}
     */
    static CursorCreator bind(RootLayerSupport support, int pfFlags, CursorContext context) {
        return () -> support.openCursor(pfFlags, context);
    }
}
