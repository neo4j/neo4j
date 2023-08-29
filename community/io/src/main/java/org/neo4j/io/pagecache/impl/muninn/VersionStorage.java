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
package org.neo4j.io.pagecache.impl.muninn;

import java.io.IOException;
import org.neo4j.io.pagecache.context.VersionContext;
import org.neo4j.io.pagecache.tracing.PinEvent;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public interface VersionStorage extends AutoCloseable, Lifecycle {
    int NEXT_REFERENCE_OFFSET = Long.BYTES;

    VersionStorage EMPTY_STORAGE = new EmptyVersionStorage();

    /**
     * Load snapshot of the page as determined by version context into provided page cursor.
     * As part of that snapshot loading cursor may be repointed to some page in specific version storage.
     * @param pageCursor user supplied cursor to read snapshot page
     * @param versionContext version context with information about required versions
     * @param pinEvent tracing event
     */
    void loadReadSnapshot(MuninnPageCursor pageCursor, VersionContext versionContext, PinEvent pinEvent);

    /**
     * Create version snapshot of the page provided by page cursor. Newly created version will be offloaded to version storage
     * and reference to that page returned as result of this call.
     *
     * @param pageCursor     user supplied cursor to write snapshot page updates
     * @param versionContext version context with information about ongoing version changes
     * @param chainHeadVersion    page chain head version
     * @param pinEvent       tracing event
     */
    long createPageSnapshot(
            MuninnPageCursor pageCursor, VersionContext versionContext, long chainHeadVersion, PinEvent pinEvent);

    /**
     * @return accessor that can allocate pages in version storage and provide PageCursor access to them
     */
    VersionStorageAccessor accessor();

    /**
     * Provide total size of versioned store
     */
    long size() throws IOException;

    @Override
    void close();

    class EmptyVersionStorage extends LifecycleAdapter implements VersionStorage {
        private static final long EMPTY_PAGE_REFERENCE = 0;

        @Override
        public void loadReadSnapshot(MuninnPageCursor pageCursor, VersionContext versionContext, PinEvent pinEvent) {}

        @Override
        public long createPageSnapshot(
                MuninnPageCursor pageCursor, VersionContext versionContext, long chainHeadVersion, PinEvent pinEvent) {
            return EMPTY_PAGE_REFERENCE;
        }

        @Override
        public long size() {
            return 0;
        }

        @Override
        public VersionStorageAccessor accessor() {
            return VersionStorageAccessor.EMPTY_ACCESSOR;
        }

        @Override
        public void close() {}
    }
}
