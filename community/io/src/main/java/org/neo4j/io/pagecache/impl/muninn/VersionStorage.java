/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.io.pagecache.impl.muninn;

import java.io.IOException;
import org.neo4j.io.pagecache.context.VersionContext;
import org.neo4j.io.pagecache.tracing.PinEvent;

public interface VersionStorage extends AutoCloseable {
    int NEXT_ID_OFFSET = Long.BYTES;
    int CHECKSUM_OFFSET = Long.BYTES * 2;

    VersionStorage EMPTY_STORAGE = new VersionStorage() {
        @Override
        public void loadReadSnapshot(MuninnPageCursor pageCursor, VersionContext versionContext, PinEvent pinEvent) {}

        @Override
        public void loadWriteSnapshot(MuninnPageCursor pageCursor, VersionContext versionContext, PinEvent pinEvent) {}

        @Override
        public long size() {
            return 0;
        }

        @Override
        public void patchSnapshotChain(MuninnPageCursor pageCursor) {}

        @Override
        public void close() {}
    };

    /**
     * Load snapshot of the page as determined by version context into provided page cursor.
     * As part of that snapshot loading cursor may be repointed to some page in specific version storage.
     * @param pageCursor user supplied cursor to read snapshot page
     * @param versionContext version context with information about required versions
     * @param pinEvent tracing event
     */
    void loadReadSnapshot(MuninnPageCursor pageCursor, VersionContext versionContext, PinEvent pinEvent);

    /**
     * Load write version snapshot to provided page cursor. If any page copies will be required as part of that
     * version storage will be able to do that. As part of that work cursor may be repointed to newly created page or
     * to previously existing page in version storage.
     * @param pageCursor user supplied cursor to write snapshot page updates
     * @param versionContext version context with information about ongoing version changes
     * @param pinEvent tracing event
     */
    void loadWriteSnapshot(MuninnPageCursor pageCursor, VersionContext versionContext, PinEvent pinEvent);

    /**
     * Update chain version when write is complete to particular snapshot.
     * As result of snapshot patching any update that was written to particular snapshot will be propagated to
     * the all versions up to the head of chain of snapshot.
     * @param pageCursor user supplied write cursor.
     */
    void patchSnapshotChain(MuninnPageCursor pageCursor);

    /**
     * Provide total size of versioned store
     */
    long size() throws IOException;

    @Override
    void close();
}
