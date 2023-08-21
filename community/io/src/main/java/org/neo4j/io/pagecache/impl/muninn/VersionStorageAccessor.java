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
import org.neo4j.io.pagecache.PageCursor;

public interface VersionStorageAccessor {

    VersionStorageAccessor EMPTY_ACCESSOR = new VersionStorageAccessor() {
        @Override
        public long allocate(long visibilityBoundary, int requestedSize) {
            throw new UnsupportedOperationException("Empty accessor");
        }

        @Override
        public PageCursor io(long reference, int flags) {
            throw new UnsupportedOperationException("Empty accessor");
        }

        @Override
        public int maxAreaSize() {
            return 0;
        }
    };

    /**
     * Returns reference that can be used to open cursor via {@link #io(long, int)}
     * @param visibilityBoundary - referenced area can be reused once there is no observers that see transaction id below this one
     * @param requestedSize - size of the area to allocate
     * @return reference
     */
    long allocate(long visibilityBoundary, int requestedSize) throws IOException;

    /**
     * Returns ready-to-use cursor pointed to the page and offset specified by reference
     */
    PageCursor io(long reference, int flags) throws IOException;

    /**
     * @return maximum area size that can be allocated
     */
    int maxAreaSize();
}
