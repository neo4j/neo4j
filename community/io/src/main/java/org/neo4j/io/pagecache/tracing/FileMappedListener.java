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
package org.neo4j.io.pagecache.tracing;

import org.neo4j.io.pagecache.PagedFile;

/**
 * Listener for a dynamic page cache file mapping.
 * When page cache will map new page file, on all registered listeners {@link #fileMapped(PagedFile)} will be invoked.
 * In the same way, during unmap - {@link #fileUnmapped(PagedFile)} will be invoked on all registered listeners
 */
public interface FileMappedListener {
    FileMappedListener EMPTY = new FileMappedListener() {
        @Override
        public void fileMapped(PagedFile pagedFile) {}

        @Override
        public void fileUnmapped(PagedFile pagedFile) {}
    };

    /**
     * Called from page cache after new file mapping was performed
     * @param pagedFile newly mapped paged file
     */
    void fileMapped(PagedFile pagedFile);

    /**
     * Called from page cache before file will be unmapped
     *
     * @param pagedFile page file that will be unmapped
     */
    void fileUnmapped(PagedFile pagedFile);
}
