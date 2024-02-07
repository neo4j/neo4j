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
package org.neo4j.io.pagecache;

import com.sun.nio.file.ExtendedOpenOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import org.eclipse.collections.api.set.ImmutableSet;

/**
 * {@link OpenOption}s that are specific to {@link PageCache#map(Path, int, String, ImmutableSet)},
 * and not normally supported by file systems.
 */
public enum PageCacheOpenOptions implements OpenOption {
    /**
     * Map the file even if the specified file page size conflicts with an existing mapping of that file.
     * If so, the given file page size will be ignored and a {@link PagedFile} will be returned that uses the
     * file page size of the existing mapping.
     */
    ANY_PAGE_SIZE,

    /**
     * Map the file with direct I/O flag.
     * Please note that support for this option is limited.
     * Please check that your platform is supported before providing this option.
     * @see ExtendedOpenOption for details.
     */
    DIRECT,

    /**
     * Use big-endian byte order when operating with mapped file
     */
    BIG_ENDIAN,

    /**
     * Map with mvcc support. Each page has some bytes reserved for version data.
     */
    MULTI_VERSIONED,

    /**
     * Do not try to preallocate specific mapped file
     */
    NOPREALLOCATION,

    /**
     * Update context with version information in non multi_versioned mapped file
     */
    CONTEXT_VERSION_UPDATES
}
