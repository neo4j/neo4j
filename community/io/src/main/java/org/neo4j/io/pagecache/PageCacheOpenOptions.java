/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.io.pagecache;

import java.io.File;
import java.nio.file.OpenOption;

/**
 * {@link OpenOption}s that are specific to {@link PageCache#map(File, int, OpenOption...)},
 * and not normally supported by file systems.
 */
public enum PageCacheOpenOptions implements OpenOption
{
    /**
     * Map the file even if the specified file page size conflicts with an existing mapping of that file.
     * If so, the given file page size will be ignored and a {@link PagedFile} will be returned that uses the
     * file page size of the existing mapping.
     */
    ANY_PAGE_SIZE
}
