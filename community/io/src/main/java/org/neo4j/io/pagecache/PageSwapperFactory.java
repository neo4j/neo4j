/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import java.io.IOException;

/**
 * Creates PageSwappers for the given files.
 *
 * A PageSwapper is responsible for swapping file pages in and out of memory.
 *
 * The PageSwapperFactory presumably knows about what file system to use.
 */
public interface PageSwapperFactory
{
    /**
     * Create a PageSwapper for the given file.
     * @param file The file that the PageSwapper will move file pages in and
     *             out of.
     * @param filePageSize The size of the pages in the file. Presumably a
     *                     multiple of some record size.
     * @param onEviction The PageSwapper will be told about evictions, and has
     *                   the responsibility of informing the PagedFile via this callback.
     * @return A working PageSwapper instance for the given file.
     * @throws IOException If the PageSwapper could not be created, for
     * instance if the underlying file could not be opened.
     */
    public PageSwapper createPageSwapper(
            File file,
            int filePageSize,
            PageEvictionCallback onEviction ) throws IOException;
}
