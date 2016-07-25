/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;

/**
 * A page caching mechanism that allows caching multiple files and accessing their data
 * in pages via a re-usable cursor.
 * <p>
 * This interface does not specify the cache eviction and allocation behavior, it may be
 * backed by implementations that map entire files into RAM, or implementations with smart
 * eviction strategies, trying to keep "hot" pages in RAM.
 */
public interface PageCache extends AutoCloseable
{
    /**
     * Ask for a handle to a paged file, backed by this page cache.
     * <p>
     * Note that this currently asks for the pageSize to use, which is an artifact or records being
     * of varying size in the stores. This should be consolidated to use a standard page size for the
     * whole cache, with records aligning on those page boundaries.
     *
     * @param file The file to map.
     * @param pageSize The file page size to use for this mapping. If the file is already mapped with a different page
     * size, an exception will be thrown.
     * @param openOptions The set of open options to use for mapping this file.
     * The {@link StandardOpenOption#READ} and {@link StandardOpenOption#WRITE} options always implicitly specified.
     * The {@link StandardOpenOption#CREATE} open option will create the given file if it does not already exist, and
     * the {@link StandardOpenOption#TRUNCATE_EXISTING} will truncate any existing file <em>iff</em> it has not already
     * been mapped.
     * The {@link StandardOpenOption#DELETE_ON_CLOSE} will cause the file to be deleted after the last unmapping.
     * The {@link PageCacheOpenOptions#EXCLUSIVE} will cause the {@code map} method to throw if the file is already
     * mapped. Otherwise, the file will be mapped exclusively, and subsequent attempts at mapping the file will fail
     * with an exception until the exclusively mapped file is closed.
     * All other options are either silently ignored, or will cause an exception to be thrown.
     * @throws java.nio.file.NoSuchFileException if the given file does not exist, and the
     * {@link StandardOpenOption#CREATE} option was not specified.
     * @throws IOException if the file could otherwise not be mapped. Causes include the file being locked, or exclusive
     * mapping conflicts.
     */
    PagedFile map( File file, int pageSize, OpenOption... openOptions ) throws IOException;

    /** Flush all dirty pages */
    void flushAndForce() throws IOException;

    /**
     * Flush all dirty pages, but limit the rate of IO as advised by the given IOPSLimiter.
     * @param limiter The {@link IOLimiter} that determines if pauses or sleeps should be injected into the flushing
     * process to keep the IO rate down.
     */
    void flushAndForce( IOLimiter limiter ) throws IOException;

    /** Flush all dirty pages and close the page cache. */
    void close() throws IOException;

    /** The size in bytes of the pages managed by this cache. */
    int pageSize();

    /** The max number of cached pages. */
    int maxCachedPages();
}
