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
import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Optional;

import org.neo4j.io.fs.FileSystemAbstraction;

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
     * The default {@link #pageSize()}.
     */
    int PAGE_SIZE = 8192;

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
     * All other options are either silently ignored, or will cause an exception to be thrown.
     * @throws java.nio.file.NoSuchFileException if the given file does not exist, and the
     * {@link StandardOpenOption#CREATE} option was not specified.
     * @throws IOException if the file could otherwise not be mapped. Causes include the file being locked.
     */
    PagedFile map( File file, int pageSize, OpenOption... openOptions ) throws IOException;

    /**
     * Ask for an already mapped paged file, backed by this page cache.
     * <p>
     * If mapping exist, the returned {@link Optional} will report {@link Optional#isPresent()} true and
     * {@link Optional#get()} will return the same {@link PagedFile} instance that was initially returned my
     * {@link #map(File, int, OpenOption...)}.
     * If no mapping exist for this file, then returned {@link Optional} will report {@link Optional#isPresent()}
     * false.
     * <p>
     * <strong>NOTE:</strong> The calling code is responsible for closing the returned paged file, if any.
     *
     * @param file The file to try to get the mapped paged file for.
     * @return {@link Optional} containing the {@link PagedFile} mapped by this {@link PageCache} for given file, or an
     * empty {@link Optional} if no mapping exist.
     * @throws IOException if page cache has been closed or page eviction problems occur.
     */
    Optional<PagedFile> getExistingMapping( File file ) throws IOException;

    /**
     * List a snapshot of the current file mappings.
     * <p>
     * The mappings can change as soon as this method returns.
     * <p>
     * <strong>NOTE:</strong> The calling code should <em>not</em> close the returned paged files, unless it does so
     * in collaboration with the code that originally mapped the file. Any reference count in the mapping will
     * <em>not</em> be incremented by this method, so calling code must be prepared for that the returned
     * {@link PagedFile}s can be asynchronously closed elsewhere.
     *
     * @throws IOException if page cache has been closed or page eviction problems occur.
     */
    List<PagedFile> listExistingMappings() throws IOException;

    /**
     * Flush all dirty pages.
     */
    void flushAndForce() throws IOException;

    /**
     * Flush all dirty pages, but limit the rate of IO as advised by the given IOPSLimiter.
     *
     * @param limiter The {@link IOLimiter} that determines if pauses or sleeps should be injected into the flushing
     * process to keep the IO rate down.
     */
    void flushAndForce( IOLimiter limiter ) throws IOException;

    /**
     * Close the page cache to prevent any future mapping of files.
     * This also releases any internal resources, including the {@link PageSwapperFactory} through its
     * {@link PageSwapperFactory#close() close} method.
     *
     * @throws IllegalStateException if not all files have been unmapped, with {@link PagedFile#close()}, prior to
     * closing the page cache. In this case, the page cache <em>WILL NOT</em> be considered to be successfully closed.
     * @throws RuntimeException if the {@link PageSwapperFactory#close()} method throws. In this case the page cache
     * <em>WILL BE</em> considered to have been closed successfully.
     */
    @Override
    void close() throws IllegalStateException;

    /**
     * The size in bytes of the pages managed by this cache.
     */
    int pageSize();

    /**
     * The max number of cached pages.
     */
    long maxCachedPages();

    /**
     * Get the {@link FileSystemAbstraction} that represents the filesystem where the paged files reside.
     *
     * @return the filesystem that the page cache is using.
     */
    FileSystemAbstraction getCachedFileSystem();

    /**
     * Report any thread-local events to the global page cache tracer, as if acquiring a thread-specific page cursor
     * tracer, and reporting the events collected within it.
     */
    void reportEvents();

    /**
     * Check if the backing {@link FileSystemAbstraction file system} supports regular file operations or not.
     * <p>
     * E.g. the file system for block device will not work with generic open and read/write calls and all operations
     * needs to be done through the page cache.
     *
     * @return {@code true} if the backing file system supports regular file operations.
     */
    boolean fileSystemSupportsFileOperations();
}
