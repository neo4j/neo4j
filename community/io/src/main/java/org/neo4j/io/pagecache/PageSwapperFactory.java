/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.io.fs.FileSystemAbstraction;

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
     * Configure the FileSystemAbstraction to use.
     *
     * This must be called before the first PageSwapper is created.
     */
    void setFileSystemAbstraction( FileSystemAbstraction fs );

    /**
     * Get the name of this PageSwapperFactory implementation, for configuration purpose.
     */
    String implementationName();

    /**
     * Get the most optimal cache page size (in bytes) for these PageSwapper implementations.
     */
    int getCachePageSizeHint();

    /**
     * Gives <code>true</code> if the {@link #getCachePageSizeHint()} is the only cache page size that is supported for
     * these PageSwapper implementations, otherwise <code>false</code>.
     */
    boolean isCachePageSizeHintStrict();

    /**
     * Get the unit of alignment that the swappers require of the memory buffers. For instance, if page alignment is
     * required for doing direct IO, then {@link org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil#pageSize()} can be
     * returned.
     * @return The required buffer alignment byte multiple.
     */
    long getRequiredBufferAlignment();

    /**
     * Create a PageSwapper for the given file.
     * @param file The file that the PageSwapper will move file pages in and
     *             out of.
     * @param filePageSize The size of the pages in the file. Presumably a
     *                     multiple of some record size.
     * @param onEviction The PageSwapper will be told about evictions, and has
     *                   the responsibility of informing the PagedFile via this callback.
     * @param createIfNotExist When true, creates the given file if it does not exist, instead of throwing an exception.
     * @return A working PageSwapper instance for the given file.
     * @throws IOException If the PageSwapper could not be created, for
     * instance if the underlying file could not be opened, or the given file does not exist and createIfNotExist is
     * false.
     */
    PageSwapper createPageSwapper(
            File file,
            int filePageSize,
            PageEvictionCallback onEviction,
            boolean createIfNotExist ) throws IOException;

    /**
     * Forces all prior writes made through all non-closed PageSwappers that this factory has created, to all the
     * relevant devices, such that the writes are durable when this call returns.
     *
     * This method has no effect if the {@link PageSwapper#force()} method forces the writes for the individual file.
     * The {@link PageCache#flushAndForce()} method will first call <code>force</code> on the PageSwappers for all
     * mapped files, then call <code>syncDevice</code> on the PageSwapperFactory. This way, the writes are always made
     * durable regardless of which method that does the forcing.
     */
    void syncDevice() throws IOException;
}
