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
import java.nio.file.NoSuchFileException;
import java.util.stream.Stream;

import org.neo4j.io.fs.FileSystemAbstraction;

/**
 * Creates PageSwappers for the given files.
 * <p>
 * A PageSwapper is responsible for swapping file pages in and out of memory.
 * <p>
 * The PageSwapperFactory presumably knows about what file system to use.
 * <p>
 * Note that this API is <em>only</em> intended to be used by a {@link PageCache} implementation.
 * It should never be used directly by user code.
 */
public interface PageSwapperFactory
{
    /**
     * Configure the FileSystemAbstraction to use.
     * <p>
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
     *
     * @return The required buffer alignment byte multiple.
     */
    long getRequiredBufferAlignment();

    /**
     * Create a PageSwapper for the given file.
     *
     * @param file The file that the PageSwapper will move file pages in and
     * out of.
     * @param filePageSize The size of the pages in the file. Presumably a
     * multiple of some record size.
     * @param onEviction The PageSwapper will be told about evictions, and has
     * the responsibility of informing the PagedFile via this callback.
     * @param createIfNotExist When true, creates the given file if it does not exist, instead of throwing an
     * exception.
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
     * <p>
     * This method has no effect if the {@link PageSwapper#force()} method forces the writes for the individual file.
     * The {@link PageCache#flushAndForce()} method will first call <code>force</code> on the PageSwappers for all
     * mapped files, then call <code>syncDevice</code> on the PageSwapperFactory. This way, the writes are always made
     * durable regardless of which method that does the forcing.
     */
    void syncDevice() throws IOException;

    /**
     * Return a stream of {@link FileHandle file handles} for every file in the given directory, and its
     * sub-directories.
     * <p>
     * Alternatively, if the {@link File} given as an argument refers to a file instead of a directory, then a stream
     * will be returned with a file handle for just that file.
     * <p>
     * The stream is based on a snapshot of the file tree, so changes made to the tree using the returned file handles
     * will not be reflected in the stream.
     * <p>
     * No directories will be returned. Only files. If a file handle ends up leaving a directory empty through a
     * rename or a delete, then the empty directory will automatically be deleted as well.
     * Likewise, if a file is moved to a path where not all of the directories in the path exists, then those missing
     * directories will be created prior to the file rename.
     * <p>
     * This method form the basis of the implementation of the {@link PageCache#streamFilesRecursive(File)} method.
     * The key difference is that the stream and file handles are oblivious about which files are mapped or not.
     * Thus, the returned {@link FileHandle file handles} will never throw any
     * {@link org.neo4j.io.pagecache.impl.FileIsMappedException}s.
     *
     * @param directory The base directory to start streaming files from, or the specific individual file to stream.
     * @return A stream of all files in the tree.
     * @throws NoSuchFileException If the given base directory or file does not exists.
     * @throws IOException If an I/O error occurs, possibly with the canonicalisation of the paths.
     */
    Stream<FileHandle> streamFilesRecursive( File directory ) throws IOException;

    /**
     * Close and release any resources associated with this PageSwapperFactory, that it may have opened or acquired
     * during its construction or use.
     * <p>
     * This method cannot be called before all of the opened {@link PageSwapper PageSwappers} have been closed,
     * and it is guaranteed that no new page swappers will be created after this method has been called.
     */
    void close();
}
