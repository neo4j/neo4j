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
package org.neo4j.io.fs;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.CopyOption;
import java.util.function.Consumer;

/**
 * A handle to a file as seen by the page cache. The file may or may not be mapped.
 * @see FileSystemAbstraction#streamFilesRecursive(File)
 */
public interface FileHandle
{
    /**
     * Useful consumer when doing deletion in stream pipeline.
     * <p>
     * Possible IOException caused by fileHandle.delete() is wrapped in UncheckedIOException
     */
    Consumer<FileHandle> HANDLE_DELETE = fh ->
    {
        try
        {
            fh.delete();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    };

    /**
     * Create a consumer of FileHandle that uses fileHandle.rename to move file held by file handle to move from
     * directory to directory.
     * <p>
     * Possibly IOException will be wrapped in UncheckedIOException
     *
     * @param from Directory to move file from.
     * @param to Directory to move file to.
     * @return A new Consumer that moves the file wrapped by the file handle.
     */
    static Consumer<FileHandle> handleRenameBetweenDirectories( File from, File to )
    {
        return fileHandle ->
        {
            try
            {
                fileHandle.rename( FileUtils.pathToFileAfterMove( from, to, fileHandle.getFile() ) );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        };
    }

    static Consumer<FileHandle> handleRename( File to )
    {
        return fileHandle ->
        {
            try
            {
                fileHandle.rename( to );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        };
    }

    /**
     * Get a {@link File} object for the abstract path name that this file handle represents.
     *
     * Note that the File is not guaranteed to support any operations other than querying the path name.
     * For instance, to delete the file you have to use the {@link #delete()} method of this file handle, instead of
     * {@link File#delete()}.
     * @return A {@link File} for this file handle.
     */
    File getFile();

    /**
     * Get a {@link File} object for the abstract path name that this file handle represents, and that is
     * <em>relative</em> to the base path that was passed into the
     * {@link FileSystemAbstraction#streamFilesRecursive(File)} method.
     * <p>
     * This method is otherwise behaviourally the same as {@link #getFile()}.
     *
     * @return A {@link File} for this file handle.
     */
    File getRelativeFile();

    /**
     * Rename source file to the given target file, effectively moving the file from source to target.
     *
     * Both files have to be unmapped when performing the rename, otherwise an exception will be thrown.
     *
     * If the file is moved to a path where some of the directories of the path don't already exists, then those missing
     * directories will be created prior to the move. This is not an atomic operation, and an exception may be thrown if
     * the a directory in the path is deleted concurrently with the file rename operation.
     *
     * Likewise, if the file rename causes a directory to become empty, then those directories will be deleted
     * automatically. This operation is also not atomic, so if files are added to such directories concurrently with
     * the rename operation, then an exception can be thrown.
     *
     * @param to The new name of the file after the rename.
     * @param options Options to modify the behaviour of the move in possibly platform specific ways. In particular,
     * {@link java.nio.file.StandardCopyOption#REPLACE_EXISTING} may be used to overwrite any existing file at the
     * target path name, instead of throwing an exception.
     * @throws org.neo4j.io.pagecache.impl.FileIsMappedException if either the file represented by this file handle is
     * mapped, or the target file is mapped.
     * @throws java.nio.file.FileAlreadyExistsException if the target file already exists, and the
     * {@link java.nio.file.StandardCopyOption#REPLACE_EXISTING} open option was not specified.
     * @throws IOException if an I/O error occurs, for instance when canonicalising the {@code to} path.
     */
    void rename( File to, CopyOption... options ) throws IOException;

    /**
     * Delete the file that this file handle represents.
     *
     * @throws org.neo4j.io.pagecache.impl.FileIsMappedException if this file is mapped by the page cache.
     * @throws java.nio.file.NoSuchFileException if the underlying file was deleted after this handle was created.
     * @throws IOException if an I/O error occurs.
     */
    void delete() throws IOException;
}
