/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.io.fs.watcher.FileWatcher;

public interface FileSystemAbstraction extends Closeable
{
    int INVALID_FILE_DESCRIPTOR = -1;
    CopyOption[] EMPTY_COPY_OPTIONS = new CopyOption[0];

    /**
     * Create file watcher that provides possibilities to monitor directories on underlying file system
     * abstraction
     *
     * @return specific file system abstract watcher
     * @throws IOException in case exception occur during file watcher creation
     */
    FileWatcher fileWatcher() throws IOException;

    StoreChannel open( Path fileName, Set<OpenOption> options ) throws IOException;

    OutputStream openAsOutputStream( Path fileName, boolean append ) throws IOException;

    InputStream openAsInputStream( Path fileName ) throws IOException;

    Reader openAsReader( Path fileName, Charset charset ) throws IOException;

    Writer openAsWriter( Path fileName, Charset charset, boolean append ) throws IOException;

    /**
     * Open channel for user provided file in a write mode.
     * Write mode means that channel will be opened with following set of options: {@link StandardOpenOption#READ}, {@link StandardOpenOption#WRITE}
     * and {@link StandardOpenOption#CREATE}
     * @param fileName file name to open write channel for.
     * @return write channel for requested file
     * @throws IOException
     */
    StoreChannel write( Path fileName ) throws IOException;

    /**
     * Open channel for user provided file in a read mode.
     * Read mode means that channel will be opened with {@link StandardOpenOption#READ} only.
     * @param fileName file name to open readchannel for.
     * @return read channel for requested file
     * @throws IOException
     */
    StoreChannel read( Path fileName ) throws IOException;

    boolean fileExists( Path file );

    boolean mkdir( Path fileName );

    void mkdirs( Path fileName ) throws IOException;

    long getFileSize( Path fileName );

    long getBlockSize( Path file ) throws IOException;

    default void delete( Path path ) throws IOException
    {
        if ( isDirectory( path ) )
        {
            deleteRecursively( path );
        }
        else
        {
            deleteFile( path );
        }
    }

    boolean deleteFile( Path fileName );

    void deleteRecursively( Path directory ) throws IOException;

    void renameFile( Path from, Path to, CopyOption... copyOptions ) throws IOException;

    Path[] listFiles( Path directory );

    Path[] listFiles( Path directory, DirectoryStream.Filter<Path> filter );

    boolean isDirectory( Path file );

    void moveToDirectory( Path file, Path toDirectory ) throws IOException;

    void copyToDirectory( Path file, Path toDirectory ) throws IOException;

    /**
     * Copies file, allowing overwrite.
     *
     * @param from File to copy.
     * @param to File location to copy file to, overwritten if exists.
     * @throws IOException on I/O error.
     */
    default void copyFile( Path from, Path to ) throws IOException
    {
        copyFile( from, to, StandardCopyOption.REPLACE_EXISTING );
    }

    /**
     * Copies file, configurable behaviour by passing copy options explicitly.
     *
     * @param from File to copy.
     * @param to File location to copy file to.
     * @throws IOException on I/O error.
     */
    void copyFile( Path from, Path to, CopyOption... copyOptions ) throws IOException;

    void copyRecursively( Path fromDirectory, Path toDirectory ) throws IOException;

    void truncate( Path file, long size ) throws IOException;

    long lastModifiedTime( Path file );

    void deleteFileOrThrow( Path file ) throws IOException;

    /**
     * Return a stream of {@link FileHandle file handles} for every file in the given directory, and its
     * sub-directories.
     * <p>
     * Alternatively, if the {@link Path} given as an argument refers to a file instead of a directory, then a stream
     * will be returned with a file handle for just that file.
     * <p>
     * The stream is based on a snapshot of the file tree, so changes made to the tree using the returned file handles
     * will not be reflected in the stream.
     * <p>
     * No directories will be returned. Only files. If a file handle ends up leaving a directory empty through a
     * rename or a delete, then the empty directory will automatically be deleted as well.
     * Likewise, if a file is moved to a path where not all of the directories in the path exists, then those missing
     * directories will be created prior to the file rename.
     *
     * @param directory The base directory to start streaming files from, or the specific individual file to stream.
     * @return A stream of all files in the tree.
     * @throws NoSuchFileException If the given base directory or file does not exists.
     * @throws IOException If an I/O error occurs, possibly with the canonicalisation of the paths.
     */
    Stream<FileHandle> streamFilesRecursive( Path directory ) throws IOException;

    /**
     * Get underlying store channel file descriptor.
     * @param channel channel to get descriptor from
     * @return {@link #INVALID_FILE_DESCRIPTOR} when can't get descriptor from provided channel or underlying channel id otherwise.
     */
    int getFileDescriptor( StoreChannel channel );
}
