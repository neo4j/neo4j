/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.io.fs;

import java.io.Closeable;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.CopyOption;
import java.nio.file.NoSuchFileException;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.zip.ZipOutputStream;

import org.neo4j.io.fs.watcher.FileWatcher;

public interface FileSystemAbstraction extends Closeable
{

    /**
     * Create file watcher that provides possibilities to monitor directories on underlying file system
     * abstraction
     *
     * @return specific file system abstract watcher
     * @throws IOException in case exception occur during file watcher creation
     */
    FileWatcher fileWatcher() throws IOException;

    StoreChannel open( File fileName, String mode ) throws IOException;

    OutputStream openAsOutputStream( File fileName, boolean append ) throws IOException;

    InputStream openAsInputStream( File fileName ) throws IOException;

    Reader openAsReader( File fileName, Charset charset ) throws IOException;

    Writer openAsWriter( File fileName, Charset charset, boolean append ) throws IOException;

    StoreChannel create( File fileName ) throws IOException;

    boolean fileExists( File fileName );

    boolean mkdir( File fileName );

    void mkdirs( File fileName ) throws IOException;

    long getFileSize( File fileName );

    boolean deleteFile( File fileName );

    void deleteRecursively( File directory ) throws IOException;

    void renameFile( File from, File to, CopyOption... copyOptions ) throws IOException;

    File[] listFiles( File directory );

    File[] listFiles( File directory, FilenameFilter filter );

    boolean isDirectory( File file );

    void moveToDirectory( File file, File toDirectory ) throws IOException;

    void copyFile( File from, File to ) throws IOException;

    void copyRecursively( File fromDirectory, File toDirectory ) throws IOException;

    <K extends ThirdPartyFileSystem> K getOrCreateThirdPartyFileSystem( Class<K> clazz, Function<Class<K>,K> creator );

    void truncate( File path, long size ) throws IOException;

    long lastModifiedTime( File file ) throws IOException;

    void deleteFileOrThrow( File file ) throws IOException;

    interface ThirdPartyFileSystem extends Closeable
    {
        void close();

        void dumpToZip( ZipOutputStream zip, byte[] scratchPad ) throws IOException;
    }

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
     *
     * @param directory The base directory to start streaming files from, or the specific individual file to stream.
     * @return A stream of all files in the tree.
     * @throws NoSuchFileException If the given base directory or file does not exists.
     * @throws IOException If an I/O error occurs, possibly with the canonicalisation of the paths.
     */
    Stream<FileHandle> streamFilesRecursive( File directory ) throws IOException;

}
