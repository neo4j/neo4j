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
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class StreamFilesRecursive
{
    private StreamFilesRecursive()
    {
        //This is a helper class, do not instantiate it.
    }

    /**
     * Static implementation of {@link FileSystemAbstraction#streamFilesRecursive(File)} that does not require
     * any external state, other than what is presented through the given {@link FileSystemAbstraction}.
     *
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
     * @param fs The {@link FileSystemAbstraction} to use for manipulating files.
     * @return A {@link Stream} of {@link FileHandle}s
     * @throws IOException If an I/O error occurs, possibly with the canonicalisation of the paths.
     */
    public static Stream<FileHandle> streamFilesRecursive( File directory, FileSystemAbstraction fs ) throws IOException
    {
        File canonicalizedDirectory = directory.getCanonicalFile();
        // We grab a snapshot of the file tree to avoid seeing the same file twice or more due to renames.
        List<File> snapshot = streamFilesRecursiveInner( canonicalizedDirectory, fs ).collect( toList() );
        return snapshot.stream().map( f -> new WrappingFileHandle( f, canonicalizedDirectory, fs ) );
    }

    private static Stream<File> streamFilesRecursiveInner( File directory, FileSystemAbstraction fs )
    {
        File[] files = fs.listFiles( directory );
        if ( files == null )
        {
            if ( !fs.fileExists( directory ) )
            {
                return Stream.empty();
            }
            return Stream.of( directory );
        }
        else
        {
            return Stream.of( files )
                    .flatMap( f -> fs.isDirectory( f ) ? streamFilesRecursiveInner( f, fs ) : Stream.of( f ) );
        }
    }
}
