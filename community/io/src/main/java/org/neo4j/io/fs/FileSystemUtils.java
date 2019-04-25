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

import org.apache.commons.lang3.ArrayUtils;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

/**
 * This class consists exclusively of static methods that operate on files, directories, or other types of files.
 * Every method takes {@link FileSystemAbstraction} as a parameter to be able to work across all different file systems.
 *
 * @see FileUtils
 */
public class FileSystemUtils
{
    private FileSystemUtils()
    {
    }

    /**
     * Creates a file, or opens an existing file. If necessary, parent directories will be created.
     *
     * @param fs The filesystem abstraction to use.
     * @param file The file to create or open.
     * @return An output stream.
     * @throws IOException If an error occurs creating directories or opening the file.
     */
    public static OutputStream createOrOpenAsOutputStream( FileSystemAbstraction fs, File file, boolean append ) throws IOException
    {
        if ( file.getParentFile() != null )
        {
            fs.mkdirs( file.getParentFile() );
        }
        return fs.openAsOutputStream( file, append );
    }

    /**
     * Check if directory is empty.
     *
     * @param fs file system to use.
     * @param directory directory to check.
     * @return {@code true} when directory does not exist or exists and is empty, {@code false} otherwise.
     */
    public static boolean isEmptyOrNonExistingDirectory( FileSystemAbstraction fs, File directory )
    {
        if ( fs.isDirectory( directory ) )
        {
            File[] files = fs.listFiles( directory );
            return ArrayUtils.isEmpty( files );
        }
        return !fs.fileExists( directory );
    }

    /**
     * Calculates the size of a given directory or file given the provided abstract filesystem.
     *
     * @param fs the filesystem abstraction to use.
     * @param file to the file or directory.
     * @return the size, in bytes, of the file or the total size of the content in the directory, including
     * subdirectories.
     */
    public static long size( FileSystemAbstraction fs, File file )
    {
        if ( fs.isDirectory( file ) )
        {
            long size = 0L;
            File[] files = fs.listFiles( file );
            if ( files == null )
            {
                return 0L;
            }
            for ( File child : files )
            {
                size += size( fs, child );
            }
            return size;
        }
        else
        {
            return fs.getFileSize( file );
        }
    }

    public static void deleteFile( FileSystemAbstraction fs, File fileToDelete ) throws IOException
    {
        if ( fileToDelete.isDirectory() )
        {
            fs.deleteRecursively( fileToDelete );
        }
        else
        {
            fs.deleteFile( fileToDelete );
        }
    }
}
