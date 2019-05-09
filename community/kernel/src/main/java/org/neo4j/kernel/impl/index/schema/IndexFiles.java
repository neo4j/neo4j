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
package org.neo4j.kernel.impl.index.schema;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.NoSuchFileException;

import org.neo4j.io.compress.ZipUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;

/**
 * Surface for a schema indexes to act on the files that it owns.
 * One instance of this class maps to a single index or sub-index if living under a Fusion umbrella.
 * Wraps all {@link IOException IOExceptions} in {@link UncheckedIOException}.
 */
public abstract class IndexFiles
{
    /**
     * @return The single {@link File} where the online index should live.
     */
    abstract File getStoreFile();

    /**
     * @return The base directory or file belonging to this index.
     */
    abstract File getBase();

    /**
     * Delete all files belonging to this index.
     */
    public abstract void clear();

    /**
     * Create an archive file in parent directory containing this index.
     */
    public abstract void archiveIndex();

    /**
     * Make sure that parent directory to {@link #getStoreFile() store file} exists.
     */
    public abstract void ensureDirectoryExist();

    @Override
    public String toString()
    {
        return String.format( "%s[base=%s,storeFile=%s]", getClass().getSimpleName(), getBase(), getStoreFile() );
    }

    /**
     * Recursively delete directory and wrap any {@link IOException} in {@link UncheckedIOException}.
     */
    static void clearDirectory( FileSystemAbstraction fs, File directory )
    {
        try
        {
            fs.deleteRecursively( directory );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    /**
     * Delete file and wrap any {@link IOException} in {@link UncheckedIOException}.
     */
    static void clearSingleFile( FileSystemAbstraction fs, File file )
    {
        try
        {
            fs.deleteFileOrThrow( file );
        }
        catch ( NoSuchFileException e )
        {
            // File does not exist, we don't need to delete
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    /**
     * Create an archive file for directory using {@link ZipUtils#zip(FileSystemAbstraction, File, File)}.
     * Will only create the archive if directory exist and is not empty.
     * Wrap any {@link IOException} in {@link UncheckedIOException}.
     */
    static void archiveIndex( FileSystemAbstraction fs, File directory )
    {
        if ( fs.isDirectory( directory ) && fs.fileExists( directory ) && fs.listFiles( directory ).length > 0 )
        {
            try
            {
                ZipUtils.zip( fs, directory, new File( directory.getParent(), "archive-" + directory.getName() + "-" + System.currentTimeMillis() + ".zip" ) );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        }
    }

    /**
     * Create directory and any non existing parent directories and wrap any {@link IOException} in {@link UncheckedIOException}.
     */
    static void ensureDirectoryExists( FileSystemAbstraction fs, File directory )
    {
        try
        {
            fs.mkdirs( directory );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    /**
     * This index own a whole directory.
     */
    public static class Directory extends IndexFiles
    {
        private final FileSystemAbstraction fs;
        private final File directory;
        private final File storeFile;

        public Directory( FileSystemAbstraction fs, IndexDirectoryStructure directoryStructure, long indexId )
        {
            this.fs = fs;
            this.directory = directoryStructure.directoryForIndex( indexId );
            this.storeFile = new File( directory, indexFileName( indexId ) );
        }

        @Override
        public File getStoreFile()
        {
            return storeFile;
        }

        @Override
        public File getBase()
        {
            return directory;
        }

        @Override
        public void clear()
        {
            clearDirectory( fs, directory );
        }

        @Override
        public void archiveIndex()
        {
            archiveIndex( fs, getBase() );
        }

        @Override
        public void ensureDirectoryExist()
        {
            ensureDirectoryExists( fs, directory );
        }

        private static String indexFileName( long indexId )
        {
            return "index-" + indexId;
        }
    }

    /**
     * This index own only a single file.
     * Typically a Spatial or Temporal part index.
     */
    static class SingleFile extends IndexFiles
    {
        private final FileSystemAbstraction fs;
        private final File singleFile;

        SingleFile( FileSystemAbstraction fs, File singleFile )
        {
            this.fs = fs;
            this.singleFile = singleFile;
        }

        @Override
        public File getStoreFile()
        {
            return singleFile;
        }

        @Override
        public File getBase()
        {
            return singleFile;
        }

        @Override
        public void clear()
        {
            clearSingleFile( fs, singleFile );
        }

        @Override
        public void archiveIndex()
        {
            archiveIndex( fs, getBase() );
        }

        @Override
        public void ensureDirectoryExist()
        {
            ensureDirectoryExists( fs, singleFile.getParentFile() );
        }
    }
}
