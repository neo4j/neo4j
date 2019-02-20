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

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;

public abstract class IndexFiles
{
    abstract File getStoreFile();

    abstract File getBase();

    public abstract void clear();

    @Override
    public String toString()
    {
        return String.format( "%s[base=%s,storeFile=%s]", getClass().getSimpleName(), getBase(), getStoreFile() );
    }

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

        private String indexFileName( long indexId )
        {
            return "index-" + indexId;
        }
    }

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
    }
}
