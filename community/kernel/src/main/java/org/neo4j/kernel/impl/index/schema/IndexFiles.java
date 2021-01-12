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
package org.neo4j.kernel.impl.index.schema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;

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
    public abstract Path getStoreFile();

    public abstract Path getBase();

    public abstract void clear();

    public abstract void archiveIndex();

    public abstract void ensureDirectoryExist();

    @Override
    public String toString()
    {
        return String.format( "%s[base=%s,storeFile=%s]", getClass().getSimpleName(), getBase(), getStoreFile() );
    }

    public static class Directory extends IndexFiles
    {
        private final FileSystemAbstraction fs;
        private final Path directory;
        private final Path storeFile;

        public Directory( FileSystemAbstraction fs, IndexDirectoryStructure directoryStructure, long indexId )
        {
            this.fs = fs;
            this.directory = directoryStructure.directoryForIndex( indexId );
            this.storeFile = directory.resolve( indexFileName( indexId ) );
        }

        @Override
        public Path getStoreFile()
        {
            return storeFile;
        }

        @Override
        public Path getBase()
        {
            return directory;
        }

        @Override
        public void clear()
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

        @Override
        public void archiveIndex()
        {
            if ( fs.isDirectory( directory ) &&
                 fs.fileExists( directory ) && fs.listFiles( directory ).length > 0 )
            {
                try
                {
                    ZipUtils.zip( fs, directory,
                            directory.getParent().resolve( "archive-" + directory.getFileName() + "-" + System.currentTimeMillis() + ".zip" ) );
                }
                catch ( IOException e )
                {
                    throw new UncheckedIOException( e );
                }
            }
        }

        @Override
        public void ensureDirectoryExist()
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

        private static String indexFileName( long indexId )
        {
            return "index-" + indexId;
        }
    }

    static class SingleFile extends IndexFiles
    {
        private final FileSystemAbstraction fs;
        private final Path singleFile;

        SingleFile( FileSystemAbstraction fs, Path singleFile )
        {
            this.fs = fs;
            this.singleFile = singleFile;
        }

        @Override
        public Path getStoreFile()
        {
            return singleFile;
        }

        @Override
        public Path getBase()
        {
            return singleFile;
        }

        @Override
        public void clear()
        {
            try
            {
                if ( fs.fileExists( singleFile ) )
                {
                    fs.deleteFileOrThrow( singleFile );
                }
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        }

        @Override
        public void archiveIndex()
        {
            if ( fs.fileExists( singleFile ) )
            {
                try
                {
                    ZipUtils.zip( fs, singleFile,
                            singleFile.resolve( "archive-" + singleFile.getFileName() + "-" + System.currentTimeMillis() + ".zip" ) );
                }
                catch ( IOException e )
                {
                    throw new UncheckedIOException( e );
                }
            }
        }

        @Override
        public void ensureDirectoryExist()
        {
            // no-op
        }
    }
}
