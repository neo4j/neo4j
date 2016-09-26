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
package org.neo4j.io.pagecache.impl;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.CopyOption;
import java.nio.file.NoSuchFileException;
import java.util.List;
import java.util.stream.Stream;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.FileHandle;
import org.neo4j.io.pagecache.PageEvictionCallback;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.PageSwapperFactory;

import static java.util.stream.Collectors.toList;

/**
 * A factory for SingleFilePageSwapper instances.
 *
 * @see org.neo4j.io.pagecache.impl.SingleFilePageSwapper
 */
public class SingleFilePageSwapperFactory implements PageSwapperFactory
{
    private FileSystemAbstraction fs;

    @Override
    public void setFileSystemAbstraction( FileSystemAbstraction fs )
    {
        this.fs = fs;
    }

    @Override
    public PageSwapper createPageSwapper(
            File file,
            int filePageSize,
            PageEvictionCallback onEviction,
            boolean createIfNotExist ) throws IOException
    {
        if ( !fs.fileExists( file ) )
        {
            if ( createIfNotExist )
            {
                fs.create( file ).close();
            }
            else
            {
                throw new NoSuchFileException( file.getPath(), null, "Cannot map non-existing file" );
            }
        }
        return new SingleFilePageSwapper( file, fs, filePageSize, onEviction );
    }

    @Override
    public void syncDevice()
    {
        // Nothing do to, since we `fsync` files individually in `force()`.
    }

    @Override
    public void renameUnopenedFile( File sourceFile, File targetFile, CopyOption... copyOptions ) throws IOException
    {
        fs.renameFile( sourceFile, targetFile, copyOptions );
    }

    @Override
    public Stream<FileHandle> streamFilesRecursive( File directory ) throws IOException
    {
        try
        {
            // We grab a snapshot of the file tree to avoid seeing the same file twice or more due to renames.
            List<File> snapshot = streamFilesRecursiveInner( directory ).collect( toList() );
            return snapshot.stream().map( f -> new WrappingFileHandle( f, fs ) );
        }
        catch ( UncheckedIOException e )
        {
            // We sneak checked IOExceptions through UncheckedIOExceptions due to our use of streams and lambdas.
            throw e.getCause();
        }
    }

    private Stream<File> streamFilesRecursiveInner( File directory )
    {
        File[] files = fs.listFiles( directory );
        if ( files == null )
        {
            if ( !fs.fileExists( directory ) )
            {
                throw new UncheckedIOException( new NoSuchFileException( directory.getPath() ) );
            }
            return Stream.of( directory );
        }
        return Stream.of( files )
                     .flatMap( f -> fs.isDirectory( f )? streamFilesRecursiveInner( f ) : Stream.of( f ) );
    }

    @Override
    public String implementationName()
    {
        return "single";
    }

    @Override
    public int getCachePageSizeHint()
    {
        return 8192;
    }

    @Override
    public boolean isCachePageSizeHintStrict()
    {
        return false;
    }

    @Override
    public long getRequiredBufferAlignment()
    {
        return 1;
    }

    private class WrappingFileHandle implements FileHandle
    {
        private final File file;
        private final FileSystemAbstraction fs;

        public WrappingFileHandle( File file, FileSystemAbstraction fs )
        {
            this.file = file;
            this.fs = fs;
        }

        @Override
        public String getAbsolutePath()
        {
            return file.getAbsolutePath();
        }

        @Override
        public File getFile()
        {
            return file;
        }

        @Override
        public void renameFile( File to, CopyOption... options ) throws IOException
        {
            File parentFile = file.getParentFile();
            fs.mkdirs( to.getParentFile() );
            fs.renameFile( file, to, options );
            removeEmptyParent( parentFile );
        }

        private void removeEmptyParent( File parentFile )
        {
            File[] files = fs.listFiles( parentFile );
            if ( files != null && files.length == 0 )
            {
                fs.deleteFile( parentFile );
            }
        }

        @Override
        public void delete() throws IOException
        {
            File parentFile = file.getParentFile();
            fs.deleteFileOrThrow( file );
            removeEmptyParent( parentFile );
        }
    }
}
