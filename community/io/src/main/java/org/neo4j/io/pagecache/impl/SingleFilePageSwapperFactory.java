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
    public Stream<FileHandle> streamFilesRecursive( File directory ) throws IOException
    {
        return streamFilesRecursive( directory.getCanonicalFile(), fs );
    }

    @Override
    public void close()
    {
        // We have nothing to close
    }

    /**
     * Static implementation of {@link SingleFilePageSwapperFactory#streamFilesRecursive(File)} that does not require
     * any external state, other than what is presented through the given {@link FileSystemAbstraction}.
     *
     * This method is an implementation detail of {@link PageSwapperFactory page swapper factories}, and it is made
     * available here so that other {@link PageSwapperFactory} implementations can use it as the basis of their own
     * implementations. In other words, so that other {@link PageSwapperFactory} implementations can implement
     * {@link PageSwapperFactory#streamFilesRecursive(File)} by augmenting this stream.
     * @param directory The base directory.
     * @param fs The {@link FileSystemAbstraction} to use for manipulating files.
     * @return A {@link Stream} of {@link FileHandle}s, as per the {@link PageSwapperFactory#streamFilesRecursive(File)}
     * specification.
     * @throws IOException If anything goes wrong, like {@link PageSwapperFactory#streamFilesRecursive(File)} describes.
     */
    public static Stream<FileHandle> streamFilesRecursive( File directory, FileSystemAbstraction fs ) throws IOException
    {
        try
        {
            // We grab a snapshot of the file tree to avoid seeing the same file twice or more due to renames.
            List<File> snapshot = streamFilesRecursiveInner( directory, fs ).collect( toList() );
            return snapshot.stream().map( f -> new WrappingFileHandle( f, directory, fs ) );
        }
        catch ( UncheckedIOException e )
        {
            // We sneak checked IOExceptions through UncheckedIOExceptions due to our use of streams and lambdas.
            throw e.getCause();
        }
    }

    private static Stream<File> streamFilesRecursiveInner( File directory, FileSystemAbstraction fs )
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
                     .flatMap( f -> fs.isDirectory( f ) ? streamFilesRecursiveInner( f, fs ) : Stream.of( f ) );
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

    private static class WrappingFileHandle implements FileHandle
    {
        private final File file;
        private final File baseDirectory;
        private final FileSystemAbstraction fs;

        WrappingFileHandle( File file, File baseDirectory, FileSystemAbstraction fs )
        {
            this.file = file;
            this.baseDirectory = baseDirectory;
            this.fs = fs;
        }

        @Override
        public File getFile()
        {
            return file;
        }

        @Override
        public File getRelativeFile()
        {
            int baseLength = baseDirectory.getPath().length();
            if ( baseDirectory.getParent() != null )
            {
                baseLength++;
            }
            return new File( file.getPath().substring( baseLength ) );
        }

        @Override
        public void rename( File to, CopyOption... options ) throws IOException
        {
            File parentFile = file.getParentFile();
            fs.mkdirs( to.getParentFile() );
            fs.renameFile( file, to, options );
            removeEmptyParent( parentFile );
        }

        private void removeEmptyParent( File parentFile )
        {
            // delete up to and including the base directory, but not above.
            // Note that this may be 'null' if 'baseDirectory' is the top directory.
            // Fortunately, 'File.equals(other)' handles 'null' and returns 'false' when 'other' is 'null'.
            File end = baseDirectory.getParentFile();
            while ( parentFile != null && !parentFile.equals( end ) )
            {
                File[] files = fs.listFiles( parentFile );
                if ( files == null || files.length > 0 )
                {
                    return;
                }
                fs.deleteFile( parentFile );
                parentFile = parentFile.getParentFile();
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
