/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.kvstore;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.neo4j.helpers.Pair;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;

abstract class RotationStrategy
{
    protected final FileSystemAbstraction fs;
    protected final PageCache pages;
    private final ProgressiveFormat format;
    private final RotationMonitor monitor;

    RotationStrategy( FileSystemAbstraction fs, PageCache pages, ProgressiveFormat format,
                      RotationMonitor monitor )
    {
        this.fs = fs;
        this.pages = pages;
        this.format = format;
        this.monitor = monitor;
    }

    protected abstract File initialFile();

    protected abstract Iterable<File> candidateFiles();

    protected abstract File nextFile( File previous );

    public final Pair<File, KeyValueStoreFile> open() throws IOException
    {
        KeyValueStoreFile result = null;
        File path = null;
        for ( File candidatePath : candidateFiles() )
        {
            KeyValueStoreFile file;
            if ( fs.fileExists( candidatePath ) )
            {
                try
                {
                    file = format.openStore( fs, pages, candidatePath );
                }
                catch ( Exception e )
                {
                    monitor.failedToOpenStoreFile( candidatePath, e );
                    continue;
                }
                if ( result == null || format.compareHeaders( result.headers(), file.headers() ) < 0 )
                {
                    if ( result != null )
                    {
                        result.close();
                    }
                    result = file;
                    path = candidatePath;
                }
                else
                {
                    file.close();
                }
            }
        }
        return result == null ? null : Pair.of( path, result );
    }

    public final Pair<File, KeyValueStoreFile> create( DataProvider initialData, long version ) throws IOException
    {
        File path = initialFile();
        return Pair.of( path, format.createStore(
                fs, pages, path, format.keySize(), format.valueSize(), format.initialHeaders( version ),
                initialData ) );
    }

    public final Pair<File, KeyValueStoreFile> next( File file, Headers headers, DataProvider data )
            throws IOException
    {
        File path = nextFile( file );
        monitor.beforeRotation( file, path, headers );
        KeyValueStoreFile store;
        try
        {
            store = format.createStore( fs, pages, path, format.keySize(), format.valueSize(), headers, data );
        }
        catch ( Exception e )
        {
            monitor.rotationFailed( file, path, headers, e );
            throw e;
        }
        monitor.rotationSucceeded( file, path, headers );
        return Pair.of( path, store );
    }

    final KeyValueStoreFile openStoreFile( File path ) throws IOException
    {
        return format.openStore( fs, pages, path );
    }

    static class LeftRight extends RotationStrategy
    {
        private final File left;
        private final File right;

        LeftRight( FileSystemAbstraction fs, PageCache pages, ProgressiveFormat format,
                   RotationMonitor monitor, File left, File right )
        {
            super( fs, pages, format, monitor );
            this.left = left;
            this.right = right;
        }

        @Override
        protected File initialFile()
        {
            return left;
        }

        @Override
        protected Iterable<File> candidateFiles()
        {
            return Arrays.asList( left, right );
        }

        @Override
        protected File nextFile( File previous )
        {
            if ( left.equals( previous ) )
            {
                return right;
            }
            else if ( right.equals( previous ) )
            {
                return left;
            }
            else
            {
                throw new IllegalStateException( "Invalid path: " + previous );
            }
        }
    }

    static class Incrementing extends RotationStrategy implements FilenameFilter
    {
        private static final Pattern SUFFIX = Pattern.compile( "\\.[0-9]+" );
        private final File base;

        public Incrementing( FileSystemAbstraction fs, PageCache pages, ProgressiveFormat format,
                             RotationMonitor monitor, File base )
        {
            super( fs, pages, format, monitor );
            this.base = base;
        }

        @Override
        protected File initialFile()
        {
            return new File( base.getParent(), base.getName() + ".0" );
        }

        @Override
        protected Iterable<File> candidateFiles()
        {
            return Arrays.asList( fs.listFiles( base.getParentFile(), this ) );
        }

        @Override
        protected File nextFile( File previous )
        {
            String name = previous.getName();
            int pos = name.lastIndexOf( '.' ), next;
            try
            {
                int number = Integer.parseInt( name.substring( pos + 1 ) );
                if ( !base.getParent().equals( previous.getParent() ) ||
                     !base.getName().equals( name.substring( 0, pos ) ) )
                {
                    throw new IllegalStateException( "Invalid path: " + previous );
                }
                next = number + 1;
            }
            catch ( NumberFormatException e )
            {
                throw new IllegalStateException( "Invalid path: " + previous, e );
            }
            return new File( base.getParent(), base.getName() + "." + next );
        }

        @Override
        public boolean accept( File dir, String name )
        {
            return name.startsWith( base.getName() ) &&
                   SUFFIX.matcher( name.substring( base.getName().length() ) ).matches();
        }
    }
}
