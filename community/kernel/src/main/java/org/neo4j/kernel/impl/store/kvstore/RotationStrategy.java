/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

abstract class RotationStrategy<Meta>
{
    protected final FileSystemAbstraction fs;
    protected final PageCache pages;
    private final ProgressiveFormat<Meta> format;

    RotationStrategy( FileSystemAbstraction fs, PageCache pages, ProgressiveFormat<Meta> format )
    {
        this.fs = fs;
        this.pages = pages;
        this.format = format;
    }

    protected abstract File initialFile();

    protected abstract Iterable<File> candidateFiles();

    protected abstract File nextFile( File previous );

    public final Pair<File, KeyValueStoreFile<Meta>> open() throws IOException
    {
        KeyValueStoreFile<Meta> result = null;
        File path = null;
        for ( File candidatePath : candidateFiles() )
        {
            KeyValueStoreFile<Meta> file;
            if ( fs.fileExists( candidatePath ) )
            {
                try
                {
                    file = format.openStore( fs, pages, candidatePath );
                }
                catch ( Exception e )
                {
                    format.failedToOpenStoreFile( candidatePath, e );
                    continue;
                }
                if ( result == null || format.compareMetadata( result.metadata(), file.metadata() ) < 0 )
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

    public final Pair<File, KeyValueStoreFile<Meta>> create()
            throws IOException
    {
        File path = initialFile();
        format.createEmptyStore( fs, path, format.keySize(), format.valueSize(), format.initialMetadata() );
        return Pair.of( path, format.openStore( fs, pages, path ) );
    }

    public final Pair<File, KeyValueStoreFile<Meta>> next( File file, Meta meta, DataProvider data )
            throws IOException
    {
        File path = nextFile( file );
        format.beforeRotation( file, path, meta );
        KeyValueStoreFile<Meta> store;
        try
        {
            store = format.createStore( fs, pages, path, format.keySize(), format.valueSize(), meta, data );
        }
        catch ( Exception e )
        {
            format.rotationFailed( file, path, meta, e );
            throw e;
        }
        format.rotationSucceeded( file, path, meta );
        return Pair.of( path, store );
    }

    final KeyValueStoreFile<Meta> openStoreFile( File path ) throws IOException
    {
        return format.openStore( fs, pages, path );
    }

    static class LeftRight<Meta> extends RotationStrategy<Meta>
    {
        private final File left;
        private final File right;

        LeftRight( FileSystemAbstraction fs, PageCache pages, ProgressiveFormat<Meta> format, File left, File right )
        {
            super( fs, pages, format );
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

    static class Incrementing<Meta> extends RotationStrategy<Meta> implements FilenameFilter
    {
        private static final Pattern SUFFIX = Pattern.compile( "\\.[0-9]+" );
        private final File base;

        public Incrementing( FileSystemAbstraction fs, PageCache pages, ProgressiveFormat<Meta> format, File base )
        {
            super( fs, pages, format );
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
