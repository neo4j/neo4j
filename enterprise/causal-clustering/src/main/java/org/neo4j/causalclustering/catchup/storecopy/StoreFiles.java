/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.FileHandle;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.MetaDataStore;

public class StoreFiles
{
    private static final FilenameFilter STORE_FILE_FILTER = ( dir, name ) ->
    {
        // Skip log files and tx files from temporary database
        return !name.startsWith( "metrics" ) && !name.startsWith( "temp-copy" ) &&
               !name.startsWith( "raft-messages." ) && !name.startsWith( "debug." ) &&
               !name.startsWith( "data" ) && !name.startsWith( "store_lock" );
    };

    private final FilenameFilter fileFilter;
    private FileSystemAbstraction fs;
    private PageCache pageCache;

    public StoreFiles( FileSystemAbstraction fs, PageCache pageCache )
    {
        this( fs, pageCache, STORE_FILE_FILTER );
    }

    public StoreFiles( FileSystemAbstraction fs, PageCache pageCache, FilenameFilter fileFilter )
    {
        this.fs = fs;
        this.pageCache = pageCache;
        this.fileFilter = fileFilter;
    }

    public void delete( File storeDir ) throws IOException
    {
        // 'files' can be null if the directory doesn't exist. This is fine, we just ignore it then.
        File[] files = fs.listFiles( storeDir, fileFilter );
        if ( files != null )
        {
            for ( File file : files )
            {
                fs.deleteRecursively( file );
            }
        }

        Iterable<FileHandle> iterator = acceptedPageCachedFiles( storeDir )::iterator;
        for ( FileHandle fh : iterator )
        {
            fh.delete();
        }
    }

    private Stream<FileHandle> acceptedPageCachedFiles( File storeDir ) throws IOException
    {
        try
        {
            Stream<FileHandle> stream = pageCache.streamFilesRecursive( storeDir );
            Predicate<FileHandle> acceptableFiles = fh -> fileFilter.accept( storeDir, fh.getRelativeFile().getPath() );
            return stream.filter( acceptableFiles );
        }
        catch ( NoSuchFileException e )
        {
            // This is fine. Just ignore empty or non-existing directories.
            return Stream.empty();
        }
    }

    public void moveTo( File source, File target ) throws IOException
    {
        for ( File candidate : fs.listFiles( source, fileFilter ) )
        {
            fs.moveToDirectory( candidate, target );
        }

        Iterable<FileHandle> fileHandles = acceptedPageCachedFiles( source )::iterator;
        for ( FileHandle fh : fileHandles )
        {
            fh.rename( new File( target, fh.getRelativeFile().getPath() ), StandardCopyOption.REPLACE_EXISTING );
        }
    }

    public boolean isEmpty( File storeDir, List<File> filesToLookFor ) throws IOException
    {
        // 'files' can be null if the directory doesn't exist. This is fine, we just ignore it then.
        File[] files = fs.listFiles( storeDir, fileFilter );
        if ( files != null )
        {
            for ( File file : files )
            {
                if ( filesToLookFor.contains( file ) )
                {
                    return false;
                }
            }
        }

        Iterable<FileHandle> fileHandles = acceptedPageCachedFiles( storeDir )::iterator;
        for ( FileHandle fh : fileHandles )
        {
            if ( filesToLookFor.contains( fh.getFile() ) )
            {
                return false;
            }
        }

        return true;
    }

    public StoreId readStoreId( File storeDir ) throws IOException
    {
        File neoStoreFile = new File( storeDir, MetaDataStore.DEFAULT_NAME );
        org.neo4j.kernel.impl.store.StoreId kernelStoreId = MetaDataStore.getStoreId( pageCache, neoStoreFile );
        return new StoreId( kernelStoreId.getCreationTime(), kernelStoreId.getRandomId() );
    }
}
