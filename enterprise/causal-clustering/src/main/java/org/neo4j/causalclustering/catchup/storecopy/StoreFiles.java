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
import java.nio.file.StandardCopyOption;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.FileHandle;
import org.neo4j.io.pagecache.PageCache;

public class StoreFiles
{
    private static final FilenameFilter STORE_FILE_FILTER = ( dir, name ) -> {
        // Skip log files and tx files from temporary database
        return !name.startsWith( "metrics" ) && !name.startsWith( "temp-copy" ) &&
               !name.startsWith( "raft-messages." ) && !name.startsWith( "debug." ) &&
               !name.startsWith( "cluster-state" ) && !name.startsWith( "store_lock" );
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
        for ( File file : fs.listFiles( storeDir, fileFilter ) )
        {
            fs.deleteRecursively( file );
        }
        Iterable<FileHandle> iterator = acceptedPageCachedFiles( storeDir )::iterator;
        for ( FileHandle fh : iterator )
        {
            fh.delete();
        }
    }

    private Stream<FileHandle> acceptedPageCachedFiles( File storeDir ) throws IOException
    {
        Predicate<FileHandle> acceptableFiles =
                fh -> fileFilter.accept( storeDir, fh.getRelativeFile().getPath() );
        return pageCache.streamFilesRecursive( storeDir ).filter( acceptableFiles );
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
}
