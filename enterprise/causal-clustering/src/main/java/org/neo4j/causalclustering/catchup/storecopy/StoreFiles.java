/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
import org.neo4j.io.fs.FileHandle;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;

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

    public void delete( File storeDir, LogFiles logFiles ) throws IOException
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

        File[] txLogs = fs.listFiles( logFiles.logFilesDirectory() );
        if ( txLogs != null )
        {
            for ( File txLog : txLogs )
            {
                fs.deleteFile( txLog );
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
            Stream<FileHandle> stream = pageCache.getCachedFileSystem().streamFilesRecursive( storeDir );
            Predicate<FileHandle> acceptableFiles = fh -> fileFilter.accept( storeDir, fh.getRelativeFile().getPath() );
            return stream.filter( acceptableFiles );
        }
        catch ( NoSuchFileException e )
        {
            // This is fine. Just ignore empty or non-existing directories.
            return Stream.empty();
        }
    }

    public void moveTo( File source, File target, LogFiles logFiles ) throws IOException
    {
        fs.mkdirs( logFiles.logFilesDirectory() );
        for ( File candidate : fs.listFiles( source, fileFilter ) )
        {
            File destination = logFiles.isLogFile( candidate) ? logFiles.logFilesDirectory() : target;
            fs.moveToDirectory( candidate, destination );
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
        return new StoreId( kernelStoreId.getCreationTime(), kernelStoreId.getRandomId(),
                kernelStoreId.getUpgradeTime(), kernelStoreId.getUpgradeId() );
    }
}
