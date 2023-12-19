/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.com.storecopy;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.List;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.store.StoreType;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.neo4j.kernel.impl.store.format.RecordFormat.NO_RECORD_SIZE;

public class ToFileStoreWriter implements StoreWriter
{
    private final File basePath;
    private final FileSystemAbstraction fs;
    private final StoreCopyClientMonitor monitor;
    private final PageCache pageCache;
    private final List<FileMoveAction> fileMoveActions;

    public ToFileStoreWriter( File graphDbStoreDir, FileSystemAbstraction fs,
            StoreCopyClientMonitor storeCopyClientMonitor, PageCache pageCache, List<FileMoveAction> fileMoveActions )
    {
        this.basePath = graphDbStoreDir;
        this.fs = fs;
        this.monitor = storeCopyClientMonitor;
        this.pageCache = pageCache;
        this.fileMoveActions = fileMoveActions;
    }

    @Override
    public long write( String path, ReadableByteChannel data, ByteBuffer temporaryBuffer, boolean hasData,
            int requiredElementAlignment ) throws IOException
    {
        try
        {
            temporaryBuffer.clear();
            File file = new File( basePath, path );
            file.getParentFile().mkdirs();

            String fullFilePath = file.toString();

            monitor.startReceivingStoreFile( fullFilePath );
            try
            {
                // Note that we don't bother checking if the page cache already has a mapping for the given file.
                // The reason is that we are copying to a temporary store location, and then we'll move the files later.
                if ( !pageCache.fileSystemSupportsFileOperations() && StoreType.canBeManagedByPageCache( file.getName() ) )
                {
                    int filePageSize = filePageSize( requiredElementAlignment );
                    try ( PagedFile pagedFile = pageCache.map( file, filePageSize, CREATE, WRITE ) )
                    {
                        final long written = writeDataThroughPageCache( pagedFile, data, temporaryBuffer, hasData );
                        addPageCacheMoveAction( file );
                        return written;
                    }
                }
                // We don't add file move actions for these files. The reason is that we will perform the file moves
                // *after* we have done recovery on the store, and this may delete some files, and add other files.
                return writeDataThroughFileSystem( file, data, temporaryBuffer, hasData );
            }
            finally
            {
                monitor.finishReceivingStoreFile( fullFilePath );
            }
        }
        catch ( Throwable t )
        {
            throw new IOException( t );
        }
    }

    // As only the page cache know towards which device (block device or normal file system) it is working, we use
    // the page cache later on when we want to move the files written through the page cache.
    private void addPageCacheMoveAction( File file )
    {
        fileMoveActions.add( FileMoveAction.moveViaPageCache( file, pageCache ) );
    }

    private int filePageSize( int alignment )
    {
        // We know we are dealing with a record store at this point, so the required alignment is the record size,
        // and we can use this to do the page size calculation in the same way as the stores would.
        final int pageCacheSize = pageCache.pageSize();
        return (alignment == NO_RECORD_SIZE) ? pageCacheSize
                                              : (pageCacheSize - (pageCacheSize % alignment));
    }

    private long writeDataThroughFileSystem( File file, ReadableByteChannel data, ByteBuffer temporaryBuffer,
            boolean hasData ) throws IOException
    {
        try ( StoreChannel channel = fs.create( file ) )
        {
            return writeData( data, temporaryBuffer, hasData, channel );
        }
    }

    private long writeDataThroughPageCache( PagedFile pagedFile, ReadableByteChannel data, ByteBuffer temporaryBuffer,
            boolean hasData ) throws IOException
    {
        try ( WritableByteChannel channel = pagedFile.openWritableByteChannel() )
        {
            return writeData( data, temporaryBuffer, hasData, channel );
        }
    }

    private long writeData( ReadableByteChannel data, ByteBuffer temporaryBuffer, boolean hasData,
            WritableByteChannel channel ) throws IOException
    {
        long totalToWrite = 0;
        long totalWritten = 0;
        if ( hasData )
        {
            while ( data.read( temporaryBuffer ) >= 0 )
            {
                temporaryBuffer.flip();
                totalToWrite += temporaryBuffer.limit();
                int bytesWritten;
                while ( (totalWritten += bytesWritten = channel.write( temporaryBuffer )) < totalToWrite )
                {
                    if ( bytesWritten < 0 )
                    {
                        throw new IOException( "Unable to write to disk, reported bytes written was " + bytesWritten );
                    }
                }
                temporaryBuffer.clear();
            }
        }
        return totalWritten;
    }

    @Override
    public void close()
    {
        // Do nothing
    }
}
