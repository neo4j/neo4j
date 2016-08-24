/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.com.storecopy;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Optional;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.store.StoreType;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.neo4j.kernel.impl.store.StoreType.isStoreType;

public class ToFileStoreWriter implements StoreWriter
{
    private final File basePath;
    private final StoreCopyClient.Monitor monitor;
    private final PageCache pageCache;

    public ToFileStoreWriter( File graphDbStoreDir, StoreCopyClient.Monitor monitor, PageCache pageCache )
    {
        this.basePath = graphDbStoreDir;
        this.monitor = monitor;
        this.pageCache = pageCache;
    }

    @Override
    public long write( String path, ReadableByteChannel data, ByteBuffer temporaryBuffer,
            boolean hasData ) throws IOException
    {
        try
        {
            temporaryBuffer.clear();
            File file = new File( basePath, path );

            String filename = file.getName();
            final Optional<StoreType> storeType = isStoreType( filename );
            final Optional<PagedFile> existingMapping = pageCache.getExistingMapping( file );

            monitor.startReceivingStoreFile( file );
            try
            {
                if ( existingMapping.isPresent() )
                {
                    try ( PagedFile pagedFile = existingMapping.get() )
                    {
                        return writeDataThroughPageCache( pagedFile, data, temporaryBuffer, hasData );
                    }
                }
                if ( storeType.isPresent() && storeType.get().isRecordStore() )
                {
                    try ( PagedFile pagedFile = pageCache.map( file, pageCache.pageSize(), CREATE, WRITE ) )
                    {
                        return writeDataThroughPageCache( pagedFile, data, temporaryBuffer, hasData );
                    }
                }
                else
                {
                    file.getParentFile().mkdirs();
                    return writeDataThroughFileSystem( file, data, temporaryBuffer, hasData );
                }
            }
            finally
            {
                monitor.finishReceivingStoreFile( file );
            }
        }
        catch ( Throwable t )
        {
            throw new IOException( t );
        }
    }

    private long writeDataThroughFileSystem( File file, ReadableByteChannel data, ByteBuffer temporaryBuffer,
            boolean hasData ) throws IOException
    {
        try ( RandomAccessFile randomAccessFile = new RandomAccessFile( file, "rw" ) )
        {
            return writeData( data, temporaryBuffer, hasData, randomAccessFile.getChannel() );
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
                while ( (totalWritten += (bytesWritten = channel.write( temporaryBuffer ))) < totalToWrite )
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
