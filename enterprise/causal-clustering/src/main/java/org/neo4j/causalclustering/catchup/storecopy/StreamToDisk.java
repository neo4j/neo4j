/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.causalclustering.catchup.tx.FileCopyMonitor;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.PageCacheFlusher;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.monitoring.Monitors;

public class StreamToDisk implements StoreFileStreams
{
    private final File storeDir;
    private final FileSystemAbstraction fs;
    private final PageCache pageCache;
    private final FileCopyMonitor fileCopyMonitor;
    private final PageCacheFlusher flusherThread;
    private final Map<String,PageCacheDestination> channels;
    private boolean closed;

    public StreamToDisk( File storeDir, FileSystemAbstraction fs, PageCache pageCache, Monitors monitors ) throws IOException
    {
        this.storeDir = storeDir;
        this.fs = fs;
        this.pageCache = pageCache;
        this.flusherThread = new PageCacheFlusher( pageCache );
        flusherThread.start();
        fs.mkdirs( storeDir );
        this.fileCopyMonitor = monitors.newMonitor( FileCopyMonitor.class );
        channels = new HashMap<>();
    }

    @Override
    public void write( String destination, int requiredAlignment, byte[] data ) throws IOException
    {
        File fileName = new File( storeDir, destination );
        fs.mkdirs( fileName.getParentFile() );

        fileCopyMonitor.copyFile( fileName );
        if ( StoreType.shouldBeManagedByPageCache( destination ) )
        {
            PageCacheDestination dest = getPageCacheDestination( destination, requiredAlignment, fileName );
            dest.write( data );
        }
        else
        {
            try ( OutputStream outputStream = fs.openAsOutputStream( fileName, true ) )
            {
                outputStream.write( data );
            }
        }
    }

    private synchronized PageCacheDestination getPageCacheDestination(
            String destination, int requiredAlignment, File fileName ) throws IOException
    {
        if ( closed )
        {
            throw new IOException( "Destination has been closed: " + fileName );
        }
        PageCacheDestination dest = channels.get( destination );
        if ( dest == null )
        {
            dest = new PageCacheDestination( pageCache, fileName, requiredAlignment );
            channels.put( destination, dest );
        }
        return dest;
    }

    private static final class PageCacheDestination implements Closeable
    {
        private final PagedFile pagedFile;
        private final WritableByteChannel channel;

        PageCacheDestination( PageCache pageCache, File fileName, int requiredAlignment ) throws IOException
        {
            int filePageSize = pageCache.pageSize() - pageCache.pageSize() % requiredAlignment;
            pagedFile = pageCache.map( fileName, filePageSize, StandardOpenOption.CREATE );
            try
            {
                channel = pagedFile.openWritableByteChannel();
            }
            catch ( IOException channelException )
            {
                try
                {
                    pagedFile.close();
                }
                catch ( IOException pagedFileException )
                {
                    channelException.addSuppressed( pagedFileException );
                }
                channelException.printStackTrace();
                throw channelException;
            }
        }

        public synchronized void write( byte[] data ) throws IOException
        {
            ByteBuffer buf = ByteBuffer.wrap( data );
            while ( buf.hasRemaining() )
            {
                channel.write( buf );
            }
        }

        @Override
        public synchronized void close() throws IOException
        {
            IOUtils.closeAll( channel, pagedFile );
        }
    }

    @Override
    public synchronized void close() throws IOException
    {
        closed = true;
        try
        {
            flusherThread.halt();
        }
        catch ( Exception haltException )
        {
            try
            {
                IOUtils.closeAll( channels.values() );
            }
            catch ( IOException channelsException )
            {
                haltException.addSuppressed( channelsException );
            }
            throw haltException;
        }
        IOUtils.closeAll( channels.values() );
    }
}
