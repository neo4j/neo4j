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
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.causalclustering.catchup.tx.FileCopyMonitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.monitoring.Monitors;

public class StreamToDisk implements StoreFileStreams
{
    private final File storeDir;
    private final FileSystemAbstraction fs;
    private final PageCache pageCache;
    private final FileCopyMonitor fileCopyMonitor;
    private final Map<String,WritableByteChannel> channels;
    private final Map<String,PagedFile> pagedFiles;

    public StreamToDisk( File storeDir, FileSystemAbstraction fs, PageCache pageCache, Monitors monitors ) throws IOException
    {
        this.storeDir = storeDir;
        this.fs = fs;
        this.pageCache = pageCache;
        fs.mkdirs( storeDir );
        this.fileCopyMonitor = monitors.newMonitor( FileCopyMonitor.class );
        channels = new HashMap<>();
        pagedFiles = new HashMap<>();

    }

    @Override
    public void write( String destination, int requiredAlignment, byte[] data ) throws IOException
    {
        File fileName = new File( storeDir, destination );
        fs.mkdirs( fileName.getParentFile() );

        fileCopyMonitor.copyFile( fileName );
        if ( StoreType.shouldBeManagedByPageCache( destination ) )
        {
            WritableByteChannel channel = channels.get( destination );
            if ( channel == null )
            {
                int filePageSize = pageCache.pageSize() - pageCache.pageSize() % requiredAlignment;
                PagedFile pagedFile = pageCache.map( fileName, filePageSize, StandardOpenOption.CREATE );
                channel = pagedFile.openWritableByteChannel();
                pagedFiles.put( destination, pagedFile );
                channels.put( destination, channel );
            }

            ByteBuffer buffer = ByteBuffer.wrap( data );
            while ( buffer.hasRemaining() )
            {
                channel.write( buffer );
            }
        }
        else
        {
            try ( OutputStream outputStream = fs.openAsOutputStream( fileName, true ) )
            {
                outputStream.write( data );
            }
        }
    }

    @Override
    public void close() throws IOException
    {
        for ( WritableByteChannel channel : channels.values() )
        {
            channel.close();
        }
        for ( PagedFile pagedFile : pagedFiles.values() )
        {
            pagedFile.close();
        }
    }
}
