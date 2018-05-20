/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.com.storecopy;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;

public class ToFileStoreWriter implements StoreWriter
{
    private final File basePath;
    private final FileSystemAbstraction fs;
    private final StoreCopyClientMonitor monitor;

    public ToFileStoreWriter( File graphDbStoreDir, FileSystemAbstraction fs,
            StoreCopyClientMonitor storeCopyClientMonitor )
    {
        this.basePath = graphDbStoreDir;
        this.fs = fs;
        this.monitor = storeCopyClientMonitor;
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

    private long writeDataThroughFileSystem( File file, ReadableByteChannel data, ByteBuffer temporaryBuffer,
            boolean hasData ) throws IOException
    {
        try ( StoreChannel channel = fs.create( file ) )
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
