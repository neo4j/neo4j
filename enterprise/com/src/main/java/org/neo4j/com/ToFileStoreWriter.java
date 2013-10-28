/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.com;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;

public class ToFileStoreWriter implements StoreWriter
{
    private final File basePath;

    public ToFileStoreWriter( File graphDbStoreDir )
    {
        this.basePath = graphDbStoreDir;
    }

    public void write( String path, ReadableByteChannel data, ByteBuffer temporaryBuffer,
            boolean hasData ) throws IOException
    {
        try
        {
            temporaryBuffer.clear();
            File file = new File( basePath, path );

            RandomAccessFile randomAccessFile = null;
            try
            {
                file.getParentFile().mkdirs();
                randomAccessFile = new RandomAccessFile( file, "rw" );
                if ( hasData )
                {
                    FileChannel channel = randomAccessFile.getChannel();
                    while ( data.read( temporaryBuffer ) >= 0 )
                    {
                        temporaryBuffer.flip();
                        channel.write( temporaryBuffer );
                        temporaryBuffer.clear();
                    }
                }
            }
            finally
            {
                if ( randomAccessFile != null )
                {
                    randomAccessFile.close();
                }
            }
        }
        catch ( Throwable t )
        {
            throw new IOException( t );
        }
    }

    public void done()
    {
        // Do nothing
    }
}
