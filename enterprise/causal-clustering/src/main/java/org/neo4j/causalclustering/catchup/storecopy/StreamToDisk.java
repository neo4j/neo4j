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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.pagecache.PagedFile;

import static org.neo4j.io.IOUtils.closeAll;

public class StreamToDisk implements StoreFileStream
{
    private WritableByteChannel writableByteChannel;
    private List<AutoCloseable> closeables;

    static StreamToDisk fromPagedFile( PagedFile pagedFile ) throws IOException
    {
        return new StreamToDisk( pagedFile.openWritableByteChannel(), pagedFile );
    }

    static StreamToDisk fromFile( FileSystemAbstraction fsa, File file ) throws IOException
    {
        return new StreamToDisk( fsa.open( file, OpenMode.READ_WRITE ) );
    }

    private StreamToDisk( WritableByteChannel writableByteChannel, AutoCloseable... closeables )
    {
        this.writableByteChannel = writableByteChannel;
        this.closeables = new ArrayList<>();
        this.closeables.add( writableByteChannel );
        this.closeables.addAll( Arrays.asList( closeables ) );
    }

    @Override
    public void write( byte[] data ) throws IOException
    {
        ByteBuffer buffer = ByteBuffer.wrap( data );
        while ( buffer.hasRemaining() )
        {
            writableByteChannel.write( buffer );
        }
    }

    @Override
    public void close() throws IOException
    {
        closeAll( closeables );
    }
}
