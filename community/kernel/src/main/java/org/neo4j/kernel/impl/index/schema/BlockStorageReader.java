/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.index.schema;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.transaction.log.ReadAheadChannel;

public class BlockStorageReader<KEY,VALUE> implements Closeable
{
    private final StoreChannel channel;
    private final long blockSize;
    private final FileSystemAbstraction fs;
    private final File file;
    private final Layout<KEY,VALUE> layout;

    BlockStorageReader( FileSystemAbstraction fs, File file, long blockSize, Layout<KEY,VALUE> layout ) throws IOException
    {
        this.fs = fs;
        this.file = file;
        this.layout = layout;
        this.channel = fs.open( file, OpenMode.READ );
        this.blockSize = blockSize;
    }

    BlockReader<KEY,VALUE> nextBlock() throws IOException
    {
        long position = channel.position();
        if ( position >= channel.size() )
        {
            return null;
        }
        StoreChannel blockChannel = fs.open( file, OpenMode.READ );
        blockChannel.position( position );
        channel.position( position + blockSize );
        PageCursor pageCursor = new ReadableChannelPageCursor( new ReadAheadChannel<>( blockChannel ) );
        return new BlockReader<>( pageCursor, layout );
    }

    @Override
    public void close() throws IOException
    {
        channel.close();
    }
}
