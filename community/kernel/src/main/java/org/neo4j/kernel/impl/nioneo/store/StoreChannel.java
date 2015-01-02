/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.nioneo.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.InterruptibleChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.channels.SeekableByteChannel;

public interface StoreChannel
        extends SeekableByteChannel, GatheringByteChannel, ScatteringByteChannel, InterruptibleChannel
{
    /**
     * Attempts to acquire an exclusive lock on this channel's file.
     * @return A lock object representing the newly-acquired lock, or null if the lock could not be acquired.
     * @throws IOException If an I/O error occurs.
     * @throws java.nio.channels.ClosedChannelException if the channel is closed.
     */
    FileLock tryLock() throws IOException;

    int write( ByteBuffer src, long position ) throws IOException;

    MappedByteBuffer map( FileChannel.MapMode mode, long position, long size ) throws IOException;

    int read( ByteBuffer dst, long position ) throws IOException;

    void force( boolean metaData ) throws IOException;

    StoreChannel position( long newPosition ) throws IOException;

    StoreChannel truncate( long size ) throws IOException;
}
