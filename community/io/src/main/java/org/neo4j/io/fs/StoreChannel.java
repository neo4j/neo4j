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
package org.neo4j.io.fs;

import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.InterruptibleChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.channels.SeekableByteChannel;

public interface StoreChannel
        extends Flushable, SeekableByteChannel, GatheringByteChannel, ScatteringByteChannel, InterruptibleChannel
{
    /**
     * Attempts to acquire an exclusive lock on this channel's file.
     * @return A lock object representing the newly-acquired lock, or null if the lock could not be acquired.
     * @throws IOException If an I/O error occurs.
     * @throws java.nio.channels.ClosedChannelException if the channel is closed.
     */
    FileLock tryLock() throws IOException;

    /**
     * Same as #write(), except this method will write the full contents of the buffer in chunks if the OS fails to
     * write it all at once.
     */
    void writeAll( ByteBuffer src, long position ) throws IOException;

    /**
     * Same as #write(), except this method will write the full contents of the buffer in chunks if the OS fails to
     * write it all at once.
     */
    void writeAll( ByteBuffer src ) throws IOException;

    /**
     * @see java.nio.channels.FileChannel#read(java.nio.ByteBuffer, long)
     */
    int read( ByteBuffer dst, long position ) throws IOException;

    /**
     * Try to Read a sequence of bytes from channel into the given buffer, till the buffer will be full.
     * In case if end of channel will be reached during reading {@link IllegalStateException} will be thrown.
     *
     * @param dst destination buffer.
     * @throws IOException if an I/O exception occurs.
     * @throws IllegalStateException if end of stream reached during reading.
     * @see ReadableByteChannel#read(ByteBuffer)
     */
    void readAll( ByteBuffer dst ) throws IOException;

    void force( boolean metaData ) throws IOException;

    @Override
    StoreChannel position( long newPosition ) throws IOException;

    @Override
    StoreChannel truncate( long size ) throws IOException;
}
