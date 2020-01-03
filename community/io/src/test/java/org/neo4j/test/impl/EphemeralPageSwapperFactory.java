/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.test.impl;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.file.NoSuchFileException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

import org.neo4j.graphdb.config.Configuration;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageEvictionCallback;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

public class EphemeralPageSwapperFactory implements PageSwapperFactory, AutoCloseable
{
    private final ConcurrentHashMap<File,EphemeralSwapper> swappers = new ConcurrentHashMap<>();

    @Override
    public void open( FileSystemAbstraction fs, Configuration config )
    {
    }

    @Override
    public String implementationName()
    {
        return "ephemeral";
    }

    @Override
    public long getRequiredBufferAlignment()
    {
        return 1;
    }

    @Override
    public PageSwapper createPageSwapper( File file, int filePageSize, PageEvictionCallback onEviction, boolean createIfNotExist, boolean noChannelStriping )
            throws IOException
    {
        EphemeralSwapper swapper = swappers.compute( file, ( f, existing ) ->
        {
            if ( existing != null )
            {
                // Reuse existing swappers in order to preserve data across mapping and unmapping.
                existing.onEviction = onEviction;
                existing.closed = false;
                return existing;
            }
            if ( createIfNotExist )
            {
                return new EphemeralSwapper( file, filePageSize, onEviction, swappers );
            }
            return null;
        } );
        if ( swapper == null )
        {
            throw new NoSuchFileException( file.getAbsolutePath() );
        }
        return swapper;
    }

    @Override
    public void syncDevice()
    {
    }

    @Override
    public void close()
    {
        swappers.forEachValue( 1, EphemeralSwapper::free );
    }

    private static class EphemeralSwapper implements PageSwapper
    {
        private final ConcurrentHashMap<Long,Long> buffers = new ConcurrentHashMap<>();
        private final File file;
        private final int filePageSize;
        private final ConcurrentHashMap<File,EphemeralSwapper> swappers;
        private final AtomicLong lastPageId;
        private final Function<Long,Long> allocateBuffer;
        private final Consumer<Long> freeMemory;
        private volatile PageEvictionCallback onEviction;
        private volatile boolean closed;

        private EphemeralSwapper( File file, int filePageSize, PageEvictionCallback onEviction, ConcurrentHashMap<File,EphemeralSwapper> swappers )
        {
            this.file = file;
            this.filePageSize = filePageSize;
            this.onEviction = onEviction;
            this.swappers = swappers;
            lastPageId = new AtomicLong( -1 );
            allocateBuffer = fpi -> UnsafeUtil.allocateMemory( filePageSize );
            freeMemory = addr -> UnsafeUtil.free( addr, filePageSize );
        }

        @Override
        public long read( long filePageId, long bufferAddress, int bufferSize ) throws IOException
        {
            assertIO( filePageId );
            Long addr = buffers.get( filePageId );
            int read = 0;
            if ( addr != null )
            {
                long ptr = addr;
                UnsafeUtil.copyMemory( ptr, bufferAddress, bufferSize );
                read = bufferSize;
            }
            if ( read < bufferSize )
            {
                UnsafeUtil.setMemory( bufferAddress + read, bufferSize - read, (byte) 0 );
            }
            return read;
        }

        @Override
        public long read( long startFilePageId, long[] bufferAddresses, int bufferSize, int arrayOffset, int length ) throws IOException
        {
            long data = 0;
            for ( int i = 0; i < length; i++ )
            {
                data += read( startFilePageId + i, bufferAddresses[arrayOffset + i], bufferSize );
            }
            return data;
        }

        @Override
        public long write( long filePageId, long bufferAddress ) throws IOException
        {
            assertIO( filePageId );
            long currMax;
            do
            {
                currMax = lastPageId.get();
            }
            while ( currMax < filePageId && !lastPageId.compareAndSet( currMax, filePageId ) );
            long addr = buffers.computeIfAbsent( filePageId, allocateBuffer );
            UnsafeUtil.copyMemory( bufferAddress, addr, filePageSize );
            return filePageSize;
        }

        private void assertIO( long filePageId ) throws IOException
        {
            if ( closed )
            {
                throw new ClosedChannelException();
            }
            if ( filePageId < 0 )
            {
                throw new IOException( "Negative page id: " + filePageId );
            }
        }

        @Override
        public long write( long startFilePageId, long[] bufferAddresses, int arrayOffset, int length ) throws IOException
        {
            long data = 0;
            for ( int i = 0; i < length; i++ )
            {
                data += write( startFilePageId + i, bufferAddresses[arrayOffset + i] );
            }
            return data;
        }

        @Override
        public void evicted( long pageId )
        {
            if ( !closed )
            {
                onEviction.onEvict( pageId );
            }
        }

        @Override
        public File file()
        {
            return file;
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public void closeAndDelete() throws IOException
        {
            truncate();
            close();
            swappers.remove( file );
        }

        void free()
        {
            buffers.forEachValue( 1000, freeMemory );
            buffers.clear();
        }

        @Override
        public void force() throws IOException
        {
            assertIO( 0 );
        }

        @Override
        public long getLastPageId()
        {
            return lastPageId.get();
        }

        @Override
        public void truncate() throws IOException
        {
            free();
            lastPageId.set( -1 );
        }
    }
}
