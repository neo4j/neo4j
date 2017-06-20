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
package org.neo4j.impl.store.prototype.neole;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import org.neo4j.impl.store.cursors.PageHandle;
import org.neo4j.impl.store.cursors.ReadCursor;

import static java.lang.Math.max;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static org.neo4j.impl.store.prototype.neole.ReadStore.lcm;
import static org.neo4j.impl.store.prototype.neole.ReadStore.nextPowerOfTwo;

abstract class StoreFile extends PageHandle implements Closeable
{
    static StoreFile fixedSizeRecordFile( File file, int recordSize ) throws IOException
    {
        return new StoreFile( file )
        {
            @Override
            int recordSize()
            {
                return recordSize;
            }
        };
    }

    private final FileChannel channel;
    final long maxReference;
    private final int pageSize;
    private long[] addresses;
    private volatile MappedByteBuffer[] buffers;

    private StoreFile( File file ) throws IOException
    {
        // even though we only use read access we need to request write access
        // in order to be able to let memory mapping extend the file
        this.channel = new RandomAccessFile( file, "rw" ).getChannel();
        maxReference = channel.size() / recordSize();
        pageSize = lcm( recordSize(), 4096 );
    }

    abstract int recordSize();

    long page( int pageId )
    {
        MappedByteBuffer[] buffers = this.buffers;
        if ( buffers == null )
        {
            synchronized ( this )
            {
                if ( (buffers = this.buffers) == null )
                {
                    this.buffers = buffers = new MappedByteBuffer[max( nextPowerOfTwo( pageId + 1 ), 16 )];
                    this.addresses = new long[buffers.length];
                }
            }
        }
        if ( buffers.length <= pageId )
        {
            synchronized ( this )
            {
                buffers = this.buffers;
                if ( buffers.length <= pageId )
                {
                    MappedByteBuffer[] newBuffers = new MappedByteBuffer[nextPowerOfTwo( pageId + 1 )];
                    long[] newAddresses = new long[newBuffers.length];
                    System.arraycopy( buffers, 0, newBuffers, 0, buffers.length );
                    System.arraycopy( addresses, 0, newAddresses, 0, addresses.length );
                    addresses = newAddresses;
                    this.buffers = buffers = newBuffers;
                }
            }
        }
        if ( buffers[pageId] == null )
        {
            synchronized ( this )
            {
                buffers = this.buffers;
                if ( buffers[pageId] == null )
                {
                    MappedByteBuffer buffer = map( pageId );
                    addresses[pageId] = addressOf( buffer );
                    buffers[pageId] = buffer;
                }
            }
        }
        return addresses[pageId];
    }

    private MappedByteBuffer map( int pageId )
    {
        int pageSize = this.pageSize;
        try
        {
            return channel.map( READ_ONLY, pageId * (long) pageSize, pageSize );
        }
        catch ( IOException e )
        {
            throw new IllegalStateException( e );
        }
    }

    private static final MethodHandle BUFFER_ADDRESS;

    static
    {
        MethodHandle handle;
        try
        {
            Field field = Buffer.class.getDeclaredField( "address" );
            field.setAccessible( true );
            handle = MethodHandles.publicLookup().unreflectGetter( field );
        }
        catch ( IllegalAccessException | NoSuchFieldException e )
        {
            throw new RuntimeException( e );
        }
        BUFFER_ADDRESS = handle;
    }

    private static long addressOf( MappedByteBuffer buffer )
    {
        try
        {
            return (long) BUFFER_ADDRESS.invoke( buffer );
        }
        catch ( Throwable e )
        {
            throw new IllegalStateException( e );
        }
    }

    @Override
    public void close() throws IOException
    {
        channel.close();
    }

    @Override
    protected boolean gotoVirtualAddress(
            long virtualAddress,
            ReadCursor cursor,
            long pageId,
            long base,
            int offset,
            long lockToken )
    {
        if ( (virtualAddress & 0xFFFF_FFF8_0000_0000L) != 0 )
        {
            return false;
        }
        long address = virtualAddress * recordSize();
        int page = (int) (address / pageSize);
        int newOffset = (int) (address % pageSize);
        if ( page == pageId )
        {
            move( cursor, virtualAddress, newOffset, recordSize() );
        }
        else
        {
            read( cursor, virtualAddress, this, page, page( page ), newOffset, recordSize() );
        }
        return true;
    }

    @Override
    protected void releasePage( long pageId, long base, int offset, long lockToken )
    {
    }

    @Override
    protected void assertValidOffset( long pageId, long base, int offset, int bound )
    {
    }

    @Override
    protected long sharedLock( long pageId, long base, int offset )
    {
        return 0; // no locks
    }

    @Override
    protected long exclusiveLock( long pageId, long base, int offset )
    {
        return 0; // no locks
    }

    @Override
    protected void releaseLock( long pageId, long base, int offset, long lockToken )
    {
        // no locks
    }

    @Override
    protected long refreshLockToken( long pageId, long base, int offset, long lockToken )
    {
        return 0; // no locks
    }

    @Override
    protected long moveLock( long pageId, long base, int offset, long lockToken, int newOffset )
    {
        return lockToken; // no locks
    }

    public int pageOf( long reference )
    {
        return (int) (reference * recordSize() / pageSize);
    }
}
