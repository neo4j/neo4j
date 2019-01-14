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
package org.neo4j.kernel.impl.transaction.log;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;

import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.LOG_HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader.readLogHeader;

public class PhysicalLogVersionedStoreChannel implements LogVersionedStoreChannel
{
    private final StoreChannel delegateChannel;
    private final long version;
    private final byte formatVersion;
    private long position;

    public PhysicalLogVersionedStoreChannel( StoreChannel delegateChannel, long version, byte formatVersion )
            throws IOException
    {
        this.delegateChannel = delegateChannel;
        this.version = version;
        this.formatVersion = formatVersion;
        this.position = delegateChannel.position();
    }

    @Override
    public FileLock tryLock() throws IOException
    {
        return delegateChannel.tryLock();
    }

    @Override
    public void writeAll( ByteBuffer src, long position )
    {
        throw new UnsupportedOperationException( "Not needed" );
    }

    @Override
    public void writeAll( ByteBuffer src ) throws IOException
    {
        advance( src.remaining() );
        delegateChannel.writeAll( src );
    }

    @Override
    public int read( ByteBuffer dst, long position )
    {
        throw new UnsupportedOperationException( "Not needed" );
    }

    @Override
    public void readAll( ByteBuffer dst )
    {
        throw new UnsupportedOperationException( "Not needed" );
    }

    @Override
    public void force( boolean metaData ) throws IOException
    {
        delegateChannel.force( metaData );
    }

    @Override
    public StoreChannel position( long newPosition ) throws IOException
    {
        this.position = newPosition;
        return delegateChannel.position( newPosition );
    }

    @Override
    public StoreChannel truncate( long size ) throws IOException
    {
        return delegateChannel.truncate( size );
    }

    @Override
    public int read( ByteBuffer dst ) throws IOException
    {
        return (int) advance( delegateChannel.read( dst ) );
    }

    private long advance( long bytes )
    {
        if ( bytes != -1 )
        {
            position += bytes;
        }
        return bytes;
    }

    @Override
    public int write( ByteBuffer src ) throws IOException
    {
        return (int) advance( delegateChannel.write( src ) );
    }

    @Override
    public long position()
    {
        return position;
    }

    @Override
    public long size() throws IOException
    {
        return delegateChannel.size();
    }

    @Override
    public boolean isOpen()
    {
        return delegateChannel.isOpen();
    }

    @Override
    public void close() throws IOException
    {
        delegateChannel.close();
    }

    @Override
    public long write( ByteBuffer[] srcs, int offset, int length ) throws IOException
    {
        return advance( delegateChannel.write( srcs, offset, length ) );
    }

    @Override
    public long write( ByteBuffer[] srcs ) throws IOException
    {
        return advance( delegateChannel.write( srcs ) );
    }

    @Override
    public long read( ByteBuffer[] dsts, int offset, int length ) throws IOException
    {
        return advance( delegateChannel.read( dsts, offset, length ) );
    }

    @Override
    public long read( ByteBuffer[] dsts ) throws IOException
    {
        return advance( delegateChannel.read( dsts ) );
    }

    @Override
    public long getVersion()
    {
        return version;
    }

    @Override
    public byte getLogFormatVersion()
    {
        return formatVersion;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        PhysicalLogVersionedStoreChannel that = (PhysicalLogVersionedStoreChannel) o;

        return version == that.version && delegateChannel.equals( that.delegateChannel );
    }

    @Override
    public int hashCode()
    {
        int result = delegateChannel.hashCode();
        result = 31 * result + (int) (version ^ (version >>> 32));
        return result;
    }

    @Override
    public void flush() throws IOException
    {
        force( false );
    }
}
