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
package org.neo4j.adversaries.fs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import org.neo4j.adversaries.Adversary;
import org.neo4j.kernel.impl.nioneo.store.StoreFileChannel;

@SuppressWarnings( "unchecked" )
public class AdversarialFileChannel extends StoreFileChannel
{
    private final Adversary adversary;

    public AdversarialFileChannel( StoreFileChannel channel, Adversary adversary )
    {
        super( channel );
        this.adversary = adversary;
    }

    @Override
    public long write( ByteBuffer[] srcs ) throws IOException
    {
        adversary.injectFailure( IOException.class );
        return super.write( srcs );
    }

    @Override
    public int write( ByteBuffer src, long position ) throws IOException
    {
        adversary.injectFailure( IOException.class );
        return super.write( src, position );
    }

    @Override
    public MappedByteBuffer map( FileChannel.MapMode mode, long position, long size ) throws IOException
    {
        adversary.injectFailure( IOException.class );
        return super.map( mode, position, size );
    }

    @Override
    public long write( ByteBuffer[] srcs, int offset, int length ) throws IOException
    {
        adversary.injectFailure( IOException.class );
        return super.write( srcs, offset, length );
    }

    @Override
    public StoreFileChannel truncate( long size ) throws IOException
    {
        adversary.injectFailure( IOException.class );
        return super.truncate( size );
    }

    @Override
    public StoreFileChannel position( long newPosition ) throws IOException
    {
        adversary.injectFailure( IOException.class );
        return super.position( newPosition );
    }

    @Override
    public int read( ByteBuffer dst, long position ) throws IOException
    {
        if ( adversary.injectFailureOrMischief( IOException.class ) )
        {
            int oldLimit = mischiefLimit( dst );
            int read = super.read( dst, position );
            dst.limit( oldLimit );
            return read;
        }
        return super.read( dst, position );
    }

    private int mischiefLimit( ByteBuffer buf )
    {
        int oldLimit = buf.limit();
        int newLimit = oldLimit - Math.max( buf.remaining() / 2, 1 );
        buf.limit( newLimit );
        return oldLimit;
    }

    @Override
    public void force( boolean metaData ) throws IOException
    {
        adversary.injectFailure( IOException.class );
        super.force( metaData );
    }

    @Override
    public int read( ByteBuffer dst ) throws IOException
    {
        if ( adversary.injectFailureOrMischief( IOException.class ) )
        {
            int oldLimit = mischiefLimit( dst );
            int read = super.read( dst );
            dst.limit( oldLimit );
            return read;
        }
        return super.read( dst );
    }

    @Override
    public long read( ByteBuffer[] dsts, int offset, int length ) throws IOException
    {
        if ( adversary.injectFailureOrMischief( IOException.class ) )
        {
            ByteBuffer lastBuf = dsts[dsts.length - 1];
            int oldLimit = mischiefLimit( lastBuf );
            long read = super.read( dsts, offset, length );
            lastBuf.limit( oldLimit );
            return read;
        }
        return super.read( dsts, offset, length );
    }

    @Override
    public long position() throws IOException
    {
        adversary.injectFailure( IOException.class );
        return super.position();
    }

    @Override
    public FileLock tryLock() throws IOException
    {
        adversary.injectFailure( IOException.class );
        return super.tryLock();
    }

    @Override
    public boolean isOpen()
    {
        adversary.injectFailure();
        return super.isOpen();
    }

    @Override
    public long read( ByteBuffer[] dsts ) throws IOException
    {
        if ( adversary.injectFailureOrMischief( IOException.class ) )
        {
            ByteBuffer lastBuf = dsts[dsts.length - 1];
            int oldLimit = mischiefLimit( lastBuf );
            long read = super.read( dsts );
            lastBuf.limit( oldLimit );
            return read;
        }
        return super.read( dsts );
    }

    @Override
    public int write( ByteBuffer src ) throws IOException
    {
        adversary.injectFailure( IOException.class );
        return super.write( src );
    }

    @Override
    public void close() throws IOException
    {
        adversary.injectFailure( IOException.class );
        super.close();
    }

    @Override
    public long size() throws IOException
    {
        adversary.injectFailure( IOException.class );
        return super.size();
    }
}
