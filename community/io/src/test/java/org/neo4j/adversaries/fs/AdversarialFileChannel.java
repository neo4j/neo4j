/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import sun.nio.ch.FileChannelImpl;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import org.neo4j.adversaries.Adversary;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.fs.StoreFileChannel;
import org.neo4j.io.fs.StoreFileChannelUnwrapper;

import static org.neo4j.adversaries.fs.AdversarialFileDispatcherFactory.makeFileDispatcherAdversarial;

@SuppressWarnings( "unchecked" )
public class AdversarialFileChannel implements StoreChannel
{
    public static volatile boolean useAdversarialFileDispatcherHack;

    private final StoreChannel delegate;
    private final Adversary adversary;

    public static StoreChannel wrap( StoreChannel channel, Adversary adversary )
    {
        if ( useAdversarialFileDispatcherHack && channel.getClass() == StoreFileChannel.class )
        {
            FileChannel innerChannel = StoreFileChannelUnwrapper.unwrap( channel );
            if ( innerChannel.getClass() == FileChannelImpl.class )
            {
                FileChannelImpl channelImpl = (FileChannelImpl) innerChannel;
                try
                {
                    Field nd = FileChannelImpl.class.getDeclaredField( "nd" );
                    nd.setAccessible( true );
                    Object fileDispatcher = nd.get( channelImpl );
                    nd.set( channelImpl, makeFileDispatcherAdversarial( fileDispatcher, adversary ) );
                    return new StoreFileChannel( innerChannel );
                }
                catch ( Exception e )
                {
                    e.printStackTrace();
                }
            }
        }
        return new AdversarialFileChannel( channel, adversary );
    }

    private AdversarialFileChannel( StoreChannel channel, Adversary adversary )
    {
        this.delegate = channel;
        this.adversary = adversary;
    }

    @Override
    public long write( ByteBuffer[] srcs ) throws IOException
    {
        if ( adversary.injectFailureOrMischief( IOException.class ) )
        {
            ByteBuffer mischievousBuffer = srcs[srcs.length - 1];
            int oldLimit = mischiefLimit( mischievousBuffer );
            long written = delegate.write( srcs );
            mischievousBuffer.limit( oldLimit );
            return written;
        }
        return delegate.write( srcs );
    }

    @Override
    public int write( ByteBuffer src, long position ) throws IOException
    {
        if ( adversary.injectFailureOrMischief( IOException.class ) )
        {
            int oldLimit = mischiefLimit( src );
            int written = delegate.write( src, position );
            src.limit( oldLimit );
            return written;
        }
        return delegate.write( src, position );
    }

    @Override
    public void writeAll( ByteBuffer src, long position ) throws IOException
    {
        adversary.injectFailure( IOException.class );
        delegate.writeAll( src, position );
    }

    @Override
    public void writeAll( ByteBuffer src ) throws IOException
    {
        adversary.injectFailure( IOException.class );
        delegate.writeAll( src );
    }

    @Override
    public long write( ByteBuffer[] srcs, int offset, int length ) throws IOException
    {
        if ( adversary.injectFailureOrMischief( IOException.class ) )
        {
            length = length == 1? 1 : length / 2;
            ByteBuffer mischievousBuffer = srcs[offset + length - 1];
            int oldLimit = mischiefLimit( mischievousBuffer );
            long written = delegate.write( srcs, offset, length );
            mischievousBuffer.limit( oldLimit );
            return written;
        }
        return delegate.write( srcs, offset, length );
    }

    @Override
    public StoreChannel truncate( long size ) throws IOException
    {
        adversary.injectFailure( IOException.class );
        return delegate.truncate( size );
    }

    @Override
    public StoreChannel position( long newPosition ) throws IOException
    {
        adversary.injectFailure( IOException.class );
        return delegate.position( newPosition );
    }

    @Override
    public int read( ByteBuffer dst, long position ) throws IOException
    {
        if ( adversary.injectFailureOrMischief( IOException.class ) )
        {
            int oldLimit = mischiefLimit( dst );
            int read = delegate.read( dst, position );
            dst.limit( oldLimit );
            return read;
        }
        return delegate.read( dst, position );
    }

    private int mischiefLimit( ByteBuffer buf )
    {
        int oldLimit = buf.limit();
        int newLimit = oldLimit - buf.remaining() / 2;
        buf.limit( newLimit );
        return oldLimit;
    }

    @Override
    public void force( boolean metaData ) throws IOException
    {
        adversary.injectFailure( IOException.class );
        delegate.force( metaData );
    }

    @Override
    public int read( ByteBuffer dst ) throws IOException
    {
        if ( adversary.injectFailureOrMischief( IOException.class ) )
        {
            int oldLimit = mischiefLimit( dst );
            int read = delegate.read( dst );
            dst.limit( oldLimit );
            return read;
        }
        return delegate.read( dst );
    }

    @Override
    public long read( ByteBuffer[] dsts, int offset, int length ) throws IOException
    {
        if ( adversary.injectFailureOrMischief( IOException.class ) )
        {
            ByteBuffer lastBuf = dsts[dsts.length - 1];
            int oldLimit = mischiefLimit( lastBuf );
            long read = delegate.read( dsts, offset, length );
            lastBuf.limit( oldLimit );
            return read;
        }
        return delegate.read( dsts, offset, length );
    }

    @Override
    public long position() throws IOException
    {
        adversary.injectFailure( IOException.class );
        return delegate.position();
    }

    @Override
    public FileLock tryLock() throws IOException
    {
        adversary.injectFailure( IOException.class );
        return delegate.tryLock();
    }

    @Override
    public boolean isOpen()
    {
        adversary.injectFailure();
        return delegate.isOpen();
    }

    @Override
    public long read( ByteBuffer[] dsts ) throws IOException
    {
        if ( adversary.injectFailureOrMischief( IOException.class ) )
        {
            ByteBuffer lastBuf = dsts[dsts.length - 1];
            int oldLimit = mischiefLimit( lastBuf );
            long read = delegate.read( dsts );
            lastBuf.limit( oldLimit );
            return read;
        }
        return delegate.read( dsts );
    }

    @Override
    public int write( ByteBuffer src ) throws IOException
    {
        if ( adversary.injectFailureOrMischief( IOException.class ) )
        {
            int oldLimit = mischiefLimit( src );
            int written = delegate.write( src );
            src.limit( oldLimit );
            return written;
        }
        return delegate.write( src );
    }

    @Override
    public void close() throws IOException
    {
        adversary.injectFailure( IOException.class );
        delegate.close();
    }

    @Override
    public long size() throws IOException
    {
        adversary.injectFailure( IOException.class );
        return delegate.size();
    }

    @Override
    public void flush() throws IOException
    {
        force( false );
    }
}
