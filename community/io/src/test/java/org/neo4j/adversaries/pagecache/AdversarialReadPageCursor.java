/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.adversaries.pagecache;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.adversaries.Adversary;
import org.neo4j.io.pagecache.PageCursor;

/**
 * A read {@linkplain PageCursor page cursor} that wraps another page cursor and an {@linkplain Adversary adversary}
 * to provide a misbehaving page cursor implementation for testing.
 * <p>
 * Depending on the adversary each read operation can throw either {@link RuntimeException} like
 * {@link SecurityException} or {@link IOException} like {@link FileNotFoundException}.
 * <p>
 * Depending on the adversary each read operation can produce an inconsistent read and require caller to retry using
 * while loop with {@link PageCursor#shouldRetry()} as a condition.
 * <p>
 * Write operations will always throw an {@link IllegalStateException} because this is a read cursor.
 * See {@link org.neo4j.io.pagecache.PagedFile#PF_SHARED_READ_LOCK} flag.
 */
@SuppressWarnings( "unchecked" )
class AdversarialReadPageCursor implements PageCursor
{
    private final PageCursor delegate;
    private final Adversary adversary;
    private PageCursor linkedCursor;

    private boolean currentReadIsInconsistent;

    AdversarialReadPageCursor( PageCursor delegate, Adversary adversary )
    {
        this.delegate = Objects.requireNonNull( delegate );
        this.adversary = Objects.requireNonNull( adversary );
    }

    @Override
    public byte getByte()
    {
        return currentReadIsInconsistent ? 0 : delegate.getByte();
    }

    @Override
    public byte getByte( int offset )
    {
        return currentReadIsInconsistent ? 0 : delegate.getByte( offset );
    }

    @Override
    public void putByte( byte value )
    {
        throw new IllegalStateException( "Cannot write using read cursor" );
    }

    @Override
    public void putByte( int offset, byte value )
    {
        throw new IllegalStateException( "Cannot write using read cursor" );
    }

    @Override
    public long getLong()
    {
        long result = delegate.getLong();
        return currentReadIsInconsistent ? 0 : result;
    }

    @Override
    public long getLong( int offset )
    {
        long result = delegate.getLong( offset );
        return currentReadIsInconsistent ? 0 : result;
    }

    @Override
    public void putLong( long value )
    {
        throw new IllegalStateException( "Cannot write using read cursor" );
    }

    @Override
    public void putLong( int offset, long value )
    {
        throw new IllegalStateException( "Cannot write using read cursor" );
    }

    @Override
    public int getInt()
    {
        int result = delegate.getInt();
        return currentReadIsInconsistent ? 0 : result;
    }

    @Override
    public int getInt( int offset )
    {
        int result = delegate.getInt( offset );
        return currentReadIsInconsistent ? 0 : result;
    }

    @Override
    public void putInt( int value )
    {
        throw new IllegalStateException( "Cannot write using read cursor" );
    }

    @Override
    public void putInt( int offset, int value )
    {
        throw new IllegalStateException( "Cannot write using read cursor" );
    }

    @Override
    public void getBytes( byte[] data )
    {
        delegate.getBytes( data );
        if ( currentReadIsInconsistent )
        {
            ThreadLocalRandom.current().nextBytes( data );
        }
    }

    @Override
    public void getBytes( byte[] data, int arrayOffset, int length )
    {
        delegate.getBytes( data, arrayOffset, length );
        if ( currentReadIsInconsistent )
        {
            ThreadLocalRandom rng = ThreadLocalRandom.current();
            for ( int i = arrayOffset; i < length; i++ )
            {
                data[i] = (byte) rng.nextInt();
            }
        }
    }

    @Override
    public void putBytes( byte[] data )
    {
        throw new IllegalStateException( "Cannot write using read cursor" );
    }

    @Override
    public void putBytes( byte[] data, int arrayOffset, int length )
    {
        throw new IllegalStateException( "Cannot write using read cursor" );
    }

    @Override
    public short getShort()
    {
        return currentReadIsInconsistent ? 0 : delegate.getShort();
    }

    @Override
    public short getShort( int offset )
    {
        return currentReadIsInconsistent ? 0 : delegate.getShort( offset );
    }

    @Override
    public void putShort( short value )
    {
        throw new IllegalStateException( "Cannot write using read cursor" );
    }

    @Override
    public void putShort( int offset, short value )
    {
        throw new IllegalStateException( "Cannot write using read cursor" );
    }

    @Override
    public void setOffset( int offset )
    {
        adversary.injectFailure( IndexOutOfBoundsException.class );
        delegate.setOffset( offset );
    }

    @Override
    public int getOffset()
    {
        return delegate.getOffset();
    }

    @Override
    public long getCurrentPageId()
    {
        return delegate.getCurrentPageId();
    }

    @Override
    public int getCurrentPageSize()
    {
        return delegate.getCurrentPageSize();
    }

    @Override
    public File getCurrentFile()
    {
        return delegate.getCurrentFile();
    }

    @Override
    public void rewind()
    {
        delegate.rewind();
    }

    @Override
    public boolean next() throws IOException
    {
        currentReadIsInconsistent = adversary.injectFailureOrMischief( FileNotFoundException.class, IOException.class,
                SecurityException.class, IllegalStateException.class );
        return delegate.next();
    }

    @Override
    public boolean next( long pageId ) throws IOException
    {
        currentReadIsInconsistent = adversary.injectFailureOrMischief( FileNotFoundException.class, IOException.class,
                SecurityException.class, IllegalStateException.class );
        return delegate.next( pageId );
    }

    @Override
    public void close()
    {
        delegate.close();
        linkedCursor = null;
    }

    @Override
    public boolean shouldRetry() throws IOException
    {
        adversary.injectFailure( FileNotFoundException.class, IOException.class, SecurityException.class,
                IllegalStateException.class );
        if ( currentReadIsInconsistent )
        {
            currentReadIsInconsistent = false;
            delegate.shouldRetry();
            delegate.setOffset( 0 );
            return true;
        }
        boolean retry = delegate.shouldRetry();
        return retry || (linkedCursor != null && linkedCursor.shouldRetry());
    }

    @Override
    public int copyTo( int sourceOffset, PageCursor targetCursor, int targetOffset, int lengthInBytes )
    {
        adversary.injectFailure( IndexOutOfBoundsException.class );
        if ( !currentReadIsInconsistent )
        {
            delegate.copyTo( sourceOffset, targetCursor, targetOffset, lengthInBytes );
        }
        return lengthInBytes;
    }

    @Override
    public boolean checkAndClearBoundsFlag()
    {
        return delegate.checkAndClearBoundsFlag() || (linkedCursor != null && linkedCursor.checkAndClearBoundsFlag());
    }

    @Override
    public void raiseOutOfBounds()
    {
        delegate.raiseOutOfBounds();
    }

    @Override
    public PageCursor openLinkedCursor( long pageId )
    {
        return linkedCursor = new AdversarialReadPageCursor( delegate.openLinkedCursor( pageId ), adversary );
    }
}
