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
package org.neo4j.adversaries.pagecache;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Objects;

import org.neo4j.adversaries.Adversary;
import org.neo4j.io.pagecache.PageCursor;

/**
 * A write {@linkplain PageCursor page cursor} that wraps another page cursor and an {@linkplain Adversary adversary}
 * to provide a misbehaving page cursor implementation for testing.
 * <p>
 * Depending on the adversary each read and write operation can throw either {@link RuntimeException} like
 * {@link SecurityException} or {@link IOException} like {@link FileNotFoundException}.
 * <p>
 * Read operations will always return a consistent value because the underlying page is exclusively write locked.
 * See {@link org.neo4j.io.pagecache.PagedFile#PF_EXCLUSIVE_LOCK} flag.
 */
@SuppressWarnings( "unchecked" )
class AdversarialWritePageCursor implements PageCursor
{
    private final PageCursor delegate;
    private final Adversary adversary;

    AdversarialWritePageCursor( PageCursor delegate, Adversary adversary )
    {
        this.delegate = Objects.requireNonNull( delegate );
        this.adversary = Objects.requireNonNull( adversary );
    }

    @Override
    public byte getByte()
    {
        adversary.injectFailure( IndexOutOfBoundsException.class );
        return delegate.getByte();
    }

    @Override
    public byte getByte( int offset )
    {
        adversary.injectFailure( IndexOutOfBoundsException.class );
        return delegate.getByte( offset );
    }

    @Override
    public void putByte( byte value )
    {
        adversary.injectFailure( IndexOutOfBoundsException.class );
        delegate.putByte( value );
    }

    @Override
    public void putByte( int offset, byte value )
    {
        adversary.injectFailure( IndexOutOfBoundsException.class );
        delegate.putByte( offset, value );
    }

    @Override
    public long getLong()
    {
        adversary.injectFailure( IndexOutOfBoundsException.class );
        return delegate.getLong();
    }

    @Override
    public long getLong( int offset )
    {
        adversary.injectFailure( IndexOutOfBoundsException.class );
        return delegate.getLong( offset );
    }

    @Override
    public void putLong( long value )
    {
        adversary.injectFailure( IndexOutOfBoundsException.class );
        delegate.putLong( value );
    }

    @Override
    public void putLong( int offset, long value )
    {
        adversary.injectFailure( IndexOutOfBoundsException.class );
        delegate.putLong( offset, value );
    }

    @Override
    public int getInt()
    {
        adversary.injectFailure( IndexOutOfBoundsException.class );
        return delegate.getInt();
    }

    @Override
    public int getInt( int offset )
    {
        adversary.injectFailure( IndexOutOfBoundsException.class );
        return delegate.getInt( offset );
    }

    @Override
    public void putInt( int value )
    {
        adversary.injectFailure( IndexOutOfBoundsException.class );
        delegate.putInt( value );
    }

    @Override
    public void putInt( int offset, int value )
    {
        adversary.injectFailure( IndexOutOfBoundsException.class );
        delegate.putInt( offset, value );
    }

    @Override
    public long getUnsignedInt()
    {
        adversary.injectFailure( IndexOutOfBoundsException.class );
        return delegate.getUnsignedInt();
    }

    @Override
    public long getUnsignedInt( int offset )
    {
        adversary.injectFailure( IndexOutOfBoundsException.class );
        return delegate.getUnsignedInt( offset );
    }

    @Override
    public void getBytes( byte[] data )
    {
        adversary.injectFailure( IndexOutOfBoundsException.class );
        delegate.getBytes( data );
    }

    @Override
    public void getBytes( byte[] data, int arrayOffset, int length )
    {
        adversary.injectFailure( IndexOutOfBoundsException.class );
        delegate.getBytes( data, arrayOffset, length );
    }

    @Override
    public void putBytes( byte[] data )
    {
        adversary.injectFailure( IndexOutOfBoundsException.class );
        delegate.putBytes( data );
    }

    @Override
    public void putBytes( byte[] data, int arrayOffset, int length )
    {
        adversary.injectFailure( IndexOutOfBoundsException.class );
        delegate.putBytes( data, arrayOffset, length );
    }

    @Override
    public short getShort()
    {
        adversary.injectFailure( IndexOutOfBoundsException.class );
        return delegate.getShort();
    }

    @Override
    public short getShort( int offset )
    {
        adversary.injectFailure( IndexOutOfBoundsException.class );
        return delegate.getShort( offset );
    }

    @Override
    public void putShort( short value )
    {
        adversary.injectFailure( IndexOutOfBoundsException.class );
        delegate.putShort( value );
    }

    @Override
    public void putShort( int offset, short value )
    {
        adversary.injectFailure( IndexOutOfBoundsException.class );
        delegate.putShort( offset, value );
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
        adversary.injectFailure( FileNotFoundException.class, IOException.class, SecurityException.class,
                IllegalStateException.class );
        return delegate.next();
    }

    @Override
    public boolean next( long pageId ) throws IOException
    {
        adversary.injectFailure( FileNotFoundException.class, IOException.class, SecurityException.class,
                IllegalStateException.class );
        return delegate.next( pageId );
    }

    @Override
    public void close()
    {
        delegate.close();
    }

    @Override
    public boolean shouldRetry() throws IOException
    {
        adversary.injectFailure( FileNotFoundException.class, IOException.class, SecurityException.class,
                IllegalStateException.class );
        return delegate.shouldRetry();
    }
}
