/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.cache;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

import static org.neo4j.io.pagecache.PagedFile.PF_NO_GROW;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_WRITE_LOCK;

public class PageCacheByteArray extends PageCacheNumberArray<ByteArray> implements ByteArray
{
    private final byte[] defaultValue;

    PageCacheByteArray( PagedFile pagedFile, long length, byte[] defaultValue, long base ) throws IOException
    {
        // Default value is handled locally in this class, in contrast to its siblings, which lets the superclass
        // handle it.
        super( pagedFile, defaultValue.length, length, base );
        this.defaultValue = defaultValue;
        setDefaultValue( -1 );
    }

    @Override
    protected void fillPageWithDefaultValue( PageCursor writeCursor, long ignoredDefaultValue, int pageSize )
    {
        for ( int i = 0; i < entriesPerPage; i++ )
        {
            writeCursor.putBytes( this.defaultValue );
        }
    }

    @Override
    public void swap( long fromIndex, long toIndex )
    {
        byte[] a = defaultValue.clone();
        byte[] b = defaultValue.clone();
        get( fromIndex, a );
        get( toIndex, b );
        set( fromIndex, b );
        set( toIndex, a );
    }

    @Override
    public void get( long index, byte[] into )
    {
        long pageId = pageId( index );
        int offset = offset( index );
        try ( PageCursor cursor = pagedFile.io( pageId, PF_SHARED_READ_LOCK ) )
        {
            cursor.next();
            do
            {
                for ( int i = 0; i < into.length; i++ )
                {
                    into[i] = cursor.getByte( offset + i );
                }
            }
            while ( cursor.shouldRetry() );
            checkBounds( cursor );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public byte getByte( long index, int offset )
    {
        long pageId = pageId( index );
        offset += offset( index );
        try ( PageCursor cursor = pagedFile.io( pageId, PF_SHARED_READ_LOCK ) )
        {
            cursor.next();
            byte result;
            do
            {
                result = cursor.getByte( offset );
            }
            while ( cursor.shouldRetry() );
            checkBounds( cursor );
            return result;
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public short getShort( long index, int offset )
    {
        long pageId = pageId( index );
        offset += offset( index );
        try ( PageCursor cursor = pagedFile.io( pageId, PF_SHARED_READ_LOCK ) )
        {
            cursor.next();
            short result;
            do
            {
                result = cursor.getShort( offset );
            }
            while ( cursor.shouldRetry() );
            checkBounds( cursor );
            return result;
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public int getInt( long index, int offset )
    {
        long pageId = pageId( index );
        offset += offset( index );
        try ( PageCursor cursor = pagedFile.io( pageId, PF_SHARED_READ_LOCK ) )
        {
            cursor.next();
            int result;
            do
            {
                result = cursor.getInt( offset );
            }
            while ( cursor.shouldRetry() );
            checkBounds( cursor );
            return result;
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public long get6ByteLong( long index, int offset )
    {
        long pageId = pageId( index );
        offset += offset( index );
        try ( PageCursor cursor = pagedFile.io( pageId, PF_SHARED_READ_LOCK ) )
        {
            cursor.next();
            long result;
            do
            {
                long low4b = cursor.getInt( offset ) & 0xFFFFFFFFL;
                long high2b = cursor.getShort( offset + Integer.BYTES );
                result = low4b | (high2b << Integer.SIZE);
            }
            while ( cursor.shouldRetry() );
            checkBounds( cursor );
            return result;
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public long getLong( long index, int offset )
    {
        long pageId = pageId( index );
        offset += offset( index );
        try ( PageCursor cursor = pagedFile.io( pageId, PF_SHARED_READ_LOCK ) )
        {
            cursor.next();
            long result;
            do
            {
                result = cursor.getLong( offset );
            }
            while ( cursor.shouldRetry() );
            checkBounds( cursor );
            return result;
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public void set( long index, byte[] value )
    {
        assert value.length == entrySize;
        long pageId = pageId( index );
        int offset = offset( index );
        try ( PageCursor cursor = pagedFile.io( pageId, PF_SHARED_WRITE_LOCK | PF_NO_GROW ) )
        {
            cursor.next();
            for ( int i = 0; i < value.length; i++ )
            {
                cursor.putByte( offset + i, value[i] );
            }
            checkBounds( cursor );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public void setByte( long index, int offset, byte value )
    {
        long pageId = pageId( index );
        offset += offset( index );
        try ( PageCursor cursor = pagedFile.io( pageId, PF_SHARED_WRITE_LOCK | PF_NO_GROW ) )
        {
            cursor.next();
            cursor.putByte( offset, value );
            checkBounds( cursor );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public void setShort( long index, int offset, short value )
    {
        long pageId = pageId( index );
        offset += offset( index );
        try ( PageCursor cursor = pagedFile.io( pageId, PF_SHARED_WRITE_LOCK | PF_NO_GROW ) )
        {
            cursor.next();
            cursor.putShort( offset, value );
            checkBounds( cursor );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public void setInt( long index, int offset, int value )
    {
        long pageId = pageId( index );
        offset += offset( index );
        try ( PageCursor cursor = pagedFile.io( pageId, PF_SHARED_WRITE_LOCK | PF_NO_GROW ) )
        {
            cursor.next();
            cursor.putInt( offset, value );
            checkBounds( cursor );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public void set6ByteLong( long index, int offset, long value )
    {
        long pageId = pageId( index );
        offset += offset( index );
        try ( PageCursor cursor = pagedFile.io( pageId, PF_SHARED_WRITE_LOCK | PF_NO_GROW ) )
        {
            cursor.next();
            cursor.putInt( offset, (int) value );
            cursor.putShort( offset + Integer.BYTES, (short) (value >>> Integer.SIZE) );
            checkBounds( cursor );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public void setLong( long index, int offset, long value )
    {
        long pageId = pageId( index );
        offset += offset( index );
        try ( PageCursor cursor = pagedFile.io( pageId, PF_SHARED_WRITE_LOCK | PF_NO_GROW ) )
        {
            cursor.next();
            cursor.putLong( offset, value );
            checkBounds( cursor );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public int get3ByteInt( long index, int offset )
    {

        long pageId = pageId( index );
        offset += offset( index );
        try ( PageCursor cursor = pagedFile.io( pageId, PF_SHARED_READ_LOCK ) )
        {
            cursor.next();
            int result;
            do
            {
                int lowWord = cursor.getShort( offset ) & 0xFFFF;
                byte highByte = cursor.getByte( offset + Short.BYTES );
                result = lowWord | (highByte << Short.SIZE);
            }
            while ( cursor.shouldRetry() );
            checkBounds( cursor );
            return result;
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public void set3ByteInt( long index, int offset, int value )
    {
        long pageId = pageId( index );
        offset += offset( index );
        try ( PageCursor cursor = pagedFile.io( pageId, PF_SHARED_WRITE_LOCK | PF_NO_GROW ) )
        {
            cursor.next();
            cursor.putShort( offset, (short) value );
            cursor.putByte( offset + Short.BYTES, (byte) (value >>> Short.SIZE) );
            checkBounds( cursor );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
