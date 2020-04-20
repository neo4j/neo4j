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
package org.neo4j.io.fs;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.memory.ByteBuffers;

/**
 * Dynamically expanding ByteBuffer substitute/wrapper. This will allocate ByteBuffers on the go
 * so that we don't have to allocate too big of a buffer up-front.
 */
class EphemeralDynamicByteBuffer implements Iterable<ByteBuffer>
{
    private static final int SECTOR_SIZE = (int) ByteUnit.kibiBytes( 1 );
    private static final ByteBuffer ZERO_BUFFER = ByteBuffer.allocate( SECTOR_SIZE );
    private SortedMap<Long,ByteBuffer> sectors;
    private Exception freeCall;

    EphemeralDynamicByteBuffer()
    {
        sectors = new TreeMap<>();
    }

    /** This is a copying constructor, the input buffer is just read from, never stored in 'this'. */
    @SuppressWarnings( "CopyConstructorMissesField" )
    private EphemeralDynamicByteBuffer( EphemeralDynamicByteBuffer toClone )
    {
        this();
        toClone.assertNotFreed();
        for ( Map.Entry<Long,ByteBuffer> entry : toClone.sectors.entrySet() )
        {
            ByteBuffer sector = allocate( SECTOR_SIZE );
            copyByteBufferContents( entry.getValue(), sector );
            sectors.put( entry.getKey(), sector );
        }
    }

    public Iterator<ByteBuffer> iterator()
    {
        if ( sectors.isEmpty() )
        {
            return Iterators.emptyResourceIterator();
        }

        return new Iterator<>()
        {
            final long last = sectors.lastKey();
            long next;

            @Override
            public boolean hasNext()
            {
                return next <= last;
            }

            @Override
            public ByteBuffer next()
            {
                if ( next > last )
                {
                    throw new NoSuchElementException();
                }
                ByteBuffer sector = sectors.get( next );
                next++;
                return Objects.requireNonNullElse( sector, ZERO_BUFFER ).position( 0 );
            }
        };
    }

    synchronized EphemeralDynamicByteBuffer copy()
    {
        return new EphemeralDynamicByteBuffer( this ); // invoke "copy constructor"
    }

    private void copyByteBufferContents( ByteBuffer from, ByteBuffer to )
    {
        int positionBefore = from.position();
        try
        {
            from.position( 0 );
            to.put( from );
        }
        finally
        {
            from.position( positionBefore );
            to.position( 0 );
        }
    }

    private ByteBuffer allocate( long capacity )
    {
        return ByteBuffers.allocate( Math.toIntExact( capacity ) );
    }

    void free()
    {
        assertNotFreed();
        sectors = null;
        freeCall = new Exception(
                "You're most likely seeing this exception because there was an attempt to use this buffer " +
                        "after it was freed. This stack trace may help you figure out where and why it was freed." );
    }

    synchronized void put( long pos, byte[] bytes, int off, int length )
    {
        long sector = pos / SECTOR_SIZE;
        int offset = (int) (pos % SECTOR_SIZE);

        for ( ;; )
        {
            ByteBuffer buf = getOrCreateSector( sector );
            buf.position( offset );
            int toPut = Math.min( buf.remaining(), length );
            buf.put( bytes, off, toPut );
            if ( toPut == length )
            {
                break;
            }
            off += toPut;
            length -= toPut;
            offset = 0;
            sector += 1;
        }
    }

    synchronized void get( long pos, byte[] bytes, int off, int length )
    {
        long sector = pos / SECTOR_SIZE;
        int offset = (int) (pos % SECTOR_SIZE);

        for ( ;; )
        {
            ByteBuffer buf = sectors.getOrDefault( sector, ZERO_BUFFER );
            buf.position( offset );
            int toGet = Math.min( buf.remaining(), length );
            buf.get( bytes, off, toGet );
            if ( toGet == length )
            {
                break;
            }
            off += toGet;
            length -= toGet;
            offset = 0;
            sector += 1;
        }
    }

    private ByteBuffer getOrCreateSector( long sector )
    {
        ByteBuffer buf = sectors.get( sector );
        if ( buf == null )
        {
            buf = allocate( SECTOR_SIZE );
            sectors.put( sector, buf );
        }
        return buf;
    }

    private void assertNotFreed()
    {
        if ( sectors == null )
        {
            throw new IllegalStateException( "This buffer has been freed.", freeCall );
        }
    }
}
