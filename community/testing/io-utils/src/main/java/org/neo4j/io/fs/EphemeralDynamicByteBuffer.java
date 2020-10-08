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
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.neo4j.io.ByteUnit;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.util.Preconditions;

import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

/**
 * Dynamically expanding ByteBuffer substitute/wrapper. This will allocate ByteBuffers on the go
 * so that we don't have to allocate too big of a buffer up-front.
 */
class EphemeralDynamicByteBuffer
{
    private static final int SECTOR_SIZE = (int) ByteUnit.kibiBytes( 1 );
    private static final byte[] ZERO_BUFFER_ARRAY = new byte[SECTOR_SIZE];

    // A reusable zero buffer. It can be reused only in this object in synchronised blocks!
    // Do resist temptation to make it static and share it among all instances of this class!
    // The reason is that when reading from a ByteBuffer its internal state is modified (position
    // is adjusted based on the amount of data read). This generally makes non-empty
    // ByteBuffers unsuitable for sharing for concurrent use even if they contain static data.
    private final ByteBuffer zeroBuffer = ByteBuffer.wrap( ZERO_BUFFER_ARRAY );
    private SortedMap<Long,ByteBuffer> sectors;
    private Exception freeCall;
    private long size;

    EphemeralDynamicByteBuffer()
    {
        sectors = new TreeMap<>();
    }

    /** This is a copying constructor, the input buffer is just read from, never stored in 'this'. */
    @SuppressWarnings( { "CopyConstructorMissesField", "SynchronizationOnLocalVariableOrMethodParameter" } )
    private EphemeralDynamicByteBuffer( EphemeralDynamicByteBuffer toClone )
    {
        this();
        synchronized ( toClone ) // Necessary for safely accessing toClone.sectors field.
        {
            toClone.assertNotFreed();
            for ( Map.Entry<Long,ByteBuffer> entry : toClone.sectors.entrySet() )
            {
                ByteBuffer sector = newSector();
                copyByteBufferContents( entry.getValue(), sector );
                sectors.put( entry.getKey(), sector );
            }
            size = toClone.getSize();
        }
    }

    synchronized EphemeralDynamicByteBuffer copy()
    {
        return new EphemeralDynamicByteBuffer( this ); // invoke "copy constructor"
    }

    synchronized void free()
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

        size = Math.max( size, pos + length );
        while ( true )
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

        while ( true )
        {
            ByteBuffer buf = sectors.getOrDefault( sector, zeroBuffer );
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

    synchronized long getSize()
    {
        return size;
    }

    synchronized void truncate( long newSize )
    {
        Preconditions.requireNonNegative( newSize );
        size = newSize;
        SortedMap<Long, ByteBuffer> tail = sectors.tailMap( newSize - ( SECTOR_SIZE - 1 ) );
        if ( tail.isEmpty() )
        {
            return;
        }
        Long firstKey = tail.firstKey();
        if ( firstKey <= newSize )
        {
            ByteBuffer buffer = tail.get( firstKey );
            int tailToClear = Math.toIntExact( newSize - firstKey );
            buffer.position( tailToClear );
            while ( buffer.hasRemaining() )
            {
                buffer.put( (byte) 0 );
            }
        }
        sectors.tailMap( firstKey + 1 ).clear();
    }

    private static void copyByteBufferContents( ByteBuffer from, ByteBuffer to )
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

    private static ByteBuffer newSector()
    {
        return ByteBuffers.allocate( EphemeralDynamicByteBuffer.SECTOR_SIZE, INSTANCE );
    }

    private synchronized ByteBuffer getOrCreateSector( long sector )
    {
        ByteBuffer buf = sectors.get( sector );
        if ( buf == null )
        {
            buf = newSector();
            sectors.put( sector, buf );
        }
        return buf;
    }

    private synchronized void assertNotFreed()
    {
        if ( sectors == null )
        {
            throw new IllegalStateException( "This buffer has been freed.", freeCall );
        }
    }
}
