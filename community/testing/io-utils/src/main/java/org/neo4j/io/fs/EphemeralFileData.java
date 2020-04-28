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

import java.io.File;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.neo4j.internal.helpers.collection.PrefetchingIterator;
import org.neo4j.io.ByteUnit;

import static java.lang.Math.min;

class EphemeralFileData
{
    private static final ThreadLocal<byte[]> SCRATCH_PAD =
            ThreadLocal.withInitial( () -> new byte[(int) ByteUnit.kibiBytes( 1 )] );
    private final File file;
    private final Clock clock;
    private final Collection<WeakReference<EphemeralFileChannel>> channels = new ArrayList<>();
    private volatile EphemeralDynamicByteBuffer fileAsBuffer;
    private volatile EphemeralDynamicByteBuffer forcedBuffer;
    private volatile long lastModified;
    private int locked; // Guarded by lock on 'channels'

    EphemeralFileData( File file, Clock clock )
    {
        this( file, new EphemeralDynamicByteBuffer(), clock );
    }

    private EphemeralFileData( File file, EphemeralDynamicByteBuffer data, Clock clock )
    {
        this.file = file;
        this.fileAsBuffer = data;
        this.forcedBuffer = data.copy();
        this.clock = clock;
        this.lastModified = clock.millis();
    }

    int read( EphemeralPositionable fc, ByteBuffer dst )
    {
        int wanted = dst.limit() - dst.position();
        var fileBuffer = fileAsBuffer; // Snapshot buffer instance with volatile read.
        long size = fileBuffer.getSize();
        long available = min( wanted, size - fc.pos() );
        if ( available <= 0 )
        {
            return -1; // EOF
        }
        long pending = available;
        // Read up until our internal size
        byte[] scratchPad = SCRATCH_PAD.get();
        while ( pending > 0 )
        {
            int howMuchToReadThisTime = Math.toIntExact( min( pending, scratchPad.length ) );
            long pos = fc.pos();
            fileBuffer.get( pos, scratchPad, 0, howMuchToReadThisTime );
            fc.pos( pos + howMuchToReadThisTime );
            dst.put( scratchPad, 0, howMuchToReadThisTime );
            pending -= howMuchToReadThisTime;
        }
        return Math.toIntExact( available ); // return how much data was read
    }

    int write( EphemeralPositionable fc, ByteBuffer src )
    {
        int wanted = src.limit() - src.position();
        int pending = wanted;
        byte[] scratchPad = SCRATCH_PAD.get();
        var fileBuffer = fileAsBuffer; // Snapshot buffer instance with volatile read.

        while ( pending > 0 )
        {
            int howMuchToWriteThisTime = min( pending, scratchPad.length );
            src.get( scratchPad, 0, howMuchToWriteThisTime );
            long pos = fc.pos();
            fileBuffer.put( pos, scratchPad, 0, howMuchToWriteThisTime );
            fc.pos( pos + howMuchToWriteThisTime );
            pending -= howMuchToWriteThisTime;
        }

        lastModified = clock.millis();
        return wanted;
    }

    Iterable<ByteBuffer> buffers()
    {
        return fileAsBuffer;
    }

    EphemeralFileData copy()
    {
        return new EphemeralFileData( file, fileAsBuffer.copy(), clock );
    }

    void free()
    {
        fileAsBuffer.free();
    }

    void open( EphemeralFileChannel channel )
    {
        synchronized ( channels )
        {
            channels.add( new WeakReference<>( channel ) );
        }
    }

    void force()
    {
        forcedBuffer = fileAsBuffer.copy();
    }

    void crash()
    {
        fileAsBuffer = forcedBuffer.copy();
    }

    void close( EphemeralFileChannel channel )
    {
        synchronized ( channels )
        {
            locked = 0; // Regular file systems seems to release all file locks when closed...
            Iterator<WeakReference<EphemeralFileChannel>> iterator = channels.iterator();
            while ( iterator.hasNext() )
            {
                WeakReference<EphemeralFileChannel> reference = iterator.next();
                EphemeralFileChannel openChannel = reference.get();
                if ( openChannel == null || openChannel == channel )
                {
                    iterator.remove();
                }
            }
        }
    }

    Iterator<EphemeralFileChannel> getOpenChannels()
    {
        ArrayList<WeakReference<EphemeralFileChannel>> snapshot;
        synchronized ( channels )
        {
            snapshot = new ArrayList<>( channels );
        }
        final Iterator<WeakReference<EphemeralFileChannel>> refs = snapshot.iterator();

        return new PrefetchingIterator<>()
        {
            @Override
            protected EphemeralFileChannel fetchNextOrNull()
            {
                while ( refs.hasNext() )
                {
                    EphemeralFileChannel channel = refs.next().get();
                    if ( channel != null )
                    {
                        return channel;
                    }
                    refs.remove();
                }
                return null;
            }

            @Override
            public void remove()
            {
                refs.remove();
            }
        };
    }

    long size()
    {
        return fileAsBuffer.getSize();
    }

    void truncate( long newSize )
    {
        this.fileAsBuffer.truncate( newSize );
    }

    boolean takeLock()
    {
        synchronized ( channels )
        {
            if ( locked != 0 )
            {
                return false;
            }
            locked++;
            return true;
        }
    }

    void releaseLock()
    {
        synchronized ( channels )
        {
            if ( locked != 0 )
            {
                locked--;
            }
        }
    }

    @Override
    public String toString()
    {
        return "EphemeralFileData[size: " + fileAsBuffer.getSize() + ']';
    }

    long getLastModified()
    {
        return lastModified;
    }
}
