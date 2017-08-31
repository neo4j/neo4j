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
package org.neo4j.kernel.impl.store.id;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;

import static org.neo4j.kernel.impl.store.id.IdContainer.NO_RESULT;

/**
 * Instances of this class maintain a list of free ids with the potential of overflowing to disk if the number
 * of free ids becomes too large. This class has no expectations and makes no assertions as to the ids freed.
 * Such consistency guarantees, for example uniqueness of values, should be imposed from users of this class.
 * <p>
 * There is no guarantee as to the ordering of the values returned (i.e. FIFO, LIFO or any other temporal strategy),
 * primarily because the aggressiveReuse argument influences exactly that behaviour.
 * <p>
 * The {@link #aggressiveReuse} parameter controls whether or not IDs which are freed during this lifecycle will
 * be allowed to be reused during the same lifecycle. The alternative non-aggressive behaviour is that the IDs
 * will only be reused after a close/open cycle. This would generally correlate with a restart of the database.
 */
public class FreeIdKeeper implements Closeable
{
    private static final int ID_ENTRY_SIZE = Long.BYTES;

    private final List<Long> freeIds = new ArrayList<>();
    private final List<Long> readFromDisk = new ArrayList<>();
    private final StoreChannel channel;
    private final int threshold;
    private final boolean aggressiveReuse;

    private long freeIdCount;

    private long readPosition; // the place from where we read. Always <= maxReadPosition
    private long maxReadPosition;

    /**
     * A keeper of freed IDs.
     *
     * @param channel a channel to the free ID file.
     * @param threshold the threshold for when the in-memory buffer is flushed to the file.
     * @param aggressiveReuse whether to reuse freed IDs during this lifecycle.
     * @throws IOException if an I/O error occurs.
     */
    FreeIdKeeper( StoreChannel channel, int threshold, boolean aggressiveReuse ) throws IOException
    {
        this.channel = channel;
        this.threshold = threshold;
        this.aggressiveReuse = aggressiveReuse;

        this.maxReadPosition = channel.size();
        this.freeIdCount = maxReadPosition / ID_ENTRY_SIZE;
    }

    public void freeId( long id )
    {
        freeIds.add( id );
        freeIdCount++;

        if ( freeIds.size() >= threshold )
        {
            long endPosition = flushFreeIds( ByteBuffer.allocate( threshold * ID_ENTRY_SIZE ) );
            if ( aggressiveReuse )
            {
                maxReadPosition = endPosition;
            }
        }
    }

    public long getId()
    {
        long result;
        if ( freeIds.size() > 0 && aggressiveReuse )
        {
            result = freeIds.remove( 0 );
            freeIdCount--;
        }
        else if ( readFromDisk.size() > 0 )
        {
            result = readFromDisk.remove( 0 );
            freeIdCount--;
        }
        else if ( freeIdCount > 0 && readIdBatch() )
        {
            result = readFromDisk.remove( 0 );
            freeIdCount--;
        }
        else
        {
            result = NO_RESULT;
        }
        return result;
    }

    public long getCount()
    {
        return freeIdCount;
    }

    /*
     * Returns true iff there are bytes between the current readPosition
     * and maxReadPosition, i.e. there are more entries to read.
     */
    private boolean canReadMoreIdBatches()
    {
        assert (maxReadPosition - readPosition) % ID_ENTRY_SIZE == 0 : String.format(
                "maxReadPosition %d, readPosition %d do not contain an integral number of entries", maxReadPosition, readPosition );
        return readPosition < maxReadPosition;
    }

    /*
     * After this method returns, if there were any entries found, they are placed in the readFromDisk list and the
     * readPosition is updated accordingly.
     */
    private boolean readIdBatch()
    {
        try
        {
            return readIdBatch0();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Failed reading free id batch", e );
        }
    }

    private boolean readIdBatch0() throws IOException
    {
        if ( !canReadMoreIdBatches() )
        {
            return false;
        }
        boolean readAnyIds = false;

        int howMuchToRead = (int) Math.min( threshold * ID_ENTRY_SIZE, maxReadPosition - readPosition );
        assert howMuchToRead % ID_ENTRY_SIZE == 0 : "reads should happen in multiples of ID_ENTRY_SIZE, instead was " + howMuchToRead;
        ByteBuffer readBuffer = ByteBuffer.allocate( howMuchToRead );

        channel.position( readPosition );
        int bytesRead = channel.read( readBuffer );
        readPosition += bytesRead;
        assert channel.position() <= maxReadPosition;
        readBuffer.flip();
        assert (bytesRead % ID_ENTRY_SIZE) == 0;
        int idsRead = bytesRead / ID_ENTRY_SIZE;
        for ( int i = 0; i < idsRead; i++ )
        {
            long id = readBuffer.getLong();
            if ( id != NO_RESULT )
            {
                readFromDisk.add( id );
                readAnyIds = true;
            }
        }
        return readAnyIds;
    }

    /**
     * Flushes the currently collected in-memory freed IDs to the storage.
     */
    private long flushFreeIds( ByteBuffer writeBuffer )
    {
        try
        {
            return flushFreeIds0( writeBuffer );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Unable to write free id batch", e );
        }
    }

    private long flushFreeIds0( ByteBuffer writeBuffer ) throws IOException
    {
        channel.position( channel.size() );
        writeBuffer.clear();
        while ( !freeIds.isEmpty() )
        {
            long id = freeIds.remove( 0 );
            if ( id == NO_RESULT )
            {
                continue;
            }
            writeBuffer.putLong( id );
            if ( writeBuffer.position() == writeBuffer.capacity() )
            {
                writeBuffer.flip();
                channel.writeAll( writeBuffer );
                writeBuffer.clear();
            }
        }
        writeBuffer.flip();
        if ( writeBuffer.hasRemaining() )
        {
            channel.writeAll( writeBuffer );
        }
        return channel.position();
    }

    /*
     * Writes both freeIds and readFromDisk lists to disk and truncates the channel to size.
     * It forces but does not close the channel.
     */
    @Override
    public void close() throws IOException
    {
        ByteBuffer writeBuffer = ByteBuffer.allocate( threshold * ID_ENTRY_SIZE );
        flushFreeIds( writeBuffer );
        freeIds.addAll( readFromDisk );
        flushFreeIds( writeBuffer );
        compact( writeBuffer );
        channel.force( false );
    }

    /**
     * Compacts away the already returned IDs. The remaining IDs
     * are moved to the beginning of the file and the end is
     * then truncated away.
     */
    private void compact( ByteBuffer writeBuffer ) throws IOException
    {
        if ( readPosition == 0 )
        {
            // there is no compaction to be done
            return;
        }

        long writePosition = 0;
        long position = readPosition;
        int nBytes;
        do
        {
            writeBuffer.clear();
            channel.position( position );
            nBytes = channel.read( writeBuffer );
            position += nBytes;

            if ( nBytes > 0 )
            {
                writeBuffer.flip();
                channel.position( writePosition );
                channel.writeAll( writeBuffer );
                writePosition += nBytes;
            }
        }
        while ( nBytes > 0 );

        channel.truncate( writePosition );
    }
}
