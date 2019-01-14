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
package org.neo4j.kernel.impl.store.id;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.collection.primitive.PrimitiveLongArrayQueue;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static org.neo4j.kernel.impl.store.id.IdContainer.NO_RESULT;

/**
 * Instances of this class maintain a list of free ids with the potential of overflowing to disk if the number
 * of free ids becomes too large. This class has no expectations and makes no assertions as to the ids freed.
 * Such consistency guarantees, for example uniqueness of values, should be imposed from users of this class.
 * <p>
 * There is no guarantee as to the ordering of the values returned (i.e. FIFO, LIFO or any other temporal strategy),
 * primarily because the aggressiveMode argument influences exactly that behaviour.
 * <p>
 * The {@link #aggressiveMode} parameter controls whether or not IDs which are freed during this lifecycle will
 * be allowed to be reused during the same lifecycle. The alternative non-aggressive behaviour is that the IDs
 * will only be reused after a close/open cycle. This would generally correlate with a restart of the database.
 */
public class FreeIdKeeper implements Closeable
{
    private static final int ID_ENTRY_SIZE = Long.BYTES;

    private final PrimitiveLongArrayQueue freeIds = new PrimitiveLongArrayQueue();
    private final PrimitiveLongArrayQueue readFromDisk = new PrimitiveLongArrayQueue();
    private final StoreChannel channel;
    private final int batchSize;
    private final boolean aggressiveMode;

    private long freeIdCount;

    /**
     * Keeps the position where batches of IDs will be flushed out to.
     * This can be viewed as being put on top of a stack.
     */
    private long stackPosition;

    /**
     * The position before we started this run.
     * <p>
     * Useful to keep track of the gap that will form in non-aggressive mode
     * when IDs from old runs get reused and newly freed IDs are put on top
     * of the stack. During a clean shutdown the gap will be compacted away.
     * <p>
     * During an aggressive run a gap is never formed since batches of free
     * IDs are flushed on top of the stack (end of file) and also read in
     * from the top of the stack.
     */
    private long initialPosition;

    /**
     * A keeper of freed IDs.
     *
     * @param channel a channel to the free ID file.
     * @param batchSize the number of IDs which are read/written to disk in one go.
     * @param aggressiveMode whether to reuse freed IDs during this lifecycle.
     * @throws IOException if an I/O error occurs.
     */
    FreeIdKeeper( StoreChannel channel, int batchSize, boolean aggressiveMode ) throws IOException
    {
        this.channel = channel;
        this.batchSize = batchSize;
        this.aggressiveMode = aggressiveMode;

        this.initialPosition = channel.size();
        this.stackPosition = initialPosition;
        this.freeIdCount = stackPosition / ID_ENTRY_SIZE;
    }

    static long countFreeIds( StoreChannel channel ) throws IOException
    {
        return channel.size() / ID_ENTRY_SIZE;
    }

    public void freeId( long id )
    {
        freeIds.enqueue( id );
        freeIdCount++;

        if ( freeIds.size() >= batchSize )
        {
            long endPosition = flushFreeIds( ByteBuffer.allocate( batchSize * ID_ENTRY_SIZE ) );
            if ( aggressiveMode )
            {
                stackPosition = endPosition;
            }
        }
    }

    private void truncate( long position )
    {
        try
        {
            channel.truncate( position );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Failed to truncate", e );
        }
    }

    public long getId()
    {
        long result;
        if ( freeIds.size() > 0 && aggressiveMode )
        {
            result = freeIds.dequeue();
            freeIdCount--;
        }
        else
        {
            result = getIdFromDisk();
            if ( result != NO_RESULT )
            {
                freeIdCount--;
            }
        }
        return result;
    }

    public long[] getIds( int numberOfIds )
    {
        if ( freeIdCount == 0 )
        {
            return PrimitiveLongCollections.EMPTY_LONG_ARRAY;
        }
        int reusableIds = (int) min( numberOfIds, freeIdCount );
        long[] ids = new long[reusableIds];
        int cursor = 0;
        while ( (cursor < reusableIds) && !freeIds.isEmpty() )
        {
            ids[cursor++] = freeIds.dequeue();
        }
        while ( cursor < reusableIds )
        {
            ids[cursor++] = getIdFromDisk();
        }
        freeIdCount -= reusableIds;
        return ids;
    }

    private long getIdFromDisk()
    {
        if ( readFromDisk.isEmpty() )
        {
            readIdBatch();
        }
        if ( !readFromDisk.isEmpty() )
        {
            return readFromDisk.dequeue();
        }
        else
        {
            return NO_RESULT;
        }
    }

    public long getCount()
    {
        return freeIdCount;
    }

    /*
     * After this method returns, if there were any entries found, they are placed in the readFromDisk list.
     */
    private void readIdBatch()
    {
        try
        {
            readIdBatch0();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Failed reading free id batch", e );
        }
    }

    private void readIdBatch0() throws IOException
    {
        if ( stackPosition == 0 )
        {
            return;
        }

        long startPosition = max( stackPosition - batchSize * ID_ENTRY_SIZE, 0 );
        int bytesToRead = toIntExact( stackPosition - startPosition );
        ByteBuffer readBuffer = ByteBuffer.allocate( bytesToRead );

        channel.position( startPosition );
        channel.readAll( readBuffer );
        stackPosition = startPosition;

        readBuffer.flip();
        int idsRead = bytesToRead / ID_ENTRY_SIZE;
        for ( int i = 0; i < idsRead; i++ )
        {
            long id = readBuffer.getLong();
            readFromDisk.enqueue( id );
        }
        if ( aggressiveMode )
        {
            truncate( startPosition );
        }
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
            long id = freeIds.dequeue();
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
        ByteBuffer writeBuffer = ByteBuffer.allocate( batchSize * ID_ENTRY_SIZE );
        flushFreeIds( writeBuffer );
        freeIds.addAll( readFromDisk );
        flushFreeIds( writeBuffer );
        if ( !aggressiveMode )
        {
            compact( writeBuffer );
        }
        channel.force( false );
    }

    /**
     * Compacts away the gap which will form in non-aggressive (regular) mode
     * when batches are read in from disk.
     * <p>
     * The gap will contain already used IDs so it is important to remove it
     * on a clean shutdown. The freed IDs will not be reused after an
     * unclean shutdown, as guaranteed by the external user.
     * <pre>
     * Below diagram tries to explain the situation
     *
     *   S = old IDs which are still free (on the Stack)
     *   G = the Gap which has formed, due to consuming old IDs
     *   N = the New IDs which have been freed during this run (will be compacted to the left)
     *
     *     stackPosition
     *          v
     * [ S S S S G G G N N N N N N N N ]
     *                ^
     *          initialPosition
     * </pre>
     * After compaction the state will be:
     * <pre>
     * [ S S S S N N N N N N N N ]
     * </pre>
     * and the last part of the file is truncated.
     */
    private void compact( ByteBuffer writeBuffer ) throws IOException
    {
        assert stackPosition <= initialPosition; // the stack can only be consumed in regular mode
        if ( initialPosition == stackPosition )
        {
            // there is no compaction to be done
            return;
        }

        long writePosition = stackPosition;
        long readPosition = initialPosition; // readPosition to end of file contain new free IDs, to be compacted
        int nBytes;
        do
        {
            writeBuffer.clear();
            channel.position( readPosition );
            nBytes = channel.read( writeBuffer );

            if ( nBytes > 0 )
            {
                readPosition += nBytes;

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
