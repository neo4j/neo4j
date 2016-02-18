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
package org.neo4j.kernel.impl.store.id;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;

import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;

/**
 * Instances of this class maintain a list of free ids with the potential to overflow to disk if the number
 * of free ids becomes too large.
 * This class has no expectations and makes no assertions as to the ids freed. Such consistency guarantees, for
 * example uniqueness of values, should be imposed from users of this class.
 * There is no guarantee as to the ordering of the values returned (i.e. FIFO, LIFO or any other temporal strategy),
 * primarily because the aggressiveReuse argument influences exactly that behaviour.
 * The {@link StoreChannel} used for persistence can be used by other writers as well. The expectation of this class
 * is that the span of the file from the position it is at when passed at the constructor and forward is available for
 * reads and exclusive writes. Equivalently, instances of this class will never write in the portion of the channel
 * from the beginning until the position it is at when passed at the constructor.
 */
public class FreeIdKeeper implements Closeable
{
    public static final long NO_RESULT = -1;
    public static final int ID_ENTRY_SIZE = Long.BYTES;
    private final LinkedList<Long> freeIds = new LinkedList<>();
    private final LinkedList<Long> readFromDisk = new LinkedList<>();
    private final StoreChannel channel;
    private final int threshold;
    /*
     * aggressiveReuse flags if ids freed during this run (before close() is called) are legitimate return values or not.
     * If yes, then they are shown preference over ids persisted to disk. If not, then they are persisted in batches
     * but are not returned until this keeper is closed and reopened. This is achieved by marking the position of the file
     * where the persisted ids run up to when the file was opened and never reading past that. The newly freed ids then
     * are persisted beyond that point and are never read.
     */
    private final boolean aggressiveReuse;
    private long defraggedIdCount;

    private final long lowWatermarkForChannelPosition; // the lowest possible position the channel can be at - we don't own anything "in front" of that
    /*
     * maxReadPosition remains constant if aggressiveReuse is false, pointing to the position after which we should not read because
     * it contains overflow ids from this run. If aggressiveReuse is true, then it points to the end of the file.
     */
    private long maxReadPosition;
    private long readPosition; // the place from where we read. Always <= maxReadPosition

    public FreeIdKeeper( StoreChannel channel, int threshold, boolean aggressiveReuse ) throws IOException
    {
        this.channel = channel;
        this.threshold = threshold;
        this.aggressiveReuse = aggressiveReuse;
        this.lowWatermarkForChannelPosition = channel.position();
        readPosition = lowWatermarkForChannelPosition;
        restoreIdsOnStartup();
    }

    private void restoreIdsOnStartup() throws IOException
    {
        maxReadPosition = channel.size(); // this is always true regardless of aggressiveReuse. It only matters once we start writing
        defraggedIdCount = ( maxReadPosition - lowWatermarkForChannelPosition ) / ID_ENTRY_SIZE;
        readIdBatch();
    }

    public void freeId( long id )
    {
        freeIds.add( id );
        defraggedIdCount++;
        if ( freeIds.size() >= threshold )
        {
            writeIdBatch( ByteBuffer.allocate( threshold * ID_ENTRY_SIZE ) );
        }
    }

    public long getId()
    {
        long result;
        if ( freeIds.size() > 0 && aggressiveReuse )
        {
            result = freeIds.poll();
            defraggedIdCount--;
        }
        else if ( readFromDisk.size() > 0 )
        {
            result = readFromDisk.removeFirst();
            defraggedIdCount--;
        }
        else if ( defraggedIdCount > 0 && canReadMoreIdBatches() )
        {
            readIdBatch();
            result = readFromDisk.removeFirst();
            defraggedIdCount--;
        }
        else
        {
            result = NO_RESULT;
        }
        return result;
    }

    public long getCount()
    {
        return defraggedIdCount;
    }

    /*
     * Returns true iff there are bytes between the current readPosition and maxReadPosition, i.e. there are more
     * entries to read.
     */
    private boolean canReadMoreIdBatches()
    {
        assert (maxReadPosition - readPosition) % ID_ENTRY_SIZE == 0 :
                String.format("maxReadPosition %d, readPosition %d do not contain an integral number of entries",
                        maxReadPosition, readPosition);
        return readPosition < maxReadPosition;
    }

    /*
     * After this method returns, if there were any entries found, they are placed in the readFromDisk list and the
     * readPosition is updated accordingly.
     */
    private void readIdBatch()
    {
        if ( !canReadMoreIdBatches() )
        {
            return;
        }

        try
        {
            int howMuchToRead = (int) Math.min( threshold * ID_ENTRY_SIZE, maxReadPosition - readPosition );
            assert howMuchToRead % ID_ENTRY_SIZE == 0 : "reads should happen in multiples of ID_ENTRY_SIZE, instead was " + howMuchToRead;
            ByteBuffer readBuffer = ByteBuffer.allocate( howMuchToRead );

            positionChannel( readPosition );
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
                }
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException(
                    "Failed reading defragged id batch", e );
        }
    }

    /*
     * Writes both freeIds and readFromDisk lists to disk and truncates the channel to size. It forces but does not
     * close the channel.
     */
    @Override
    public void close() throws IOException
    {
        ByteBuffer writeBuffer = ByteBuffer.allocate( threshold * ID_ENTRY_SIZE );
        writeIdBatch( writeBuffer );
        while ( !readFromDisk.isEmpty() )
        {
            freeIds.add( readFromDisk.removeFirst() );
        }
        writeIdBatch( writeBuffer );
        defragReusableIdsInFile( writeBuffer );
        channel.force( false );
    }

    /*
     * writes to disk, after the current channel.position(), the contents of the freeIds list. If aggressiveReuse
     * is set, it will also forward the maxReadPosition to the end of the file.
     */
    private void writeIdBatch( ByteBuffer writeBuffer )
    {
        try
        {
            // position at end
            positionChannel( channel.size() );
            writeBuffer.clear();
            while ( !freeIds.isEmpty() )
            {
                long id = freeIds.removeFirst();
                if ( id == NO_RESULT )
                {
                    continue;
                }
                writeBuffer.putLong( id );
                if ( writeBuffer.position() == writeBuffer.capacity() )
                {
                    writeBuffer.flip();
                    while ( writeBuffer.hasRemaining() )
                    {
                        channel.write( writeBuffer );
                    }
                    writeBuffer.clear();
                }
            }
            writeBuffer.flip();
            while ( writeBuffer.hasRemaining() )
            {
                channel.write( writeBuffer );
            }
            if ( aggressiveReuse )
            {
                maxReadPosition = channel.size();
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException(
                    "Unable to write defragged id " + " batch", e );
        }
    }

    private void positionChannel( long newPosition ) throws IOException
    {
        if ( newPosition < lowWatermarkForChannelPosition )
        {
            throw new IllegalStateException( String.format( "%d is less than the lowest position (%d) this id keeper " +
                    "can go", newPosition, lowWatermarkForChannelPosition ) );
        }
        channel.position( newPosition );
    }

    /**
     * Utility method that will dump all defragged id's to console. Do not call
     * while running store using this id generator since it could corrupt the id
     * generator (not thread safe). This method will close the id generator after
     * being invoked.
     */
    // TODO make this a nice, cosy, reusable visitor instead?
    public synchronized void dumpFreeIds() throws IOException
    {
        while ( canReadMoreIdBatches() )
        {
            readIdBatch();
        }
        for ( Long id : freeIds )
        {
            System.out.print( " " + id );
        }
        close();
    }

    private void defragReusableIdsInFile( ByteBuffer writeBuffer ) throws IOException
    {
        if ( readPosition > lowWatermarkForChannelPosition )
        {
            long writePosition = lowWatermarkForChannelPosition;
            long position = Math.min( readPosition, maxReadPosition );
            int bytesRead;
            do
            {
                writeBuffer.clear();
                channel.position( position );
                bytesRead = channel.read( writeBuffer );
                position += bytesRead;
                writeBuffer.flip();
                channel.position( writePosition );
                writePosition += channel.write( writeBuffer );
            }
            while ( bytesRead > 0 );
            // truncate
            channel.truncate( writePosition );
        }
    }
}
