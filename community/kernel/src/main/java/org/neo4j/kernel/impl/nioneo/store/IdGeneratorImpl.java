/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

import static org.neo4j.kernel.impl.util.FileUtils.truncateFile;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class generates unique ids for a resource type. For example, nodes in a
 * nodes space are connected to each other via relationships. On nodes and
 * relationship one can add properties. We have three different resource types
 * here (nodes, relationships and properties) where each resource needs a unique
 * id to be able to differ resources of the same type from each other. Creating
 * three id generators (one for each resource type ) will do the trick.
 * <p>
 * <CODE>IdGenerator</CODE> makes use of so called "defragged" ids. A
 * defragged id is an id that has been in use one or many times but the resource
 * that was using it doesn't exist anymore. This makes it possible to reuse the
 * id and that in turn makes it possible to write a resource store with fixed
 * records and size (you can calculate the position of a record by knowing the
 * id without using indexes or a translation table).
 * <p>
 * The id returned from {@link #nextId} may not be the lowest
 * available id but will be one of the defragged ids if such exist or the next
 * new free id that has never been used.
 * <p>
 * The {@link #freeId} will not check if the id passed in to it really is free.
 * Passing a non free id will corrupt the id generator and {@link #nextId}
 * method will eventually return that id.
 * <p>
 * The {@link #close()} method must always be invoked when done using an
 * generator (for this time). Failure to do will render the generator as
 * "sticky" and unusable next time you try to initialize a generator using the
 * same file. There can only be one id generator instance per id generator file.
 * <p>
 * In case of disk/file I/O failure an <CODE>IOException</CODE> is thrown.
 */
public class IdGeneratorImpl implements IdGenerator
{
    // sticky(byte), nextFreeId(long)
    private static final int HEADER_SIZE = 9;

    // if sticky the id generator wasn't closed properly so it has to be
    // rebuilt (go through the node, relationship, property, rel type etc files)
    private static final byte CLEAN_GENERATOR = (byte) 0;
    private static final byte STICKY_GENERATOR = (byte) 1;
    
    public static final long INTEGER_MINUS_ONE = 0xFFFFFFFFL;  // 4294967295L;
    
    // number of defragged ids to grab from file in batch (also used for write)
    private int grabSize = -1;
    private AtomicLong nextFreeId = new AtomicLong( -1 );
    // total bytes read from file, used in writeIdBatch() and close()
    private long totalBytesRead = 0;
    // true if more defragged ids can be read from file
    private boolean haveMore = true;
    // marks where this sessions released ids will be written
    private long readBlocksTo = HEADER_SIZE;
    // used to calculate number of ids actually in use
    private long defraggedIdCount = -1;

    private final String fileName;
    private FileChannel fileChannel = null;
    // in memory defragged ids read from file (and from freeId)
    private final LinkedList<Long> defragedIdList = 
        new LinkedList<Long>();
    // in memory newly free defragged ids that havn't been flushed to disk yet
    private final LinkedList<Long> releasedIdList = 
        new LinkedList<Long>();
    // buffer used in readIdBatch()
    private ByteBuffer readBuffer = null;
    // buffer used in writeIdBatch() and close()
    private ByteBuffer writeBuffer = null;

    private final long max;

    /**
     * Opens the id generator represented by <CODE>fileName</CODE>. The
     * <CODE>grabSize</CODE> means how many defragged ids we should keep in
     * memory and is also the size (x4) of the two buffers used for reading and
     * writing to the id generator file. The highest returned id will be read
     * from file and if <CODE>grabSize</CODE> number of ids exist they will be
     * read into memory (if less exist all defragged ids will be in memory).
     * <p>
     * If this id generator hasn't been closed properly since the previous
     * session (sticky) an <CODE>IOException</CODE> will be thrown. When this
     * happens one has to rebuild the id generator from the (node/rel/prop)
     * store file.
     * 
     * @param fileName
     *            The file name (and path if needed) for the id generator to be
     *            opened
     * @param grabSize
     *            The number of defragged ids to keep in memory
     * @param max is the highest possible id to be returned by this id generator from
     * {@link #nextId()}.
     * @throws UnderlyingStorageException
     *             If no such file exist or if the id generator is sticky
     */
    public IdGeneratorImpl( String fileName, int grabSize, long max )
    {
        if ( grabSize < 1 )
        {
            throw new IllegalArgumentException( "Illegal grabSize: " + grabSize );
        }
        this.max = max;
        this.fileName = fileName;
        this.grabSize = grabSize;
        readBuffer = ByteBuffer.allocate( grabSize * 8 );
        writeBuffer = ByteBuffer.allocate( grabSize * 8 );
        initGenerator();
    }

    /**
     * Returns the next "free" id. If a defragged id exist it will be returned
     * else the next free id that hasn't been used yet is returned. If no id
     * exist the capacity is exceeded (all values <= max are taken) and a
     * {@link UnderlyingStorageException} will be thrown.
     * 
     * @return The next free id
     * @throws UnderlyingStorageException
     *             If the capacity is exceeded
     * @throws IllegalStateException if this id generator has been closed
     */
    public synchronized long nextId()
    {
        assertStillOpen();
        long nextDefragId = nextIdFromDefragList();
        if ( nextDefragId != -1 )
        {
            return nextDefragId;
        }
        long id = nextFreeId.get();
        if ( id == INTEGER_MINUS_ONE )
        {
            // Skip the integer -1 (0xFFFFFFFF) because it represents
            // special values, f.ex. the end of a relationships/property chain.
            id = nextFreeId.incrementAndGet();
        }
        assertIdWithinCapacity( id );
        nextFreeId.incrementAndGet();
        return id;
    }

    private void assertIdWithinCapacity( long id )
    {
        if ( id > max || id < 0  )
        {
            throw new UnderlyingStorageException( "Id capacity exceeded" );
        }
    }
    
    private long nextIdFromDefragList()
    {
        if ( defragedIdList.size() > 0 )
        {
            long id = defragedIdList.removeFirst();
            if ( haveMore && defragedIdList.size() == 0 )
            {
                readIdBatch();
            }
            defraggedIdCount--;
            return id;
        }
        return -1;
    }

    private void assertStillOpen()
    {
        if ( fileChannel == null )
        {
            throw new IllegalStateException( "Closed id generator " + fileName );
        }
    }
    
    public IdRange nextIdBatch( int size )
    {
        assertStillOpen();
        
        // Get from defrag list
        int count = 0;
        long[] defragIds = new long[size];
        while ( count < size )
        {
            long id = nextIdFromDefragList();
            if ( id == -1 )
            {
                break;
            }
            defragIds[count++] = id;
        }
        
        // Shrink the array to actual size
        long[] tmpArray = defragIds;
        defragIds = new long[count];
        System.arraycopy( tmpArray, 0, defragIds, 0, count );
        
        int sizeLeftForRange = size-count;
        long start = nextFreeId.get();
        long newHighId = start + sizeLeftForRange;
        assertIdWithinCapacity( newHighId );
        nextFreeId.set( newHighId );
        return new IdRange( defragIds, start, sizeLeftForRange );
    }

    /**
     * Sets the next free "high" id. This method should be called when an id
     * generator has been rebuilt. {@code id} must not be higher than {@code max}.
     * 
     * @param id
     *            The next free id
     */
    public void setHighId( long id )
    {
        assertIdWithinCapacity( id );
        nextFreeId.set( id );
    }

    /**
     * Returns the next "high" id that will be returned if no defragged ids
     * exist.
     * 
     * @return The next free "high" id
     */
    public long getHighId()
    {
        return nextFreeId.get();
    }

    /**
     * Frees the <CODE>id</CODE> making it a defragged id that will be
     * returned by next id before any new id (that hasn't been used yet) is
     * returned.
     * <p>
     * This method will throw an <CODE>IOException</CODE> if id is negative or
     * if id is greater than the highest returned id. However as stated in the
     * class documentation above the id isn't validated to see if it really is
     * free.
     * 
     * @param id
     *            The id to be made available again
     * @throws IOException
     *             If id is negative or greater than the highest returned id
     */
    public synchronized void freeId( long id )
    {
        if ( id == INTEGER_MINUS_ONE )
        {
            return;
        }
        
        if ( fileChannel == null )
        {
            throw new IllegalStateException( "Generator closed " + fileName );
        }
        if ( id < 0 || id >= nextFreeId.get() )
        {
            throw new IllegalArgumentException( "Illegal id[" + id + "]" );
        }
        releasedIdList.add( id );
        defraggedIdCount++;
        if ( releasedIdList.size() >= grabSize )
        {
            writeIdBatch();
        }
    }

    /**
     * Closes the id generator flushing defragged ids in memory to file. The
     * file will be truncated to the minimal size required to hold all defragged
     * ids and it will be marked as clean (not sticky).
     * <p>
     * An invoke to the <CODE>nextId</CODE> or <CODE>freeId</CODE> after
     * this method has been invoked will result in an <CODE>IOException</CODE>
     * since the highest returned id has been set to a negative value.
     * 
     * @throws IOException
     *             If unable to close this id generator
     */
    public synchronized void close()
    {
        if ( nextFreeId.get() == -1 )
        {
            return;
        }

        // write out lists
        if ( releasedIdList.size() > 0 )
        {
            writeIdBatch();
        }
        if ( defragedIdList.size() > 0 )
        {
            while ( defragedIdList.size() > 0 )
            {
                releasedIdList.add( defragedIdList.removeFirst() );
            }
            writeIdBatch();
        }

        // write header
        try
        {
            fileChannel.position( 0 );
            ByteBuffer buffer = ByteBuffer.allocate( HEADER_SIZE );
            buffer.put( STICKY_GENERATOR ).putLong( nextFreeId.get() );
            buffer.flip();
            fileChannel.write( buffer );
            // move data to remove fragmentation in file
            if ( totalBytesRead > HEADER_SIZE )
            {
                long writePosition = HEADER_SIZE;
                long readPosition = readBlocksTo;
                if ( totalBytesRead < readBlocksTo )
                {
                    readPosition = totalBytesRead;
                }
                int bytesRead = -1;
                do
                {
                    writeBuffer.clear();
                    fileChannel.position( readPosition );
                    bytesRead = fileChannel.read( writeBuffer );
                    readPosition += bytesRead;
                    writeBuffer.flip();
                    fileChannel.position( writePosition );
                    writePosition += fileChannel.write( writeBuffer );
                }
                while ( bytesRead > 0 );
                // truncate
                fileChannel.truncate( writePosition );
            }
            // flush
            fileChannel.force( false );
            // remove sticky
            buffer.clear();
            buffer.put( CLEAN_GENERATOR );
            buffer.limit( 1 );
            buffer.flip();
            fileChannel.position( 0 );
            fileChannel.write( buffer );
            // flush and close
            fileChannel.force( false );
            fileChannel.close();
            fileChannel = null;
            // make this generator unusable
            nextFreeId.set( -1 );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( 
                "Unable to close id generator " + fileName, e );
        }
    }

    /**
     * Returns the file associated with this id generator.
     * 
     * @return The id generator's file name
     */
    public String getFileName()
    {
        return this.fileName;
    }

    /**
     * Creates a new id generator.
     * 
     * @param fileName
     *            The name of the id generator
     * @throws IOException
     *             If unable to create the id generator
     */
    public static void createGenerator( String fileName )
    {
        // sanity checks
        if ( fileName == null )
        {
            throw new IllegalArgumentException( "Null filename" );
        }
        File file = new File( fileName );
        if ( file.exists() )
        {
            throw new IllegalStateException( "Can't create IdGeneratorFile["
                + fileName + "], file already exists" );
        }
        try
        {
            FileChannel channel = new FileOutputStream( fileName ).getChannel();
            // write the header
            ByteBuffer buffer = ByteBuffer.allocate( HEADER_SIZE );
            buffer.put( CLEAN_GENERATOR ).putLong( 0 ).flip();
            channel.write( buffer );
            channel.force( false );
            channel.close();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( 
                "Unable to create id generator" + fileName, e );
        }
    }

    // initialize the id generator and performs a simple validation
    private synchronized void initGenerator()
    {
        try
        {
            fileChannel = new RandomAccessFile( fileName, "rw" ).getChannel();
            ByteBuffer buffer = ByteBuffer.allocate( HEADER_SIZE );
            totalBytesRead = fileChannel.read( buffer );
            if ( totalBytesRead != HEADER_SIZE )
            {
                fileChannel.close();
                throw new InvalidIdGeneratorException(
                    "Unable to read header, bytes read: " + totalBytesRead );
            }
            buffer.flip();
            byte storageStatus = buffer.get();
            if ( storageStatus != CLEAN_GENERATOR )
            {
                fileChannel.close();
                throw new InvalidIdGeneratorException( "Sticky generator[ "
                    + fileName
                    + "] delete this id generator and build a new one" );
            }
            this.nextFreeId.set( buffer.getLong() );
            buffer.flip();
            buffer.put( STICKY_GENERATOR ).limit( 1 ).flip();
            fileChannel.position( 0 );
            fileChannel.write( buffer );
            fileChannel.position( HEADER_SIZE );
            readBlocksTo = fileChannel.size();
            defraggedIdCount = (int) (readBlocksTo - HEADER_SIZE) / 8;
            readIdBatch();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( 
                "Unable to init id generator " + fileName, e );
        }
    }

    private void readIdBatch()
    {
        if ( !haveMore )
        {
            return;
        }
        if ( totalBytesRead >= readBlocksTo )
        {
            haveMore = false;
            return;
        }
        try
        {
            if ( totalBytesRead + readBuffer.capacity() > readBlocksTo )
            {
                readBuffer.clear();
                readBuffer
                    .limit( (int) (readBlocksTo - fileChannel.position()) );
            }
            else
            {
                readBuffer.clear();
            }
            fileChannel.position( totalBytesRead );
            int bytesRead = fileChannel.read( readBuffer );
            assert fileChannel.position() <= readBlocksTo;
            totalBytesRead += bytesRead;
            readBuffer.flip();
            assert (bytesRead % 8) == 0;
            int idsRead = bytesRead / 8;
            defraggedIdCount -= idsRead;
            for ( int i = 0; i < idsRead; i++ )
            {
                long id = readBuffer.getLong();
                if ( id != INTEGER_MINUS_ONE )
                {
                    defragedIdList.add( id );
                }
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException(
                "Failed reading defragged id batch", e );
        }
    }

    // writes a batch of defragged ids to file
    private void writeIdBatch()
    {
        // position at end
        try
        {
            fileChannel.position( fileChannel.size() );
            writeBuffer.clear();
            while ( releasedIdList.size() > 0 )
            {
                long id = releasedIdList.removeFirst();
                if ( id == INTEGER_MINUS_ONE )
                {
                    continue;
                }
                writeBuffer.putLong( id );
                if ( writeBuffer.position() == writeBuffer.capacity() )
                {
                    writeBuffer.flip();
                    fileChannel.write( writeBuffer );
                    writeBuffer.clear();
                }
            }
            writeBuffer.flip();
            fileChannel.write( writeBuffer );
            // position for next readIdBatch
            fileChannel.position( totalBytesRead );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( 
                "Unable to write defragged id " + " batch", e );
        }
    }

    /**
     * Utility method that will dump all defragged id's and the "high id" to
     * console. Do not call while running store using this id generator since it
     * could corrupt the id generator (not thread safe). This method will close
     * the id generator after being invoked.
     * 
     * @throws IOException
     *             If problem dumping free ids
     */
    public synchronized void dumpFreeIds()
    {
        while ( haveMore )
        {
            readIdBatch();
        }
        java.util.Iterator<Long> itr = defragedIdList.iterator();
        while ( itr.hasNext() )
        {
            System.out.print( " " + itr.next() );
        }
        System.out.println( "\nNext free id: " + nextFreeId );
        close();
    }

    public synchronized long getNumberOfIdsInUse()
    {
        return nextFreeId.get() - defraggedIdCount;
    }

    public long getDefragCount()
    {
        return defraggedIdCount;
    }
    
    public void clearFreeIds()
    {
        releasedIdList.clear();
        defragedIdList.clear();
        defraggedIdCount = -1;
        try
        {
            truncateFile( fileChannel, HEADER_SIZE );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
