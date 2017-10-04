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

import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.id.validation.IdValidator;

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
    /**
     * Invalid and reserved id value. Represents special values, f.ex. the end of a relationships/property chain.
     * Please use {@link IdValidator} to validate generated ids.
     */
    public static final long INTEGER_MINUS_ONE = 0xFFFFFFFFL;  // 4294967295L;

    private final long max;
    private final IdContainer idContainer;
    private long highId;

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
     * @param file
     *            The file name (and path if needed) for the id generator to be
     *            opened
     * @param grabSize
     *            The number of defragged ids to keep in memory
     * @param max is the highest possible id to be returned by this id generator from
     * {@link #nextId()}.
     * @param aggressiveReuse will reuse ids during the same session, not requiring
     * a restart to be able reuse ids freed with {@link #freeId(long)}.
     * @param highId A supplier for the high id to be used if the id file is found to be empty or not properly shut down
     * @throws UnderlyingStorageException
     *             If no such file exist or if the id generator is sticky
     */
    public IdGeneratorImpl( FileSystemAbstraction fs, File file, int grabSize, long max, boolean aggressiveReuse,
            Supplier<Long> highId )
    {
        this.max = max;
        this.idContainer = new IdContainer( fs, file, grabSize, aggressiveReuse );
        /*
         * The highId supplier will be called only if the id container tells us that the information found in the
         * id file is not reliable (typically the file had to be created). Calling the supplier can be a potentially
         * expensive operation.
         */
        if ( this.idContainer.init() )
        {
            this.highId = idContainer.getInitialHighId();
        }
        else
        {
            this.highId = highId.get();
        }
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
    @Override
    public synchronized long nextId()
    {
        assertStillOpen();
        long nextDefragId = idContainer.getReusableId();
        if ( nextDefragId != IdContainer.NO_RESULT )
        {
            return nextDefragId;
        }

        if ( IdValidator.isReservedId( highId ) )
        {
            highId++;
        }
        IdValidator.assertValidId( highId, max );
        return highId++;
    }

    @Override
    public synchronized IdRange nextIdBatch( int size )
    {
        assertStillOpen();

        // Get from defrag list
        int count = 0;
        long[] defragIds = new long[size];
        while ( count < size )
        {
            long id = idContainer.getReusableId();
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

        int sizeLeftForRange = size - count;
        long start = highId;
        setHighId( start + sizeLeftForRange );
        return new IdRange( defragIds, start, sizeLeftForRange );
    }

    /**
     * Sets the next free "high" id. This method should be called when an id
     * generator has been rebuilt. {@code id} must not be higher than {@code max}.
     *
     * @param id The next free id returned from {@link #nextId()} if there are no existing free ids.
     */
    @Override
    public synchronized void setHighId( long id )
    {
        IdValidator.assertIdWithinCapacity( id, max );
        highId = id;
    }

    /**
     * Returns the next "high" id that will be returned if no defragged ids
     * exist.
     *
     * @return The next free "high" id
     */
    @Override
    public synchronized long getHighId()
    {
        return highId;
    }

    @Override
    public synchronized long getHighestPossibleIdInUse()
    {
        return highId - 1;
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
     */
    @Override
    public synchronized void freeId( long id )
    {
        idContainer.assertStillOpen();

        if ( IdValidator.isReservedId( id ) )
        {
            return;
        }

        if ( id < 0 || id >= highId )
        {
            throw new IllegalArgumentException( "Illegal id[" + id + "], highId is " + highId );
        }
        idContainer.freeId( id );
    }

    /**
     * Closes the id generator flushing defragged ids in memory to file. The
     * file will be truncated to the minimal size required to hold all defragged
     * ids and it will be marked as clean (not sticky).
     * <p>
     * An invoke to the <CODE>nextId</CODE> or <CODE>freeId</CODE> after
     * this method has been invoked will result in an <CODE>IOException</CODE>
     * since the highest returned id has been set to a negative value.
     */
    @Override
    public synchronized void close()
    {
        idContainer.close( highId );
    }

    /**
     * Creates a new id generator.
     *
     * @param fileName The name of the id generator
     * @param throwIfFileExists if {@code true} will cause an {@link UnderlyingStorageException} to be thrown if
     * the file already exists. if {@code false} will truncate the file writing the header in it.
     */
    public static void createGenerator( FileSystemAbstraction fs, File fileName, long highId,
                                        boolean throwIfFileExists )
    {
        IdContainer.createEmptyIdFile( fs, fileName, highId, throwIfFileExists );
    }

    public static long readHighId( FileSystemAbstraction fileSystem, File file ) throws IOException
    {
        return IdContainer.readHighId( fileSystem, file );
    }

    @Override
    public synchronized long getNumberOfIdsInUse()
    {
        return highId - getDefragCount();
    }

    @Override
    public synchronized long getDefragCount()
    {
        return idContainer.getFreeIdCount();
    }

    @Override
    public synchronized void delete()
    {
        idContainer.delete();
    }

    private void assertStillOpen()
    {
        idContainer.assertStillOpen();
    }

    @Override
    public String toString()
    {
        return "IdGeneratorImpl " + hashCode() + " [max=" + max + ", idContainer=" + idContainer + "]";
    }
}
