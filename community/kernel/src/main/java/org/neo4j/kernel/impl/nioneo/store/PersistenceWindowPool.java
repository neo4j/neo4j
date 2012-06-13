/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages {@link PersistenceWindow persistence windows} for a store. Each store
 * can configure how much memory it has for
 * {@link MappedPersistenceWindow memory mapped windows}. This class tries to
 * make the most efficient use of those windows by allocating them in such a way
 * that the most frequently used records/blocks (be it for read or write
 * operations) are encapsulated by a memory mapped persistence window.
 */
public class PersistenceWindowPool
{
    private static final int MAX_BRICK_COUNT = 100000;

    private final String storeName;
    // == recordSize
    private final int blockSize;
    private FileChannel fileChannel;
    private final Map<Integer,PersistenceRow> activeRowWindows =
        new HashMap<Integer,PersistenceRow>();
    private long availableMem = 0;
    private long memUsed = 0;
    private int brickCount = 0;
    private int brickSize = 0;
    private BrickElement brickArray[] = new BrickElement[0];
    private int brickMiss = 0;

    private static Logger log = Logger.getLogger( PersistenceWindowPool.class
        .getName() );
    private static final int REFRESH_BRICK_COUNT = 50000;
    private final FileChannel.MapMode mapMode;

    private int hit = 0;
    private int miss = 0;
    private int switches = 0;
    private int ooe = 0;
    private boolean useMemoryMapped = true;

    private final boolean readOnly;

    /**
     * Create new pool for a store.
     *
     * @param storeName
     *            Name of store that use this pool
     * @param blockSize
     *            The size of each record/block in the store
     * @param fileChannel
     *            A fileChannel to the store
     * @param mappedMem
     *            Number of bytes dedicated to memory mapped windows
     * @throws IOException
     *             If unable to create pool
     */
    public PersistenceWindowPool( String storeName, int blockSize,
        FileChannel fileChannel, long mappedMem,
        boolean useMemoryMappedBuffers, boolean readOnly )
    {
        this.storeName = storeName;
        this.blockSize = blockSize;
        this.fileChannel = fileChannel;
        this.availableMem = mappedMem;
        this.useMemoryMapped = useMemoryMappedBuffers;
        this.readOnly = readOnly;
        this.mapMode = readOnly ? MapMode.READ_ONLY : MapMode.READ_WRITE;
        setupBricks();
        dumpStatus();
    }

    /**
     * Acquires a windows for <CODE>position</CODE> and <CODE>operationType</CODE>
     * locking the window preventing other threads from using it.
     *
     * @param position
     *            The position the needs to be encapsulated by the window
     * @param operationType
     *            The type of operation (READ or WRITE)
     * @return A locked window encapsulating the position
     * @throws IOException
     *             If unable to acquire the window
     */
    public PersistenceWindow acquire( long position, OperationType operationType )
    {
        LockableWindow window = null;
        boolean readPos = false;
//        synchronized ( activeRowWindows )
//        {
            if ( brickMiss >= REFRESH_BRICK_COUNT )
            {
//                brickMiss = 0;
                refreshBricks();
            }
            if ( brickSize > 0 )
            {
                int brickIndex = (int) (position * blockSize / brickSize);
                if ( brickIndex < brickArray.length )
                {
                    synchronized ( this )
                    {
                        window = brickArray[brickIndex].getWindow();
                        if ( window != null )
                        {
                            window.mark();
                        }
                    }
                    // assert window == null || window.encapsulates( position );
                    brickArray[brickIndex].setHit();
                }
                else
                {
                    expandBricks( brickIndex + 1 );
                    window = brickArray[brickIndex].getWindow();
                }
            }
            if ( window == null )
            {
                synchronized ( this )
                {
                    miss++;
                    brickMiss++;

                    PersistenceRow dpw = activeRowWindows.get( (int) position );

                    if ( dpw == null )
                    {
                        dpw = new PersistenceRow( position, blockSize,
                            fileChannel );
                    }
                    if ( operationType == OperationType.READ )
                    {
                        readPos = true;
                    }
                    window = dpw;
                    activeRowWindows.put( (int) position, dpw );
                    window.mark();
                }
            }
            else
            {
                hit++;
            }
//        }
        window.lock();
        if ( readPos )
        {
            ((PersistenceRow) window).readPosition();
        }
        window.setOperationType( operationType );
        return window;
    }

    void dumpStatistics()
    {
        log.finest( storeName + " hit=" + hit + " miss=" + miss + " switches="
            + switches + " ooe=" + ooe );
    }

    /**
     * Releases a window used for an operation back to the pool and unlocks it
     * so other threads may use it.
     *
     * @param window
     *            The window to be released
     * @throws IOException
     *             If unable to release window
     */
    public void release( PersistenceWindow window )
    {
        if ( window instanceof PersistenceRow )
        {
            PersistenceRow dpw = (PersistenceRow) window;
            dpw.writeOut();
            synchronized ( this )
            {
                if ( dpw.getWaitingThreadsCount() == 0 && !dpw.isMarked() )
                {
                    int key = (int) dpw.position();
                    activeRowWindows.remove( key );
                }
            }
            dpw.unLock();
        }
        else
        {
            ((LockableWindow) window).unLock();
        }
    }

    synchronized void close()
    {
        flushAll();
//        synchronized ( activeRowWindows )
//        {
            for ( BrickElement element : brickArray )
            {
                if ( element.getWindow() != null )
                {
                    element.getWindow().close();
                    element.setWindow( null );
                }
            }
            fileChannel = null;
            activeRowWindows.clear();
//        }
        // activeRowWindows = null;
        dumpStatistics();
    }

    void flushAll()
    {
        if ( readOnly ) return;

        //        synchronized ( activeRowWindows )
//        {
            for ( BrickElement element : brickArray )
            {
                PersistenceWindow window = element.getWindow();
                if ( window != null )
                {
                    window.force();
                }
            }
//        }
        try
        {
            fileChannel.force( false );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException(
                "Failed to flush file channel " + storeName, e );
        }
    }

    private static class BrickElement
    {
        private final int index;
        private int hitCount;
        private LockableWindow window = null;

        BrickElement( int index )
        {
            this.index = index;
        }

        void setWindow( LockableWindow window )
        {
            this.window = window;
        }

        LockableWindow getWindow()
        {
            return window;
        }

        int index()
        {
            return index;
        }

        void setHit()
        {
            hitCount += 10;
            if ( hitCount < 0 )
            {
                hitCount -= 10;
            }
        }

        int getHit()
        {
            return hitCount;
        }

        void refresh()
        {
            if ( window == null )
            {
                hitCount /= 1.25;
            }
            else
            {
                hitCount /= 1.15;
            }
        }

        @Override
        public String toString()
        {
            return "" + hitCount + (window == null ? "x" : "o");
        }
    }

    private void setupBricks()
    {
        long fileSize = -1;
        try
        {
            fileSize = fileChannel.size();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException(
                "Unable to get file size for " + storeName, e );
        }
        if ( blockSize == 0 )
        {
            return;
        }
        // If we can't fit even 10 blocks in available memory don't even try
        // to use available memory.
        if ( availableMem > 0 && availableMem < blockSize * 10l )
        {
            logWarn( "Unable to use " + availableMem
                + "b as memory mapped windows, need at least " + blockSize * 10
                + "b (block size * 10)" );
            logWarn( "Memory mapped windows have been turned off" );
            availableMem = 0;
            brickCount = 0;
            brickSize = 0;
            return;
        }
        if ( availableMem > 0 && fileSize > 0 )
        {
            double ratio = (availableMem + 0.0d) / fileSize;
            if ( ratio >= 1 )
            {
                brickSize = (int) (availableMem / 1000);
                if ( brickSize < 0 )
                {
                    brickSize = Integer.MAX_VALUE;
                }
                brickSize = (brickSize / blockSize) * blockSize;
                brickCount = (int) (fileSize / brickSize);
            }
            else
            {
                brickCount = (int) (1000.0d / ratio);
                if ( brickCount > MAX_BRICK_COUNT )
                {
                    brickCount = MAX_BRICK_COUNT;
                }
                if ( fileSize / brickCount > availableMem )
                {
                    logWarn( "Unable to use " + (availableMem / 1024)
                        + "kb as memory mapped windows, need at least "
                        + (fileSize / brickCount / 1024) + "kb" );
                    logWarn( "Memory mapped windows have been turned off" );
                    availableMem = 0;
                    brickCount = 0;
                    brickSize = 0;
                    return;
                }
                brickSize = (int) (fileSize / brickCount);
                if ( brickSize < 0 )
                {
                    brickSize = Integer.MAX_VALUE;
                    brickSize = (brickSize / blockSize) * blockSize;
                    brickCount = (int) (fileSize / brickSize);
                }
                else
                {
                    brickSize = (brickSize / blockSize) * blockSize;
                }
                assert brickSize > blockSize;
            }
        }
        else if ( availableMem > 0 )
        {
            brickSize = (int) (availableMem / 100);
            if ( brickSize < 0 )
            {
                brickSize = Integer.MAX_VALUE;
            }
            brickSize = (brickSize / blockSize) * blockSize;
        }
        brickArray = new BrickElement[brickCount];
        for ( int i = 0; i < brickCount; i++ )
        {
            BrickElement element = new BrickElement( i );
            brickArray[i] = element;
        }
    }

    private synchronized void freeWindows( int nr )
    {
        if ( brickSize <= 0 )
        {
            // memory mapped turned off
            return;
        }
        ArrayList<BrickElement> mappedBricks = new ArrayList<BrickElement>();
        for ( int i = 0; i < brickCount; i++ )
        {
            BrickElement be = brickArray[i];
            if ( be.getWindow() != null )
            {
                mappedBricks.add( be );
            }
        }
        Collections.sort( mappedBricks, new BrickSorter() );
        for ( int i = 0; i < nr && i < mappedBricks.size(); i++ )
        {
            BrickElement mappedBrick = mappedBricks.get( i );
            LockableWindow window = mappedBrick.getWindow();
            if ( window.getWaitingThreadsCount() == 0 && !window.isMarked() )
            {
                if ( !readOnly ) window.writeOut();
                mappedBrick.setWindow( null );
                memUsed -= brickSize;
            }
        }
    }

    private synchronized void refreshBricks()
    {
        if ( brickMiss < REFRESH_BRICK_COUNT )
        {
            return;
        }
        brickMiss = 0;
        if ( brickSize <= 0 )
        {
            // memory mapped turned off
            return;
        }
        ArrayList<BrickElement> nonMappedBricks = new ArrayList<BrickElement>();
        ArrayList<BrickElement> mappedBricks = new ArrayList<BrickElement>();
        for ( int i = 0; i < brickCount; i++ )
        {
            BrickElement be = brickArray[i];
            if ( be.getWindow() != null )
            {
                mappedBricks.add( be );
            }
            else
            {
                nonMappedBricks.add( be );
            }
            be.refresh();
        }
        Collections.sort( nonMappedBricks, new BrickSorter() );
        Collections.sort( mappedBricks, new BrickSorter() );
        int mappedIndex = 0;
        int nonMappedIndex = nonMappedBricks.size() - 1;
        // fill up unused memory
        while ( memUsed + brickSize <= availableMem && nonMappedIndex >= 0 )
        {
            BrickElement nonMappedBrick = nonMappedBricks.get(
                nonMappedIndex-- );
            if ( nonMappedBrick.getHit() == 0 )
            {
                return;
            }
            try
            {
                nonMappedBrick.setWindow(
                    allocateNewWindow( nonMappedBrick.index() ) );
                memUsed += brickSize;
            }
            catch ( MappedMemException e )
            {
                ooe++;
                logWarn( "Unable to memory map", e );
            }
            catch ( OutOfMemoryError e )
            {
                ooe++;
                logWarn( "Unable to allocate direct buffer", e );
            }
        }

        // switch bad mappings
        while ( nonMappedIndex >= 0 && mappedIndex < mappedBricks.size() )
        {
            BrickElement mappedBrick = mappedBricks.get( mappedIndex++ );
            BrickElement nonMappedBrick = nonMappedBricks
                .get( nonMappedIndex-- );
            if ( mappedBrick.getHit() >= nonMappedBrick.getHit() )
            {
                break;
            }
            LockableWindow window = mappedBrick.getWindow();
            if ( window.getWaitingThreadsCount() == 0 && !window.isMarked() )
            {
                if ( !readOnly ) window.writeOut();
                mappedBrick.setWindow( null );
                memUsed -= brickSize;
                try
                {
                    nonMappedBrick.setWindow(
                        allocateNewWindow( nonMappedBrick.index() ) );
                    memUsed += brickSize;
                    switches++;
                }
                catch ( MappedMemException e )
                {
                    ooe++;
                    logWarn( "Unable to memory map" );
                }
                catch ( OutOfMemoryError e )
                {
                    ooe++;
                    logWarn( "Unable to allocate direct buffer" );
                }
            }
        }
    }

    private synchronized void expandBricks( int newBrickCount )
    {
        if ( newBrickCount > brickCount )
        {
            BrickElement tmpArray[] = new BrickElement[newBrickCount];
            System.arraycopy( brickArray, 0, tmpArray, 0, brickArray.length );
            if ( memUsed + brickSize >= availableMem )
            {
                freeWindows( 1 );
            }
            for ( int i = brickArray.length; i < tmpArray.length; i++ )
            {
                BrickElement be = new BrickElement( i );
                tmpArray[i] = be;
                if ( memUsed + brickSize <= availableMem )
                {
                    try
                    {
                        be.setWindow( allocateNewWindow( i ) );
                        memUsed += brickSize;
                    }
                    catch ( MappedMemException e )
                    {
                        ooe++;
                        logWarn( "Unable to memory map" );
                    }
                    catch ( OutOfMemoryError e )
                    {
                        ooe++;
                        logWarn( "Unable to allocate direct buffer" );
                    }
                }
            }
            brickArray = tmpArray;
            brickCount = tmpArray.length;
        }
    }

    private LockableWindow allocateNewWindow( long brick )
    {
        if ( useMemoryMapped )
        {
             return new MappedPersistenceWindow(
                brick * brickSize / blockSize, blockSize,
                brickSize, fileChannel, mapMode );
        }
        PlainPersistenceWindow dpw =
            new PlainPersistenceWindow(
                brick * brickSize / blockSize,
                blockSize, brickSize, fileChannel );
        dpw.readPosition();
        return dpw;
    }

    static class BrickSorter implements Comparator<BrickElement>, Serializable
    {
        public int compare( BrickElement o1, BrickElement o2 )
        {
            return o1.getHit() - o2.getHit();
        }

        @Override
        public boolean equals( Object o )
        {
            if ( o instanceof BrickSorter )
            {
                return true;
            }
            return false;
        }

        @Override
        public int hashCode()
        {
            return 7371;
        }
    }

    private void dumpStatus()
    {
        try
        {
            log.fine( "[" + storeName + "] brickCount=" + brickCount
                + " brickSize=" + brickSize + "b mappedMem=" + availableMem
                + "b (storeSize=" + fileChannel.size() + "b)" );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException(
                "Unable to get file size for " + storeName, e );
        }
    }

    private void logWarn( String logMessage )
    {
        log.warning( "[" + storeName + "] " + logMessage );
    }

    private void logWarn( String logMessage, Throwable cause )
    {
        log.log( Level.WARNING, "[" + storeName + "] " + logMessage, cause );
    }

    WindowPoolStats getStats()
    {
        return new WindowPoolStats( storeName, availableMem, memUsed, brickCount,
                brickSize, hit, miss, ooe );
    }
}