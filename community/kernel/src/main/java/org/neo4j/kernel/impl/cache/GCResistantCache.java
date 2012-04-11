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
package org.neo4j.kernel.impl.cache;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.info.DiagnosticsPhase;
import org.neo4j.kernel.info.DiagnosticsProvider;

public class GCResistantCache<E extends EntityWithSize> implements Cache<E>, DiagnosticsProvider
{
    public static final long MIN_SIZE = 1;
    private final AtomicReferenceArray<E> cache;
    private final long maxSize;
    private final AtomicLong currentSize = new AtomicLong( 0 );
    private final long minLogInterval;
    private final String name;

    // non thread safe, only ~statistics (atomic update will affect performance)
    private long hitCount = 0;
    private long missCount = 0;
    private long totalPuts = 0;
    private long collisions = 0;
    private long purgeCount = 0;
    private AtomicLong highestIdSet = new AtomicLong();

    private final StringLogger logger;

    GCResistantCache( AtomicReferenceArray<E> cache )
    {
        this.cache = cache;
        this.minLogInterval = Long.MAX_VALUE;
        this.maxSize = 1024l*1024*1024;
        this.name = "test cache";
        this.logger = null;
    }
    
    public GCResistantCache( long maxSizeInBytes, float arrayHeapFraction, long minLogInterval, String name, StringLogger logger )
    {
        this.minLogInterval = minLogInterval;
        if ( arrayHeapFraction < 1 || arrayHeapFraction > 10 )
        {
            throw new IllegalArgumentException(
                    "The heap fraction used by a GC resistant cache must be between 1% and 10%, not "
                            + arrayHeapFraction + "%" );
        }
        long memToUse = (long)(((double)arrayHeapFraction) * Runtime.getRuntime().maxMemory() / 100);
        long maxElementCount = (int) ( memToUse / 8 );
        if ( memToUse > Integer.MAX_VALUE )
        {
            maxElementCount = Integer.MAX_VALUE;
        }
        if ( maxSizeInBytes < MIN_SIZE )
        {
            throw new IllegalArgumentException( "Max size can not be " + maxSizeInBytes );
        }

        this.cache = new AtomicReferenceArray<E>( (int) maxElementCount );
        this.maxSize = maxSizeInBytes;
        this.name = name == null ? super.toString() : name;
        this.logger = logger == null ? StringLogger.SYSTEM : logger;
    }

    private int getPosition( EntityWithSize obj )
    {
        return (int) ( obj.getId() % cache.length() );
    }

    private int getPosition( long id )
    {
        return (int) ( id % cache.length() );
    }

    private long putTimeStamp = 0;

    public void put( E obj )
    {
        long time = System.currentTimeMillis();
        if ( time - putTimeStamp > minLogInterval )
        {
            putTimeStamp = time;
            printStatistics();
        }
        int pos = getPosition( obj );
        E oldObj = cache.get( pos );
        if ( oldObj != obj )
        {
            int objectSize = obj.size();
            if ( cache.compareAndSet( pos, oldObj, obj ) )
            {
                setHighest( obj.getId() );
                int oldObjSize = 0;
                if ( oldObj != null )
                {
                    oldObjSize = oldObj.getRegisteredSize();
                }
                long size = currentSize.addAndGet( objectSize - oldObjSize );
                obj.setRegisteredSize( objectSize );
                if ( oldObj != null )
                {
                    collisions++;
                }
                totalPuts++;
                if ( size > maxSize )
                {
                    purgeFrom( pos );
                }
            }
        }
    }

    /**
     * Updates the highest set id if the given id is higher than any previously registered id.
     * Helps {@link #clear()} performance wise so that only the used part of the array is cleared.
     * @param id the id just put into the cache.
     */
    private void setHighest( long id )
    {
        while ( true )
        {
            long highest = highestIdSet.get();
            if ( id > highest )
            {
                if ( highestIdSet.compareAndSet( highest, id ) )
                    break;
            }
            else
                break;
        }
    }

    public E remove( long id )
    {
        int pos = getPosition( id );
        E obj = cache.get(pos);
        if ( obj != null )
        {
            if ( cache.compareAndSet( pos, obj, null ) )
            {
                currentSize.addAndGet( obj.getRegisteredSize() * -1 );
            }
        }
        return obj;
    }

    public E get( long id )
    {
        int pos = getPosition( id );
        E obj = cache.get( pos );
        if ( obj != null && obj.getId() == id )
        {
            hitCount++;
            return obj;
        }
        missCount++;
        return null;
    }

    private long lastPurgeLogTimestamp = 0;

    private synchronized void purgeFrom( int pos )
    {
        if ( currentSize.get() <= ( maxSize * 0.95f ) )
        {
            return;
        }
        purgeCount++;
        long sizeBefore = currentSize.get();
        try
        {
            int index = 1;
            do
            {
                if ( ( pos - index ) >= 0 )
                {
                    int minusPos = pos - index;
                    remove( minusPos );
                    if ( currentSize.get() <= ( maxSize * 0.9f ) )
                    {
                        return;
                    }
                }
                if ( ( pos + index ) < cache.length() )
                {
                    int plusPos = pos + index;
                    remove( plusPos );
                    if ( currentSize.get() <= ( maxSize * 0.9f ) )
                    {
                        return;
                    }
                }
                index++;
            }
            while ( ( pos - index ) >= 0 || ( pos + index ) < cache.length() );
            // current object larger than max size, clear it
            remove( pos ); 
        }
        finally
        {
            long timestamp = System.currentTimeMillis();
            if ( timestamp - lastPurgeLogTimestamp > minLogInterval )
            {
                lastPurgeLogTimestamp = timestamp;
                long sizeAfter = currentSize.get();

                String sizeBeforeStr = getSize( sizeBefore );
                String sizeAfterStr = getSize( sizeAfter );
                String diffStr = getSize( sizeBefore - sizeAfter );

                String missPercentage =  ((float) missCount / (float) (hitCount+missCount) * 100.0f) + "%";
                String colPercentage = ((float) collisions / (float) totalPuts * 100.0f) + "%";

                logger.logMessage( name + " purge (nr " + purgeCount + ") " + sizeBeforeStr + " -> " + sizeAfterStr + " (" + diffStr +
                        ") " + missPercentage + " misses, " + colPercentage + " collisions (" + collisions + ").", true );
                printAccurateStatistics();
             }
        }
    }

    private void printAccurateStatistics()
    {
        int elementCount = 0;
        long actualSize = 0;
        long registeredSize = 0;
        for ( int i = 0; i < cache.length(); i++ )
        {
            EntityWithSize obj = cache.get( i );
            if ( obj != null )
            {
                elementCount++;
                actualSize += obj.size();
                registeredSize += obj.getRegisteredSize();
            }
        }
        logger.logMessage( name + " purge (nr " + purgeCount + "): elementCount:" + elementCount + " and sizes actual:" + getSize( actualSize ) + 
                    ", perceived:" + getSize( currentSize.get() ) + " (diff:" + getSize(currentSize.get() - actualSize) + "), registered:" + getSize( registeredSize ), true );
    }

    public void printStatistics()
    {
        logStatistics( logger );
//        printAccurateStatistics();
    }

    @Override
    public String getDiagnosticsIdentifier()
    {
        return getName();
    }

    @Override
    public void acceptDiagnosticsVisitor( Object visitor )
    {
        // accept no visitors.
    }

    @Override
    public void dump( DiagnosticsPhase phase, StringLogger log )
    {
        if (phase.isExplicitlyRequested()) logStatistics(log);
    }

    private void logStatistics( StringLogger log )
    {
        log.logMessage( this.toString(), true );
    }
    
    @Override
    public String toString()
    {
        String currentSizeStr = getSize( currentSize.get() );

        String missPercentage =  ((float) missCount / (float) (hitCount+missCount) * 100.0f) + "%";
        String colPercentage = ((float) collisions / (float) totalPuts * 100.0f) + "%";
        
        return name + " array size: " + cache.length() + " purge count: " + purgeCount + " size is: " + currentSizeStr + ", " +
                missPercentage + " misses, " + colPercentage + " collisions (" + collisions + ").";
    }

    private String getSize( long size )
    {
        if ( size > ( 1024 * 1024 * 1024 ) )
        {
            float value = size / 1024.0f / 1024.0f / 1024.0f;
            return value + "Gb";
        }
        if ( size > ( 1024 * 1024 ) )
        {
            float value = size / 1024.0f / 1024.0f;
            return value + "Mb";
        }
        if ( size > 1024 )
        {
            float value = size / 1024.0f / 1024.0f;
            return value + "kb";
        }
        return size + "b";
    }

    public void clear()
    {
        for ( int i = 0; i <= highestIdSet.get() /*cache.length()*/; i++ )
        {
            cache.set( i, null );
        }
        currentSize.set( 0 );
        highestIdSet.set( 0 );
    }

    public void putAll( Collection<E> objects )
    {
        for ( E obj : objects )
        {
            put( obj );
        }
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public long size()
    {
        return currentSize.get();
    }

    @Override
    public long hitCount()
    {
        return hitCount;
    }

    @Override
    public long missCount()
    {
        return missCount;
    }

    @Override
    public void updateSize( E obj, int newSize )
    {
        int pos = getPosition( obj );
        E existingObj = cache.get( pos );
        if ( existingObj != obj )
        {
            return;
        }
        long size = currentSize.addAndGet( (newSize - existingObj.getRegisteredSize()) );
        obj.setRegisteredSize( newSize );
        if ( size > maxSize )
        {
            purgeFrom( pos );
        }
    }
}