/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.cache;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.info.DiagnosticsPhase;
import org.neo4j.kernel.info.DiagnosticsProvider;

public class HighPerformanceCache<E extends EntityWithSizeObject> extends Cache.Adapter<E>
    implements DiagnosticsProvider
{
    public interface Monitor {
        void purged( long sizeBefore, long sizeAfter, int numberOfEntitiesPurged );
    }

    public static final long MIN_SIZE = 1;
    private final AtomicReferenceArray<E> cache;
    private final long maxSize;
    private long closeToMaxSize;
    private long purgeStopSize;
    private long purgeHandoffSize;
    private final AtomicLong currentSize = new AtomicLong( 0 );
    private final long minLogInterval;
    private final String name;
    private final AtomicLong highestIdSet = new AtomicLong();

    // non thread safe, only ~statistics (atomic update will affect performance)
    private long hitCount = 0;
    private long missCount = 0;
    private long totalPuts = 0;
    private long collisions = 0;
    private long purgeCount = 0;

    private final StringLogger logger;

    private final AtomicBoolean purging = new AtomicBoolean();
    private final AtomicInteger avertedPurgeWaits = new AtomicInteger();
    private final AtomicInteger forcedPurgeWaits = new AtomicInteger();
    private long purgeTime;
    private Monitor monitor;

    HighPerformanceCache( AtomicReferenceArray<E> cache )
    {
        this.cache = cache;
        this.minLogInterval = Long.MAX_VALUE;
        this.maxSize = 1024l*1024*1024;
        this.name = "test cache";
        this.logger = null;
        calculateSizes();
    }

    public HighPerformanceCache( long maxSizeInBytes, float arrayHeapFraction, long minLogInterval, String name,
                                 StringLogger logger, Monitor monitor )
    {
        if ( logger == null )
        {
            throw new IllegalArgumentException( "Null logger" );
        }

        this.minLogInterval = minLogInterval;
        if ( arrayHeapFraction < 1 || arrayHeapFraction > 10 )
        {
            throw new IllegalArgumentException(
                    "The heap fraction used by the High-Performance Cache must be between 1% and 10%, not "
                            + arrayHeapFraction + "%" );
        }
        long memToUse = (long)(((double)arrayHeapFraction) * Runtime.getRuntime().maxMemory() / 100);
        int bytesPerElementFactor = 8;
        long maxElementCount = (int) (memToUse / bytesPerElementFactor);
        if ( memToUse > Integer.MAX_VALUE )
        {
            maxElementCount = Integer.MAX_VALUE / bytesPerElementFactor;
        }
        if ( maxSizeInBytes < MIN_SIZE )
        {
            throw new IllegalArgumentException( "Max size can not be " + maxSizeInBytes );
        }

        this.cache = new AtomicReferenceArray<>( (int) maxElementCount );
        this.maxSize = maxSizeInBytes;
        this.name = name == null ? super.toString() : name;
        this.logger = logger;
        this.monitor = monitor;
        calculateSizes();
    }

    private void calculateSizes()
    {
        this.closeToMaxSize = (long)(maxSize * 0.95d);
        this.purgeStopSize = (long)(maxSize * 0.90d);
        this.purgeHandoffSize = (long)(maxSize * 1.05d);
    }

    protected int getPosition( EntityWithSizeObject obj )
    {
        return (int) ( obj.getId() % cache.length() );
    }

    private int getPosition( long id )
    {
        return (int) ( id % cache.length() );
    }

    private long putTimeStamp = 0;

    @Override
    public E put( E obj, boolean force )
    {
        long time = System.currentTimeMillis();
        if ( time - putTimeStamp > minLogInterval )
        {
            putTimeStamp = time;
            printStatistics();
        }
        int pos = getPosition( obj );
        E oldObj = cache.get( pos );
        while ( oldObj != obj )
        {
            if ( oldObj != null && oldObj.getId() == obj.getId() && !force )
            {   // There's an existing element representing the same entity at this position, return the existing
                return oldObj;
            }

            // Either we're trying to put a new element that doesn't exist at this location an element
            // that doesn't represent the same entity that is already here. In any case, overwrite what's there
            int objectSize = obj.sizeOfObjectInBytesIncludingOverhead();
            if ( cache.compareAndSet( pos, oldObj, obj ) )
            {
                setHighest( pos );
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
                if ( size > closeToMaxSize )
                {
                    purgeFrom( pos );
                }

                // We successfully updated the cache with our new element, break and have it returned.
                break;
            }

            // Someone else put an element here right in front of our very nose
            // Get the element that was just set and have another go
            oldObj = cache.get( pos );
        }
        return obj;
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
                {
                    break;
                }
            }
            else
            {
                break;
            }
        }
    }

    @Override
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

    @Override
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

    private void purgeFrom( int pos )
    {
        long myCurrentSize = currentSize.get();
        if ( myCurrentSize <= closeToMaxSize )
        {
            return;
        }

        // if we're within 0.95 < size < 1.05 and someone else is purging then just return and let
        // the other one purge for us. if we're above 1.05 then wait for the purger to finish before returning.
        if ( purging.compareAndSet( false, true ) )
        {   // We're going to do the purge
            try
            {
                doPurge( pos );
            }
            finally
            {
                purging.set( false );
            }
        }
        else
        {   // Someone else is currently doing a purge
            if ( myCurrentSize < purgeHandoffSize )
            {   // It's safe to just return and let the purger do its thing
                avertedPurgeWaits.incrementAndGet();
            }
            else
            {
                // Wait for the current purge to complete. Some threads might slip through here before
                // A thread just entering doPurge above, but that's fine
                forcedPurgeWaits.incrementAndGet();
                waitForCurrentPurgeToComplete();
            }
        }
    }

    private synchronized void waitForCurrentPurgeToComplete()
    {
        // Just a nice way of saying "wait for the monitor on this object currently held by the thread doing a purge"
    }

    private synchronized void doPurge( int pos )
    {
        long sizeBefore = currentSize.get();
        if ( sizeBefore <= closeToMaxSize )
        {
            return;
        }

        long startTime = System.currentTimeMillis();
        purgeCount++;
        int numberOfEntitiesPurged = 0;
        try
        {
            int index = 1;
            do
            {
                int minusPos = pos - index;
                if ( minusPos >= 0 )
                {
                    if ( remove( minusPos ) != null )
                    {
                        numberOfEntitiesPurged++;
                        if ( currentSize.get() <= purgeStopSize )
                        {
                            return;
                        }
                    }
                }
                int plusPos = pos + index;
                if ( plusPos < cache.length() )
                {
                    if ( remove( plusPos ) != null )
                    {
                        numberOfEntitiesPurged++;
                        if ( currentSize.get() <= purgeStopSize )
                        {
                            return;
                        }
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
            purgeTime += (timestamp-startTime);
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
            monitor.purged( sizeBefore, currentSize.get(), numberOfEntitiesPurged );
        }
    }

    private void printAccurateStatistics()
    {
        int elementCount = 0;
        long actualSize = 0;
        long registeredSize = 0;
        for ( int i = 0; i < cache.length(); i++ )
        {
            EntityWithSizeObject obj = cache.get( i );
            if ( obj != null )
            {
                elementCount++;
                actualSize += obj.sizeOfObjectInBytesIncludingOverhead();
                registeredSize += obj.getRegisteredSize();
            }
        }
        logger.logMessage( name + " purge (nr " + purgeCount + "): elementCount:" + elementCount + " and sizes actual:" + getSize( actualSize ) +
                    ", perceived:" + getSize( currentSize.get() ) + " (diff:" + getSize(currentSize.get() - actualSize) + "), registered:" + getSize( registeredSize ), true );
    }

    @Override
    public void printStatistics()
    {
        if ( logger.isDebugEnabled() )
        {
            logStatistics( logger );
        }
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
        if (phase.isExplicitlyRequested())
        {
            logStatistics(log);
        }
    }

    private void logStatistics( StringLogger log )
    {
        log.debug( this.toString() );
    }

    @Override
    public String toString()
    {
        String currentSizeStr = getSize( currentSize.get() );

        String missPercentage =  ((float) missCount / (float) (hitCount+missCount) * 100.0f) + "%";
        String colPercentage = ((float) collisions / (float) totalPuts * 100.0f) + "%";

        return name + " array:" + cache.length() + " purge:" + purgeCount + " size:" + currentSizeStr +
                " misses:" + missPercentage + " collisions:" + colPercentage + " (" + collisions + ") av.purge waits:" +
                avertedPurgeWaits.get() + " purge waits:" + forcedPurgeWaits.get() + " avg. purge time:" + (purgeCount > 0 ? (purgeTime/purgeCount) + "ms" : "N/A");
    }

    private String getSize( long size )
    {
        if ( size > ( 1024 * 1024 * 1024 ) )
        {
            float value = size / 1024.0f / 1024.0f / 1024.0f;
            return value + "GiB";
        }
        if ( size > ( 1024 * 1024 ) )
        {
            float value = size / 1024.0f / 1024.0f;
            return value + "MiB";
        }
        if ( size > 1024 )
        {
            float value = size / 1024.0f;
            return value + "kiB";
        }
        return size + "B";
    }

    @Override
    public void clear()
    {
        for ( int i = 0; i <= highestIdSet.get() /*cache.length()*/; i++ )
        {
            cache.set( i, null );
        }
        currentSize.set( 0 );
        highestIdSet.set( 0 );
    }

    @Override
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
        long size = currentSize.addAndGet( newSize - existingObj.getRegisteredSize() );
        obj.setRegisteredSize( newSize );
        if ( size > closeToMaxSize )
        {
            purgeFrom( pos );
        }
    }
}
