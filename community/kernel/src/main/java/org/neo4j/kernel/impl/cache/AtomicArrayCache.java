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

public class AtomicArrayCache<E extends EntityWithSize> implements Cache<E>, DiagnosticsProvider
{
    public static final long MIN_SIZE = 1;
    public final AtomicReferenceArray<E> cache;
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

    private final StringLogger logger;

    public AtomicArrayCache( long maxSizeInBytes, float arrayHeapFraction )
    {
        this( maxSizeInBytes, arrayHeapFraction, 5000, null, StringLogger.SYSTEM );
    }

    public AtomicArrayCache( long maxSizeInBytes, float arrayHeapFraction, long minLogInterval, String name, StringLogger logger )
    {
        this.minLogInterval = minLogInterval;
        if ( arrayHeapFraction < 1 || arrayHeapFraction > 10 )
        {
            throw new IllegalArgumentException(
                                                "The heap fraction used by an array cache must be between 1% and 10%, not "
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
        if ( !cache.compareAndSet( pos, oldObj, obj ) )
        {
            put( obj );
        }
        else
        {
            long objectSize = obj.size();
            if ( objectSize < 1 )
            {
                throw new RuntimeException( "" + objectSize );
            }
            currentSize.addAndGet( objectSize );
            if ( oldObj != null )
            {
                currentSize.addAndGet( oldObj.size() * -1 );
                if ( oldObj.getId() != obj.getId() )
                {
                    collisions++;
                }
            }
            totalPuts++;
            if ( currentSize.get() > maxSize )
            {
                purgeFrom( pos );
            }
        }
    }

    public E remove( long id )
    {
        if ( id < 0 )
        {
            throw new IllegalArgumentException( "Can not remove enity with negative id (" + id + ")" );
        }
        int pos = getPosition( id );
        E obj = cache.get(pos);
        if ( obj != null )
        {
            if ( !cache.compareAndSet( pos, obj, null ) )
            {
                remove( id );
            }
            else
            {
                currentSize.addAndGet( obj.size() * -1 );
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

                logger.logMessage( " purge (nr " + purgeCount + ") " + sizeBeforeStr + " -> " + sizeAfterStr + " (" + diffStr +
                        ") " + missPercentage + " misses, " + colPercentage + " collisions.", true );
            }
        }
    }

    public void printStatistics()
    {
        logStatistics( logger );
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

    private void logStatistics( @SuppressWarnings( "hiding" ) StringLogger logger )
    {
        String currentSizeStr = getSize( currentSize.get() );

        String missPercentage =  ((float) missCount / (float) (hitCount+missCount) * 100.0f) + "%";
        String colPercentage = ((float) collisions / (float) totalPuts * 100.0f) + "%";

        logger.logMessage( " array size: " + cache.length() + " purge count: " + purgeCount + " size is: " + currentSizeStr + ", " +
                missPercentage + " misses, " + colPercentage + " collisions.", true );
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
        for ( int i = 0; i < cache.length(); i++ )
        {
            cache.set( i, null );
        }
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
    public void updateSize( E obj, int sizeBefore, int sizeAfter )
    {
        int pos = getPosition( obj );
        E oldObj = cache.get(pos);
        if ( oldObj != obj )
        {
            return;
        }
        if ( !cache.compareAndSet( pos, oldObj, obj ) )
        {
            put( obj );
        }
        else
        {
            currentSize.addAndGet( (sizeAfter - sizeBefore) );
            if ( currentSize.get() > maxSize )
            {
                purgeFrom( pos );
            }
        }
    }
}