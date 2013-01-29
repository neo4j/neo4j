/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import java.util.concurrent.locks.ReentrantLock;

public class LockStripedCache<E extends EntityWithSize> implements Cache<E>
{
    private final ReentrantLock locks[];
    private final Cache<E> actual;
    private final Loader<E> loader;

    public interface Loader<E>
    {
        E loadById( long id );
    }

    public LockStripedCache( Cache<E> actual, int stripeCount, Loader<E> loader )
    {
        this.loader = loader;
        this.locks = new ReentrantLock[stripeCount];
        this.actual = actual;
        for ( int i = 0; i < locks.length; i++ )
        {
            locks[i] = new ReentrantLock();
        }
    }

    @Override
    public String getName()
    {
        return actual.getName();
    }

    @Override
    public void put( E value )
    {
        actual.put( value );
    }

    @Override
    public E remove( long key )
    {
        return actual.remove( key );
    }

    @Override
    public E get( long key )
    {
        E result = actual.get( key );
        if ( result != null )
        {
            return result;
        }

        ReentrantLock lock = lockId( key );
        try
        {
            result = loader.loadById( key );
            if ( result == null )
            {
                return null;
            }
            actual.put( result );
            return result;
        }
        finally
        {
            lock.unlock();
        }
    }

    public E getIfCached( long key )
    {
        return actual.get( key );
    }

    private ReentrantLock lockId( long id )
    {
        // TODO: Change stripe mod for new 4B+
        int stripe = (int) (id / 32768) % locks.length;
        if ( stripe < 0 )
        {
            stripe *= -1;
        }
        ReentrantLock lock = locks[stripe];
        lock.lock();
        return lock;
    }

    @Override
    public void clear()
    {
        actual.clear();
    }

    @Override
    public long size()
    {
        return actual.size();
    }

    @Override
    public void putAll( Collection values )
    {
        actual.putAll( values );
    }

    @Override
    public long hitCount()
    {
        return actual.hitCount();
    }

    @Override
    public long missCount()
    {
        return actual.missCount();
    }

    @Override
    public void updateSize( E entity, int newSize )
    {
        actual.updateSize( entity, newSize );
    }

    @Override
    public void printStatistics()
    {
        actual.printStatistics();
    }
}
