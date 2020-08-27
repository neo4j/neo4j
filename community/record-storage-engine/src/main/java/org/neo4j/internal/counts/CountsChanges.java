/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.counts;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.neo4j.util.Preconditions;

class CountsChanges
{
    static final long ABSENT = -1;

    private final ConcurrentHashMap<CountsKey,AtomicLong> changes = new ConcurrentHashMap<>();
    private volatile ConcurrentHashMap<CountsKey,AtomicLong> previousChanges;
    private volatile boolean frozen;

    CountsChanges()
    {
    }

    private CountsChanges( ConcurrentHashMap<CountsKey,AtomicLong> previousChanges )
    {
        this.previousChanges = previousChanges;
    }

    CountsChanges fork()
    {
        this.frozen = true;
        return new CountsChanges( changes );
    }

    void clearPreviousChanges()
    {
        this.previousChanges = null;
    }

    void add( CountsKey key, long delta, Function<CountsKey,AtomicLong> defaultToStoredCount )
    {
        Preconditions.checkState( !frozen, "Can't make changes in a frozen state" );
        getCounter( key, defaultToStoredCount ).addAndGet( delta );
    }

    private AtomicLong getCounter( CountsKey key, Function<CountsKey,AtomicLong> defaultToStoredCount )
    {
        ConcurrentHashMap<CountsKey,AtomicLong> prev = previousChanges;
        Function<CountsKey,AtomicLong> defaultFunction = prev == null ? defaultToStoredCount : k ->
        {
            AtomicLong prevCount = prev.get( k );
            if ( prevCount != null )
            {
                return new AtomicLong( prevCount.get() );
            }
            return defaultToStoredCount.apply( k );
        };
        return changes.computeIfAbsent( key, defaultFunction );
    }

    Iterable<Map.Entry<CountsKey,AtomicLong>> sortedChanges( Comparator<CountsKey> comparator )
    {
        List<Map.Entry<CountsKey,AtomicLong>> changeList = new ArrayList<>( changes.entrySet() );
        changeList.sort( ( e1, e2 ) -> comparator.compare( e1.getKey(), e2.getKey() ) );

        return changes.entrySet();
    }

    boolean containsChange( CountsKey key )
    {
        if ( changes.containsKey( key ) )
        {
            return true;
        }
        ConcurrentHashMap<CountsKey,AtomicLong> prev = previousChanges;
        return prev != null && prev.containsKey( key );
    }

    long get( CountsKey key )
    {
        AtomicLong count = changes.get( key );
        if ( count != null )
        {
            return count.get();
        }
        ConcurrentHashMap<CountsKey,AtomicLong> prev = previousChanges;
        if ( prev != null )
        {
            AtomicLong prevCount = prev.get( key );
            if ( prevCount != null )
            {
                return prevCount.get();
            }
        }
        return ABSENT;
    }

    void freeze()
    {
        frozen = true;
    }
}
