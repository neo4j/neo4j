/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.util.collection;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.function.Consumer;
import org.neo4j.function.Factory;
import org.neo4j.helpers.Clock;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static org.neo4j.helpers.Format.duration;

/**
 * A concurrent repository that allows users to manage objects with a specified timeout on idleness.
 * The repository owns the lifecycle of it's objects, granting clients exclusive access to them via its
 * acquire/release methods.
 *
 * The {@link #run()} method here performs one "sweep", checking for idle entries to reap. You will want to trigger
 * that run on a recurring basis, for instance using {@link org.neo4j.kernel.impl.util.JobScheduler}.
 */
public class TimedRepository<KEY, VALUE> implements Runnable
{
    private final ConcurrentMap<KEY, Entry> repo = new ConcurrentHashMap<>();
    private final Factory<VALUE> factory;
    private final Consumer<VALUE> reaper;
    private final long timeout;
    private final Clock clock;

    private class Entry
    {
        static final int IDLE = 0, IN_USE = 1, MARKED_FOR_END = 2;

        private final AtomicInteger state = new AtomicInteger( IDLE );
        private final VALUE value;
        private volatile long latestActivityTimestamp;

        public Entry( VALUE value )
        {
            this.value = value;
            this.latestActivityTimestamp = clock.currentTimeMillis();
        }

        public boolean acquire()
        {
            return state.compareAndSet( IDLE, IN_USE );
        }

        /**
         * Calling this is only allowed if you have previously acquired this entry.
         * @return true if the release was successful, false if this entry has been marked for removal, and thus is not
         * allowed to be released back into the public.
         */
        public boolean release()
        {
            latestActivityTimestamp = clock.currentTimeMillis();
            return state.compareAndSet( IN_USE, IDLE );
        }

        public boolean markForEndingIfInUse()
        {
            return state.compareAndSet( IN_USE, MARKED_FOR_END );
        }

        public boolean isMarkedForEnding()
        {
            return state.get() == MARKED_FOR_END;
        }

        @Override
        public String toString()
        {
            return format( "%s[%s last accessed at %d (%s ago)", getClass().getSimpleName(),
                    value, latestActivityTimestamp, duration( currentTimeMillis()-latestActivityTimestamp ) );
        }
    }

    public TimedRepository( Factory<VALUE> provider, Consumer<VALUE> reaper, long timeout, Clock clock )
    {
        this.factory = provider;
        this.reaper = reaper;
        this.timeout = timeout;
        this.clock = clock;
    }

    public void begin( KEY key ) throws ConcurrentAccessException
    {
        VALUE instance = factory.newInstance();
        Entry existing;
        if ( (existing = repo.putIfAbsent( key, new Entry( instance ) )) != null )
        {
            reaper.accept( instance ); // Need to clear up our optimistically allocated value
            throw new ConcurrentAccessException( String.format(
                    "Cannot begin '%s', because %s with that key already exists.", key, existing ) );
        }
    }

    /**
     * End the life of a stored entry. If the entry is currently in use, it will be thrown out as soon as the other client
     * is done with it.
     */
    public VALUE end( KEY key )
    {
        while(true)
        {
            Entry entry = repo.get( key );
            if ( entry == null )
            {
                return null;
            }

            // Ending the life of an entry is somewhat complicated, because we promise the callee here that if someone
            // else is concurrently using the entry, we will ensure that either we or the other user will end the entry
            // life when the other user is done with it.

            // First, assume the entry is in use and try and mark it to be ended by the other user
            if ( entry.markForEndingIfInUse() )
            {
                // The entry was indeed in use, and we successfully marked it to be ended. That's all we need to do here,
                // the other user will see the ending flag when releasing the entry.
                return entry.value;
            }

            // Marking it for ending failed, likely because the entry is currently idle - lets try and just acquire it
            // and throw it out ourselves
            if ( entry.acquire() )
            {
                // Got it, just throw it away
                end0( key, entry.value );
                return entry.value;
            }

            // We didn't manage to mark this for ending, and we didn't manage to acquire it to end it ourselves, which
            // means either we're racing with another thread using it (and we just need to retry), or it's already
            // marked for ending. In the latter case, we can bail here.
            if ( entry.isMarkedForEnding() )
            {
                // Someone did indeed manage to mark it for ending, which means it will be thrown out (or has already).
                return entry.value;
            }
        }
    }

    public VALUE acquire( KEY key ) throws NoSuchEntryException, ConcurrentAccessException
    {
        Entry entry = repo.get( key );
        if ( entry == null )
        {
            throw new NoSuchEntryException( String.format("Cannot access '%s', no such entry exists.", key) );
        }
        if(entry.acquire())
        {
            return entry.value;
        }
        throw new ConcurrentAccessException( String.format("Cannot access '%s', because another client is currently using it.", key) );
    }

    public void release( KEY key )
    {
        Entry entry = repo.get( key );
        if(entry != null && !entry.release())
        {
            // This happens when another client has asked that this entry be ended while we were using it, leaving us
            // a note to not release the object back to the public, and to end its life when we are done with it.
            end0(key, entry.value);
        }
    }

    public Set<KEY> keys()
    {
        return repo.keySet();
    }

    @Override
    public void run()
    {
        long maxAllowedAge = clock.currentTimeMillis() - timeout;
        for ( KEY key : keys() )
        {
            Entry entry = repo.get( key );
            if ( (entry != null) && (entry.latestActivityTimestamp < maxAllowedAge) )
            {
                if ( (entry.latestActivityTimestamp < maxAllowedAge) && entry.acquire() )
                {
                    end0( key, entry.value );
                }
            }
        }
    }

    private void end0(KEY key, VALUE value)
    {
        repo.remove( key );
        reaper.accept( value );
    }
}
