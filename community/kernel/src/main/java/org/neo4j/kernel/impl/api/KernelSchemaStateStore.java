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
package org.neo4j.kernel.impl.api;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.neo4j.function.Function;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * Used for the actual storage of "schema state".
 * Schema state is transient state that should be invalidated when the schema changes.
 * Examples of things stored in schema state is execution plans for cypher.
 */
public class KernelSchemaStateStore implements UpdateableSchemaState
{
    private Map<Object, Object> state;

    private final Log log;
    private final ReadWriteLock lock = new ReentrantReadWriteLock( true );

    public KernelSchemaStateStore( LogProvider logProvider )
    {
        this.state = new HashMap<>(  );
        this.log = logProvider.getLog( getClass() );
    }

    @SuppressWarnings("unchecked")
    public <K, V> V get(K key)
    {
        lock.readLock().lock();
        try {
            return (V) state.get( key );
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> V getOrCreate(K key, Function<K, V> creator) {
        V currentValue = get(key);
        if (currentValue == null)
        {
            lock.writeLock().lock();
            try {
                V lockedValue = (V) state.get( key );
                if (lockedValue == null)
                {
                    V newValue = creator.apply( key );
                    state.put( key, newValue );
                    return newValue;
                }
                else
                    return lockedValue;
            }
            finally {
                lock.writeLock().unlock();
            }
        }
        else
            return currentValue;
    }

    public void replace(Map<Object, Object> replacement)
    {
        lock.writeLock().lock();
        try {
            state = replacement;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    public <K, V> void apply(Map<K, V> updates)
    {
        lock.writeLock().lock();
        try {
            state.putAll( updates );
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    public void clear()
    {
        lock.writeLock().lock();
        try {
            state.clear();
        }
        finally {
            lock.writeLock().unlock();
        }
        log.info( "Schema state store has been cleared." );
    }
}
