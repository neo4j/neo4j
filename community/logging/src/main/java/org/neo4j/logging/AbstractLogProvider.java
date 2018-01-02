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
package org.neo4j.logging;

import org.neo4j.function.Supplier;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An abstract {@link LogProvider} implementation, which ensures {@link Log}s are cached and reused.
 */
public abstract class AbstractLogProvider<T extends Log> implements LogProvider
{
    private final ConcurrentHashMap<String, T> logCache = new ConcurrentHashMap<>();

    @Override
    public T getLog( final Class loggingClass )
    {
        return getLog( loggingClass.getName(), new Supplier<T>()
        {
            @Override
            public T get()
            {
                return buildLog( loggingClass );
            }
        } );
    }

    @Override
    public T getLog( final String name )
    {
        return getLog( name, new Supplier<T>()
        {
            @Override
            public T get()
            {
                return buildLog( name );
            }
        } );
    }

    private T getLog( String name, Supplier<T> logSupplier )
    {
        T log = logCache.get( name );
        if ( log == null )
        {
            T newLog = logSupplier.get();
            log = logCache.putIfAbsent( name, newLog );
            if ( log == null )
            {
                log = newLog;
            }
        }
        return log;
    }

    /**
     * @return a {@link Collection} of the {@link Log} mappings that are currently held in the cache
     */
    protected Collection<T> cachedLogs()
    {
        return logCache.values();
    }

    /**
     * @param loggingClass the context for the returned {@link Log}
     * @return a {@link Log} that logs messages with the {@code loggingClass} as the context
     */
    protected abstract T buildLog( Class loggingClass );

    /**
     * @param name the context for the returned {@link Log}
     * @return a {@link Log} that logs messages with the specified name as the context
     */
    protected abstract T buildLog( String name );
}
