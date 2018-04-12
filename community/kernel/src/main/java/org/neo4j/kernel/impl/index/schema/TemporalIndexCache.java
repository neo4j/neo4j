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
package org.neo4j.kernel.impl.index.schema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.neo4j.values.storable.ValueGroup;

import static org.neo4j.kernel.impl.index.schema.TemporalIndexCache.Offset.date;
import static org.neo4j.kernel.impl.index.schema.TemporalIndexCache.Offset.duration;
import static org.neo4j.kernel.impl.index.schema.TemporalIndexCache.Offset.localDateTime;
import static org.neo4j.kernel.impl.index.schema.TemporalIndexCache.Offset.localTime;
import static org.neo4j.kernel.impl.index.schema.TemporalIndexCache.Offset.zonedDateTime;
import static org.neo4j.kernel.impl.index.schema.TemporalIndexCache.Offset.zonedTime;

/**
 * Cache for lazily creating parts of the temporal index. Each part is created using the factory
 * the first time it is selected in a select() query, or the first time it's explicitly
 * asked for using e.g. date().
 * <p>
 * Iterating over the cache will return all currently created parts.
 *
 * @param <T> Type of parts
 */
class TemporalIndexCache<T> implements Iterable<T>
{
    private final Factory<T> factory;

    enum Offset
    {
        date,
        localDateTime,
        zonedDateTime,
        localTime,
        zonedTime,
        duration
    }

    private final ConcurrentHashMap<Offset,T> cache = new ConcurrentHashMap<>();

    TemporalIndexCache( Factory<T> factory )
    {
        this.factory = factory;
    }

    /**
     * Select the part corresponding to the given ValueGroup. Creates the part if needed,
     * and rethrows any create time exception as a RuntimeException.
     *
     * @param valueGroup target value group
     * @return selected part
     */
    T uncheckedSelect( ValueGroup valueGroup )
    {
        switch ( valueGroup )
        {
        case DATE:
            return date();

        case LOCAL_DATE_TIME:
            return localDateTime();

        case ZONED_DATE_TIME:
            return zonedDateTime();

        case LOCAL_TIME:
            return localTime();

        case ZONED_TIME:
            return zonedTime();

        case DURATION:
            return duration();

        default:
            throw new IllegalStateException( "Unsupported value group " + valueGroup );
        }
    }

    /**
     * Select the part corresponding to the given ValueGroup. Creates the part if needed,
     * in which case an exception of type E might be thrown.
     *
     * @param valueGroup target value group
     * @return selected part
     */
    T select( ValueGroup valueGroup ) throws IOException
    {
        try
        {
            return uncheckedSelect( valueGroup );
        }
        catch ( UncheckedIOException e )
        {
            throw e.getCause();
        }
    }

    /**
     * Select the part corresponding to the given ValueGroup, apply function to it and return the result.
     * If the part isn't created yet return orElse.
     *
     * @param valueGroup target value group
     * @param function function to apply to part
     * @param orElse result to return if part isn't created yet
     * @param <RESULT> type of result
     * @return the result
     */
    <RESULT> RESULT selectOrElse( ValueGroup valueGroup, Function<T,RESULT> function, RESULT orElse )
    {
        T cachedValue;
        switch ( valueGroup )
        {
        case DATE:
            cachedValue = cache.get( date );
            break;
        case LOCAL_DATE_TIME:
            cachedValue = cache.get( localDateTime );
            break;
        case ZONED_DATE_TIME:
            cachedValue = cache.get( zonedDateTime );
            break;
        case LOCAL_TIME:
            cachedValue = cache.get( localTime );
            break;
        case ZONED_TIME:
            cachedValue = cache.get( zonedTime );
            break;
        case DURATION:
            cachedValue = cache.get( duration );
            break;
        default:
            throw new IllegalStateException( "Unsupported value group " + valueGroup );
        }

        return cachedValue != null ? function.apply( cachedValue ) : orElse;
    }

    private T date() throws UncheckedIOException
    {
        return cache.computeIfAbsent( date, d ->
        {
            try
            {
                return factory.newDate();
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        } );
    }

    private T localDateTime() throws UncheckedIOException
    {

        return cache.computeIfAbsent( localDateTime, d  ->
        {
            try
            {
                return factory.newLocalDateTime();
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        } );
    }

    private T zonedDateTime() throws UncheckedIOException
    {
        return cache.computeIfAbsent( zonedDateTime, d ->
        {
            try
            {
                return factory.newZonedDateTime();
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        } );
    }

    private T localTime() throws UncheckedIOException
    {
        return cache.computeIfAbsent( localTime, d ->
        {
            try
            {
                return factory.newLocalTime();
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        } );
    }

    private T zonedTime() throws UncheckedIOException
    {
        return cache.computeIfAbsent( zonedTime, d ->
        {
            try
            {
                return factory.newZonedTime();
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        } );
    }

    private T duration() throws UncheckedIOException
    {
        return cache.computeIfAbsent( duration, d ->
        {
            try
            {
                return factory.newDuration();
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        } );
    }

    void loadAll()
    {
        try
        {
            date();
            zonedDateTime();
            localDateTime();
            zonedTime();
            localTime();
            duration();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public Iterator<T> iterator()
    {
        return cache.values().iterator();
    }

    /**
     * Factory used by the TemporalIndexCache to create parts.
     *
     * @param <T> Type of parts
     */
    interface Factory<T>
    {
        T newDate() throws IOException;

        T newLocalDateTime() throws IOException;

        T newZonedDateTime() throws IOException;

        T newLocalTime() throws IOException;

        T newZonedTime() throws IOException;

        T newDuration() throws IOException;
    }
}
