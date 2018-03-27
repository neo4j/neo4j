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

import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;

import org.neo4j.values.storable.ValueGroup;

import static org.neo4j.kernel.impl.index.schema.TemporalIndexCache.Offset.date;
import static org.neo4j.kernel.impl.index.schema.TemporalIndexCache.Offset.localDateTime;
import static org.neo4j.kernel.impl.index.schema.TemporalIndexCache.Offset.zonedDateTime;
import static org.neo4j.kernel.impl.index.schema.TemporalIndexCache.Offset.localTime;
import static org.neo4j.kernel.impl.index.schema.TemporalIndexCache.Offset.zonedTime;
import static org.neo4j.kernel.impl.index.schema.TemporalIndexCache.Offset.duration;

/**
 * Cache for lazily creating parts of the temporal index. Each part is created using the factory
 * the first time it is selected in a select() query, or the first time it's explicitly
 * asked for using e.g. date().
 *
 * Iterating over the cache will return all currently created parts.
 *
 * @param <T> Type of parts
 * @param <E> Type of exception potentially thrown during creation
 */
class TemporalIndexCache<T, E extends Exception> implements Iterable<T>
{
    private final Factory<T, E> factory;

    enum Offset
    {
        date,
        localDateTime,
        zonedDateTime,
        localTime,
        zonedTime,
        duration
    }

    private final Object dateLock = new Object();
    private final Object localDateTimeLock = new Object();
    private final Object zonedDateTimeLock = new Object();
    private final Object localTimeLock = new Object();
    private final Object zonedTimeLock = new Object();
    private final Object durationLock = new Object();

    private T[] parts;

    TemporalIndexCache( Factory<T, E> factory )
    {
        this.factory = factory;
        //noinspection unchecked
        this.parts = (T[]) new Object[Offset.values().length];
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
        try
        {
            return select( valueGroup );
        }
        catch ( Exception t )
        {
            throw new RuntimeException( t );
        }
    }

    /**
     * Select the part corresponding to the given ValueGroup. Creates the part if needed,
     * in which case an exception of type E might be thrown.
     *
     * @param valueGroup target value group
     * @return selected part
     * @throws E exception potentially thrown during creation
     */
    T select( ValueGroup valueGroup ) throws E
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
     * Select the part corresponding to the given ValueGroup, apply function to it and return the result.
     * If the part isn't created yet return orElse.
     *
     * @param valueGroup target value group
     * @param function function to apply to part
     * @param orElse result to return if part isn't created yet
     * @param <RESULT> type of result
     * @return the result
     */
    <RESULT> RESULT selectOrElse( ValueGroup valueGroup, Function<T, RESULT> function, RESULT orElse )
    {
        switch ( valueGroup )
        {
        case DATE:
            return parts[date.ordinal()] != null ? function.apply( parts[date.ordinal()] ) : orElse;

        case LOCAL_DATE_TIME:
            return parts[localDateTime.ordinal()] != null ? function.apply( parts[localDateTime.ordinal()] ) : orElse;

        case ZONED_DATE_TIME:
            return parts[zonedDateTime.ordinal()] != null ? function.apply( parts[zonedDateTime.ordinal()] ) : orElse;

        case LOCAL_TIME:
            return parts[localTime.ordinal()] != null ? function.apply( parts[localTime.ordinal()] ) : orElse;

        case ZONED_TIME:
            return parts[zonedTime.ordinal()] != null ? function.apply( parts[zonedTime.ordinal()] ) : orElse;

        case DURATION:
            return parts[duration.ordinal()] != null ? function.apply( parts[duration.ordinal()] ) : orElse;

        default:
            throw new IllegalStateException( "Unsupported value group " + valueGroup );
        }
    }

    T date() throws E
    {
        synchronized ( dateLock )
        {
            if ( parts[date.ordinal()] == null )
            {
                parts[date.ordinal()] = factory.newDate();
            }
        }
        return parts[date.ordinal()];
    }

    T localDateTime() throws E
    {
        synchronized ( localDateTimeLock )
        {
            if ( parts[localDateTime.ordinal()] == null )
            {
                parts[localDateTime.ordinal()] = factory.newLocalDateTime();
            }
        }
        return parts[localDateTime.ordinal()];
    }

    T zonedDateTime() throws E
    {
        synchronized ( zonedDateTimeLock )
        {
            if ( parts[zonedDateTime.ordinal()] == null )
            {
                parts[zonedDateTime.ordinal()] = factory.newZonedDateTime();
            }
        }
        return parts[zonedDateTime.ordinal()];
    }

    T localTime() throws E
    {
        synchronized ( localTimeLock )
        {
            if ( parts[localTime.ordinal()] == null )
            {
                parts[localTime.ordinal()] = factory.newLocalTime();
            }
        }
        return parts[localTime.ordinal()];
    }

    T zonedTime() throws E
    {
        synchronized ( zonedTimeLock )
        {
            if ( parts[zonedTime.ordinal()] == null )
            {
                parts[zonedTime.ordinal()] = factory.newZonedTime();
            }
        }
        return parts[zonedTime.ordinal()];
    }

    T duration() throws E
    {
        synchronized ( durationLock )
        {
            if ( parts[duration.ordinal()] == null )
            {
                parts[duration.ordinal()] = factory.newDuration();
            }
        }
        return parts[duration.ordinal()];
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
        return Arrays.stream( parts ).filter( Objects::nonNull ).iterator();
    }

    /**
     * Factory used by the TemporalIndexCache to create parts.
     *
     * @param <T> Type of parts
     * @param <E> Type of exception potentially thrown during create
     */
    interface Factory<T, E extends Exception>
    {
        T newDate() throws E;
        T newLocalDateTime() throws E;
        T newZonedDateTime() throws E;
        T newLocalTime() throws E;
        T newZonedTime() throws E;
        T newDuration() throws E;
    }
}
