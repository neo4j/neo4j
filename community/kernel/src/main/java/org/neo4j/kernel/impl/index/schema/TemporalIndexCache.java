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

import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.values.storable.ValueGroup;

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

    private volatile T date;
    private volatile T dateTime;
    private volatile T dateTimeZoned;
    private volatile T time;
    private volatile T timeZoned;
    private volatile T duration;

    TemporalIndexCache( Factory<T, E> factory )
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
            return dateTime();

        case ZONED_DATE_TIME:
            return dateTimeZoned();

        case LOCAL_TIME:
            return time();

        case ZONED_TIME:
            return timeZoned();

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
            return date != null ? function.apply( date ) : orElse;

        case LOCAL_DATE_TIME:
            return dateTime != null ? function.apply( dateTime ) : orElse;

        case ZONED_DATE_TIME:
            return dateTimeZoned != null ? function.apply( dateTimeZoned ) : orElse;

        case LOCAL_TIME:
            return time != null ? function.apply( time ) : orElse;

        case ZONED_TIME:
            return timeZoned != null ? function.apply( timeZoned ) : orElse;

        case DURATION:
            return duration != null ? function.apply( duration ) : orElse;

        default:
            throw new IllegalStateException( "Unsupported value group " + valueGroup );
        }
    }

    T date() throws E
    {
        if ( date == null )
        {
            date = factory.newDate();
        }
        return date;
    }

    T dateTime() throws E
    {
        if ( dateTime == null )
        {
            dateTime = factory.newDateTime();
        }
        return dateTime;
    }

    T dateTimeZoned() throws E
    {
        if ( dateTimeZoned == null )
        {
            dateTimeZoned = factory.newDateTimeZoned();
        }
        return dateTimeZoned;
    }

    T time() throws E
    {
        if ( time == null )
        {
            time = factory.newTime();
        }
        return time;
    }

    T timeZoned() throws E
    {
        if ( timeZoned == null )
        {
            timeZoned = factory.newTimeZoned();
        }
        return timeZoned;
    }

    T duration() throws E
    {
        if ( duration == null )
        {
            duration = factory.newDuration();
        }
        return duration;
    }

    @Override
    public Iterator<T> iterator()
    {
        return Iterators.filter(
                Objects::nonNull, Iterators.iterator( date, dateTime, dateTimeZoned, time, timeZoned, duration ) );
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
        T newDateTime() throws E;
        T newDateTimeZoned() throws E;
        T newTime() throws E;
        T newTimeZoned() throws E;
        T newDuration() throws E;
    }
}
