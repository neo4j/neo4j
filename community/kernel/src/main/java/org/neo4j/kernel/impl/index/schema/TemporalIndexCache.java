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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

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
    private volatile T localDateTime;
    private volatile T zonedDateTime;
    private volatile T localTime;
    private volatile T zonedTime;
    private volatile T duration;

    private List<T> parts;

    TemporalIndexCache( Factory<T, E> factory )
    {
        this.factory = factory;
        this.parts = new ArrayList<>();
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
            return date != null ? function.apply( date ) : orElse;

        case LOCAL_DATE_TIME:
            return localDateTime != null ? function.apply( localDateTime ) : orElse;

        case ZONED_DATE_TIME:
            return zonedDateTime != null ? function.apply( zonedDateTime ) : orElse;

        case LOCAL_TIME:
            return localTime != null ? function.apply( localTime ) : orElse;

        case ZONED_TIME:
            return zonedTime != null ? function.apply( zonedTime ) : orElse;

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
            addPartToList( date );
        }
        return date;
    }

    T localDateTime() throws E
    {
        if ( localDateTime == null )
        {
            localDateTime = factory.newLocalDateTime();
            addPartToList( localDateTime );
        }
        return localDateTime;
    }

    T zonedDateTime() throws E
    {
        if ( zonedDateTime == null )
        {
            zonedDateTime = factory.newZonedDateTime();
            addPartToList( zonedDateTime );
        }
        return zonedDateTime;
    }

    T localTime() throws E
    {
        if ( localTime == null )
        {
            localTime = factory.newLocalTime();
            addPartToList( localTime );
        }
        return localTime;
    }

    T zonedTime() throws E
    {
        if ( zonedTime == null )
        {
            zonedTime = factory.newZonedTime();
            addPartToList( zonedTime );
        }
        return zonedTime;
    }

    T duration() throws E
    {
        if ( duration == null )
        {
            duration = factory.newDuration();
            addPartToList( duration );
        }
        return duration;
    }

    private void addPartToList( T t )
    {
        if ( t != null )
        {
            parts.add( t );
        }
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
        return parts.iterator();
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
