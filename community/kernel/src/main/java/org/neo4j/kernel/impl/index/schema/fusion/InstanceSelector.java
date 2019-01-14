/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.index.schema.fusion;

import org.neo4j.function.ThrowingConsumer;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.helpers.Exceptions;

/**
 * Selects an instance given a certain slot.
 * @param <T> type of instance
 */
class InstanceSelector<T>
{
    final T[] instances;
    boolean closed;

    /**
     * @param instances array of fully instantiated instances
     */
    InstanceSelector( T[] instances )
    {
        this.instances = instances;
    }

    /**
     * Returns the instance at the given slot.
     *
     * @param slot slot number to return instance for.
     * @return the instance at the given {@code slot}.
     */
    T select( int slot )
    {
        if ( instances[slot] == null )
        {
            throw new IllegalStateException( "Instance is not instantiated" );
        }
        return instances[slot];
    }

    /**
     * Convenience method typically for calling a method on each of the instances, a method that returns another type of instance.
     * Even called on instances that haven't been instantiated yet. All those created instances are put into the provided array and returned.
     *
     * @param target array to put the created instances into, also returned.
     * @param converter {@link ThrowingFunction} which converts from the source to target instance.
     * @param <R> type of returned instance.
     * @param <E> type of exception that converter may throw.
     * @return the target array which was passed in, now populated.
     * @throws E exception from converter.
     */
    <R,E extends Exception> R[] instancesAs( R[] target, ThrowingFunction<T,R,E> converter ) throws E
    {
        for ( int slot = 0; slot < instances.length; slot++ )
        {
            target[slot] = converter.apply( select( slot ) );
        }
        return target;
    }

    /**
     * Convenience method for doing something to all instances, even those that haven't already been instantiated.
     *
     * @param consumer {@link ThrowingConsumer} which performs some action on an instance.
     * @param <E> type of exception the action may throw.
     * @throws E exception from action.
     */
    <E extends Exception> void forAll( ThrowingConsumer<T,E> consumer ) throws E
    {
        E exception = null;
        for ( int slot = 0; slot < instances.length; slot++ )
        {
            exception = consume( exception, consumer, select( slot ) );
        }
        if ( exception != null )
        {
            throw exception;
        }
    }

    /**
     * Perform a final action on instantiated instances and then closes this selector, preventing further instantiation.
     *
     * @param consumer {@link ThrowingConsumer} which performs some action on an instance.
     * @param <E> type of exception the action may throw.
     * @throws E exception from action.
     */
    <E extends Exception> void close( ThrowingConsumer<T,E> consumer ) throws E
    {
        if ( !closed )
        {
            try
            {
                forInstantiated( consumer );
            }
            finally
            {
                closed = true;
            }
        }
    }

    /**
     * Convenience method for doing something to already instantiated instances.
     *
     * @param consumer {@link ThrowingConsumer} which performs some action on an instance.
     * @param <E> type of exception the action may throw.
     * @throws E exception from action.
     */
    private <E extends Exception> void forInstantiated( ThrowingConsumer<T,E> consumer ) throws E
    {
        E exception = null;
        for ( T instance : instances )
        {
            if ( instance != null )
            {
                exception = consume( exception, consumer, instance );
            }
        }
        if ( exception != null )
        {
            throw exception;
        }
    }

    private static <E extends Exception, T> E consume( E exception, ThrowingConsumer<T,E> consumer, T instance )
    {
        try
        {
            consumer.accept( instance );
        }
        catch ( Exception e )
        {
            exception = Exceptions.chain( exception, (E) e );
        }
        return exception;
    }
}
