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
package org.neo4j.kernel.impl.index.schema.fusion;

import java.util.function.IntFunction;

import org.neo4j.function.ThrowingConsumer;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.helpers.Exceptions;

import static org.neo4j.kernel.impl.index.schema.fusion.SlotSelector.INSTANCE_COUNT;

/**
 * Selects an instance given a certain slot.
 * @param <T> type of instance
 */
class InstanceSelector<T>
{
    private final T[] instances;
    private final IntFunction<T> factory;
    private boolean closed;

    /**
     * @param instances fully instantiated instances so that no factory is needed.
     */
    InstanceSelector( T[] instances )
    {
        this( instances, slot ->
        {
            throw new IllegalStateException( "No instantiation expected" );
        } );
    }

    /**
     * @param instances uninstantiated instances, instantiated lazily by the {@code factory}.
     * @param factory {@link IntFunction} for instantiating instances for specific slots.
     */
    InstanceSelector( T[] instances, IntFunction<T> factory )
    {
        this.instances = instances;
        this.factory = factory;
    }

    /**
     * Returns the instance at the given slot. Instantiating it if it hasn't already been instantiated.
     *
     * @param slot slot number to return instance for.
     * @return the instance at the given {@code slot}.
     */
    T select( int slot )
    {
        if ( instances[slot] == null )
        {
            assertOpen();
            instances[slot] = factory.apply( slot );
        }
        return instances[slot];
    }

    private void assertOpen()
    {
        if ( closed )
        {
            throw new IllegalStateException( "This selector has been closed" );
        }
    }

    /**
     * Returns the instance at the given slot. If the instance at the given {@code slot} hasn't been instantiated yet, {@code null} is returned.
     *
     * @param slot slot number to return instance for.
     * @return the instance at the given {@code slot}, or {@code null}.
     */
    T getIfInstantiated( int slot )
    {
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
        for ( int slot = 0; slot < INSTANCE_COUNT; slot++ )
        {
            target[slot] = converter.apply( select( slot ) );
        }
        return target;
    }

    /**
     * Convenience method for doing something to already instantiated instances.
     *
     * @param consumer {@link ThrowingConsumer} which performs some action on an instance.
     * @param <E> type of exception the action may throw.
     * @throws E exception from action.
     */
    <E extends Exception> void forInstantiated( ThrowingConsumer<T,E> consumer ) throws E
    {
        E exception = null;
        for ( int slot = 0; slot < INSTANCE_COUNT; slot++ )
        {
            T instance = getIfInstantiated( slot );
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
        for ( int slot = 0; slot < INSTANCE_COUNT; slot++ )
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
