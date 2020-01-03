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
package org.neo4j.kernel.impl.index.schema.fusion;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.function.Consumer;

import org.neo4j.function.ThrowingConsumer;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.helpers.Exceptions;

/**
 * Selects an instance given a certain slot.
 * @param <T> type of instance
 */
class InstanceSelector<T>
{
    final EnumMap<IndexSlot,T> instances;
    boolean closed;

    /**
     * Empty selector
     */
    InstanceSelector()
    {
        this( new EnumMap<>( IndexSlot.class ) );
    }

    /**
     * Selector with initial mapping
     *
     * @param map with initial mapping
     */
    InstanceSelector( EnumMap<IndexSlot,T> map )
    {
        this.instances = map;
    }

    /**
     * Put a new mapping to this selector
     */
    void put( IndexSlot slot, T instance )
    {
        instances.put( slot, instance );
    }

    /**
     * Returns the instance at the given slot.
     *
     * @param slot slot number to return instance for.
     * @return the instance at the given {@code slot}.
     */
    T select( IndexSlot slot )
    {
        if ( !instances.containsKey( slot )  )
        {
            throw new IllegalStateException( "Instance is not instantiated" );
        }
        return instances.get( slot );
    }

    /**
     * Map current instances to some other type using the converter function.
     * Even called on instances that haven't been instantiated yet.
     * Mapping is preserved in returned {@link EnumMap}.
     *
     * @param converter {@link ThrowingFunction} which converts from the source to target instance.
     * @param <R> type of returned instance.
     * @param <E> type of exception that converter may throw.
     * @return A new {@link EnumMap} containing the mapped values.
     * @throws E exception from converter.
     */
    <R,E extends Exception> EnumMap<IndexSlot,R> map( ThrowingFunction<T,R,E> converter ) throws E
    {
        EnumMap<IndexSlot,R> result = new EnumMap<>( IndexSlot.class );
        for ( IndexSlot slot : IndexSlot.values() )
        {
            result.put( slot, converter.apply( select( slot ) ) );
        }
        return result;
    }

    /**
     * Map current instances to some other type using the converter function,
     * without preserving the mapping.
     * Even called on instances that haven't been instantiated yet.
     *
     * @param converter {@link ThrowingFunction} which converts from the source to target instance.
     * @param <R> type of returned instance.
     * @param <E> type of exception that converter may throw.
     * @return A new {@link EnumMap} containing the mapped values.
     * @throws E exception from converter.
     */
    @SuppressWarnings( "unchecked" )
    <R,E extends Exception> Iterable<R> transform( ThrowingFunction<T,R,E> converter ) throws E
    {
        List<R> result = new ArrayList<>();
        for ( IndexSlot slot : IndexSlot.values() )
        {
            result.add( converter.apply( select( slot ) ) );
        }
        return result;
    }

    /**
     * Convenience method for doing something to all instances, even those that haven't already been instantiated.
     *
     * @param consumer {@link Consumer} which performs some action on an instance.
     */
    void forAll( Consumer<T> consumer )
    {
        RuntimeException exception = null;
        for ( IndexSlot slot : IndexSlot.values() )
        {
            exception = consumeAndChainException( select( slot ), consumer, exception );
        }
        if ( exception != null )
        {
            throw exception;
        }
    }

    /**
     * Perform a final action on instantiated instances and then closes this selector, preventing further instantiation.
     *
     * @param consumer {@link Consumer} which performs some action on an instance.
     */
    void close( Consumer<T> consumer )
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

    @Override
    public String toString()
    {
        return instances.toString();
    }

    /**
     * Convenience method for doing something to already instantiated instances.
     *
     * @param consumer {@link ThrowingConsumer} which performs some action on an instance.
     */
    private void forInstantiated( Consumer<T> consumer )
    {
        RuntimeException exception = null;
        for ( T instance : instances.values() )
        {
            if ( instance != null )
            {
                exception = consumeAndChainException( instance, consumer, exception );
            }
        }
        if ( exception != null )
        {
            throw exception;
        }
    }

    private RuntimeException consumeAndChainException( T instance, Consumer<T> consumer, RuntimeException exception )
    {
        try
        {
            consumer.accept( instance );
        }
        catch ( RuntimeException e )
        {
            exception = Exceptions.chain( exception, e );
        }
        return exception;
    }
}
