/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import java.util.Arrays;
import java.util.function.Function;

import org.neo4j.collection.primitive.PrimitiveIntCollections;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

import static org.neo4j.kernel.impl.index.schema.fusion.SlotSelector.INSTANCE_COUNT;

/**
 * Acting as a simplifier for the multiplexing that is going in inside a fusion index. A fusion index consists of multiple parts,
 * each handling one or more value groups. Each instance, be it a reader, populator or accessor should extend this class
 * to get that multiplexing at a low cost. All parts will live in an array with specific slot constants to each specific part.
 *
 * @param <T> type of instances
 */
public abstract class FusionIndexBase<T>
{
    static Function<Value,ValueGroup> GROUP_OF = Value::valueGroup;

    final SlotSelector slotSelector;
    final Selector<T> selector;

    FusionIndexBase( SlotSelector slotSelector, Selector<T> selector )
    {
        this.slotSelector = slotSelector;
        this.selector = selector;
    }

    /**
     * Short-hand for calling the static {@link #instancesAs(Selector, Object[], ThrowingFunction)}, here with the local {@link #selector}.
     */
    <R,E extends Exception> R[] instancesAs( R[] target, ThrowingFunction<T,R,E> converter ) throws E
    {
        return instancesAs( selector, target, converter );
    }

    /**
     * Convenience method typically for calling a method on each of the sub-parts of a fusion entity,
     * one which creates another instance. All those instances are returned as an array, or actually put into an array
     * created by the caller to avoid reflection to instantiate the array.
     *
     * @param selector {@link Selector} to use as the source.
     * @param target array to put the created instances into, also returned.
     * @param converter {@link ThrowingFunction} which converts from the source to target instance.
     * @param <S> type of source instance.
     * @param <T> type of target instance.
     * @param <E> type of exception that converter may throw.
     * @return the target array which was passed in, now populated.
     * @throws E exception from converter.
     */
    static <S,T,E extends Exception> T[] instancesAs( Selector<S> selector, T[] target, ThrowingFunction<S,T,E> converter ) throws E
    {
        for ( int slot = 0; slot < INSTANCE_COUNT; slot++ )
        {
            target[slot] = converter.apply( selector.select( slot ) );
        }
        return target;
    }

    static <T, E extends Exception> void forInstantiated( ThrowingConsumer<T,E> consumer, Selector<T> selector ) throws E
    {
        E exception = null;
        for ( int slot = 0; slot < INSTANCE_COUNT; slot++ )
        {
            T instance = selector.getIfInstantiated( slot );
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

    public static <T, E extends Exception> void forAll( ThrowingConsumer<T,E> consumer, Selector<T> selector ) throws E
    {
        E exception = null;
        for ( int slot = 0; slot < INSTANCE_COUNT; slot++ )
        {
            exception = consume( exception, consumer, selector.select( slot ) );
        }
        if ( exception != null )
        {
            throw exception;
        }
    }

    /**
     * See {@link #forAll(ThrowingConsumer, Object[])}
     *
     * Method for calling a lambda function on many objects when it is expected that the function might
     * throw an exception. First exception will be thrown and subsequent will be suppressed.
     *
     * For example, in FusionIndexAccessor:
     * <pre>
     *    public void drop() throws IOException
     *    {
     *        forAll( IndexAccessor::drop, firstAccessor, secondAccessor, thirdAccessor );
     *    }
     * </pre>
     *
     * @param consumer lambda function to call on each object passed
     * @param subjects varargs array of objects to call the function on
     * @param <E> the type of exception anticipated, inferred from the lambda
     * @throws E if consumption fails with this exception
     */
    public static <T, E extends Exception> void forAll( ThrowingConsumer<T,E> consumer, Iterable<T> subjects ) throws E
    {
        E exception = null;
        for ( T instance : subjects )
        {
            exception = consume( exception, consumer, instance );
        }
        if ( exception != null )
        {
            throw exception;
        }
    }

    /**
     * Method for calling a lambda function on many objects when it is expected that the function might
     * throw an exception. First exception will be thrown and subsequent will be suppressed.
     *
     * For example, in FusionIndexAccessor:
     * <pre>
     *    public void drop() throws IOException
     *    {
     *        forAll( IndexAccessor::drop, firstAccessor, secondAccessor, thirdAccessor );
     *    }
     * </pre>
     *
     * @param consumer lambda function to call on each object passed
     * @param subjects varargs array of objects to call the function on
     * @param <E> the type of exception anticipated, inferred from the lambda
     * @throws E if consumption fails with this exception
     */
    public static <T, E extends Exception> void forAll( ThrowingConsumer<T,E> consumer, T[] subjects ) throws E
    {
        forAll( consumer, Arrays.asList( subjects ) );
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

    static void validateSelectorInstances( Object[] instances, int... aliveIndex )
    {
        for ( int i = 0; i < instances.length; i++ )
        {
            boolean expected = PrimitiveIntCollections.contains( aliveIndex, i );
            boolean actual = instances[i] != IndexProvider.EMPTY;
            if ( expected != actual )
            {
                throw new IllegalArgumentException(
                        String.format( "Only indexes expected to be separated from IndexProvider.EMPTY are %s but was %s",
                                Arrays.toString( aliveIndex ), Arrays.toString( instances ) ) );
            }
        }
    }
}
