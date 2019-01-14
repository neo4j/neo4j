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

import java.util.Arrays;
import java.util.function.Function;

import org.neo4j.function.ThrowingConsumer;
import org.neo4j.helpers.Exceptions;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

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
    final InstanceSelector<T> instanceSelector;

    FusionIndexBase( SlotSelector slotSelector, InstanceSelector<T> instanceSelector )
    {
        this.slotSelector = slotSelector;
        this.instanceSelector = instanceSelector;
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
     *        forAll( IndexAccessor::drop, accessorList );
     *    }
     * </pre>
     *
     * @param consumer lambda function to call on each object passed
     * @param subjects {@link Iterable} of objects to call the function on
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
     * See {@link #forAll(ThrowingConsumer, Iterable)}
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
}
