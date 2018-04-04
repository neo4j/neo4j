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

import java.lang.reflect.Array;
import java.util.Arrays;

import org.neo4j.collection.primitive.PrimitiveIntCollections;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.api.index.IndexProvider;

/**
 * Acting as a simplifier for the multiplexing that is going in inside a fusion index. A fusion index consists of multiple parts,
 * each handling one or more value groups. Each instance, be it a reader, populator or accessor should extend this class
 * to get that multiplexing at a low cost. All parts will live in an array with specific slot constants to each specific part.
 *
 * @param <T> type of instances
 */
public abstract class FusionIndexBase<T>
{
    static final int INSTANCE_COUNT = 5;

    static final int STRING = 0;
    static final int NUMBER = 1;
    static final int SPATIAL = 2;
    static final int TEMPORAL = 3;
    static final int LUCENE = 4;

    final T[] instances;
    final FusionIndexProvider.Selector selector;

    FusionIndexBase( T[] instances, FusionIndexProvider.Selector selector )
    {
        assert instances.length == INSTANCE_COUNT;
        this.instances = instances;
        this.selector = selector;
    }

    <R,E extends Exception> R[] instancesAs( Class<R> cls, ThrowingFunction<T,R,E> converter ) throws E
    {
        return instancesAs( instances, cls, converter );
    }

    static <T,R,E extends Exception> R[] instancesAs( T[] instances, Class<R> cls, ThrowingFunction<T,R,E> converter ) throws E
    {
        R[] result = (R[]) Array.newInstance( cls, instances.length );
        for ( int i = 0; i < instances.length; i++ )
        {
            result[i] = converter.apply( instances[i] );
        }
        return result;
    }

    /**
     * NOTE: duplicate of {@link #forAll(ThrowingConsumer, Iterable)} to avoid having to wrap subjects of one form into another.
     * There are some real use cases for passing in an array instead of {@link Iterable} out there...
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
    @SafeVarargs
    public static <T, E extends Exception> void forAll( ThrowingConsumer<T,E> consumer, T... subjects ) throws E
    {
        // Duplicate this method for array to avoid creating a purely internal list to shove that in to the other method.
        E exception = null;
        for ( T subject : subjects )
        {
            try
            {
                consumer.accept( subject );
            }
            catch ( Exception e )
            {
                exception = Exceptions.chain( exception, (E) e );
            }
        }
        if ( exception != null )
        {
            throw exception;
        }
    }

    /**
     * See {@link #forAll(ThrowingConsumer, Object[])}
     * NOTE: duplicate of {@link #forAll(ThrowingConsumer, Object[])} to avoid having to wrap subjects of one form into another.
     * There are some real use cases for passing in an Iterable instead of array out there...
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
        for ( T subject : subjects )
        {
            try
            {
                consumer.accept( subject );
            }
            catch ( Exception e )
            {
                exception = Exceptions.chain( exception, (E) e );
            }
        }
        if ( exception != null )
        {
            throw exception;
        }
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
