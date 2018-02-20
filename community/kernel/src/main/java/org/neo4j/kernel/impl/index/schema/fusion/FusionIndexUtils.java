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

import java.util.Arrays;
import java.util.Collection;

import org.neo4j.function.ThrowingConsumer;

/**
 * Utility methods for working with multiple sub-components within the Fusion Index system
 */
public abstract class FusionIndexUtils
{
    /**
     * Method for calling a lambda function on many objects when it is expected that the function might
     * throw an exception. First exception will be thrown and subsequent will be suppressed.
     *
     * For example, in FusionIndexAccessor:
     * <pre>
     *    public void drop() throws IOException
     *    {
     *        forAll( IndexAccessor::drop, nativeAccessor, spatialAccessor, luceneAccessor );
     *        dropAction.drop( indexId );
     *    }
     * </pre>
     *
     * @param consumer lambda function to call on each object passed
     * @param subjects varargs array of objects to call the function on
     * @param <E> the type of exception anticipated, inferred from the lambda
     * @throws E if consumption fails with this exception
     */
    public static <T, E extends Exception> void forAll( ThrowingConsumer<T,E> consumer, Collection<T> subjects ) throws E
    {
        E exception = null;
        for ( T subject : subjects )
        {
            try
            {
                consumer.accept( subject );
            }
            catch ( Throwable t )
            {
                E e = (E) t;
                if ( exception == null )
                {
                    exception = e;
                }
                else
                {
                    exception.addSuppressed( e );
                }
            }
        }
        if ( exception != null )
        {
            throw exception;
        }
    }

    /**
     * See {@link FusionIndexUtils#forAll(ThrowingConsumer, Collection)}
     */
    public static <T, E extends Exception> void forAll( ThrowingConsumer<T,E> consumer, T... subjects ) throws E
    {
        forAll( consumer, Arrays.asList( subjects ) );
    }
}
