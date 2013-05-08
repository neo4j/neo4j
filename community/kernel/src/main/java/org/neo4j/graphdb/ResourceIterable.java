/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.graphdb;

/**
 * {@link Iterable} whose {@link ResourceIterator iterators} have associated resources
 * that need to be released.
 * 
 * {@link ResourceIterator ResourceIterators} obtained inside a transaction are always
 * automatically released when the transaction is committed or rolled back. Outside a transaction, 
 * you must ensure that all returned ResourceIterators are either fully exhausted, 
 * or explicitly closed. Failure to ensure this may cause indefinitely blocking of write operations.
 * <p>
 * If you intend to exhaust the returned iterators, you can use conventional
 * code as you would with a normal Iterable:
 *
 * <pre>
 * {@code
 * ResourceIterable<Object> iterable;
 * for ( Object item : iterable )
 * {
 *     ...
 * }
 * }</pre>
 *
 * However, if your code might not exhaust the iterator, (run until {@link java.util.Iterator#hasNext()} 
 * returns {@code false}), {@link ResourceIterator} provides you with a {@link ResourceIterator#close()} method that 
 * should be invoked to free its resources, by using a {@code finally}-block, or try-with-resource.
 *
 * <pre>
 * {@code
 * ResourceIterable<Object> iterable;
 * ResourceIterator<Object> iterator = iterable.iterator();
 * try
 * {
 *     while ( iterator.hasNext() )
 *     {
 *         Object item = iterator.next();
 *         if ( ... )
 *         {
 *             return item; // iterator may not be exhausted.
 *         }
 *     }
 * }
 * finally
 * {
 *     iterator.close();
 * }
 * }</pre>
 *
 * @param <T> the type of values returned through the iterators
 * 
 * @see ResourceIterator
 * @see Transaction
 */
public interface ResourceIterable<T> extends Iterable<T>
{
    /**
     * Returns an {@link ResourceIterator iterator} with associated resources that must be managed.
     *
     * Don't forget to either exhaust the returned iterator or call the
     * {@link ResourceIterator#close() close method} on it.
     */
    @Override
    ResourceIterator<T> iterator();
}
