/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.notification.Notification;

/**
 * Represents the result of {@link GraphDatabaseService#execute(String, java.util.Map) executing} a query.
 * <p/>
 * The result is comprised of a number of rows, potentially computed lazily, with this result object being an iterator
 * over those rows. Each row is represented as a <code>{@link Map}&lt;{@link String}, {@link Object}&gt;</code>, the
 * keys in this map are the names of the columns in the row, as specified by the {@code return} clause of the query,
 * and the values of the map is the corresponding computed value of the expression in the {@code return} clause. Each
 * row will thus have the same set of keys, and these keys can be retrieved using the
 * {@linkplain #columns() columns-method}.
 * <p/>
 * To ensure that any resource, including transactions bound to the query, are properly freed, the result must either
 * be fully exhausted, by means of the {@linkplain java.util.Iterator iterator protocol}, or the result has to be
 * explicitly closed, by invoking the {@linkplain #close() close-method}.
 * <p/>
 * Idiomatic use of the Result object would look like this:
 * <pre><code>
 * try ( Result result = graphDatabase.execute( query, parameters ) )
 * {
 *     while ( result.hasNext() )
 *     {
 *         Map&lt;String, Object&gt; row = result.next();
 *         for ( String key : result.columns() )
 *         {
 *             System.out.printf( "%s = %s%n", key, row.get( key ) );
 *         }
 *     }
 * }
 * </code></pre>
 * If the result consists of only a single column, or if only one of the columns is of interest, a projection can be
 * extracted using {@link #columnAs(String)}. This produces a new iterator over the values of the named column. It
 * should be noted that this iterator consumes the rows of the result in the same way as invoking {@link #next()} on
 * this object would, and that the {@link #close() close-method} on either iterator has the same effect. It is thus
 * safe to either close the projected column iterator, or this iterator, or both if all rows have not been consumed.
 * <p/>
 * In addition to the {@link #next() iteration methods} on this interface, {@link #close()}, and the
 * {@link #columnAs(String) column projection method}, there are two methods for getting a string representation of the
 * result that also consumes the entire result if invoked. {@link #resultAsString()} returns a single string
 * representation of all (remaining) rows in the result, and {@link #writeAsStringTo(PrintWriter)} does the same, but
 * streams the result to the provided {@link PrintWriter} instead, without allocating large string objects.
 * <p/>
 * The methods that do not consume any rows from the result, or in other ways alter the state of the result are safe to
 * invoke at any time, even after the result has been {@linkplain #close() closed} or fully exhausted. These methods
 * are:
 * <ul>
 * <li>{@link #columns()}</li>
 * <li>{@link #getQueryStatistics()}</li>
 * <li>{@link #getQueryExecutionType()}</li>
 * <li>{@link #getExecutionPlanDescription()}</li>
 * </ul>
 * <p/>
 * Not all queries produce an actual result, and some queries that do might yield an empty result set. In order to
 * distinguish between these cases the {@link QueryExecutionType} {@linkplain #getQueryExecutionType() of this result}
 * can be queried.
 */
public interface Result extends ResourceIterator<Map<String, Object>>
{
    /**
     * Indicates what kind of query execution produced this result.
     *
     * @return an object that indicates what kind of query was executed to produce this result.
     */
    QueryExecutionType getQueryExecutionType();

    /**
     * The exact names used to represent each column in the result set.
     *
     * @return List of the column names.
     */
    List<String> columns();

    /**
     * Returns an iterator with the result objects from a single column of the result set. This method is best used for
     * single column results.
     *
     * <p><b>To ensure that any resources, including transactions bound to it, are properly closed, the iterator must
     * either be fully exhausted, or the {@link org.neo4j.graphdb.ResourceIterator#close() close()} method must be
     * called.</b></p>
     *
     * @param name exact name of the column, as it appeared in the original query
     * @param <T>  desired type cast for the result objects
     * @return an iterator of the result objects, possibly empty
     * @throws ClassCastException                  when the result object can not be cast to the requested type
     * @throws org.neo4j.graphdb.NotFoundException when the column name does not appear in the original query
     */
    <T> ResourceIterator<T> columnAs( String name );

    /**
     * Denotes there being more rows available in this result. These rows must either be consumed, by invoking
     * {@link #next()}, or the result has to be {@link #close() closed}.
     *
     * @return {@code true} if there is more rows available in this result, {@code false} otherwise.
     */
    boolean hasNext();

    /**
     * Returns the next row in this result.
     *
     * @return the next row in this result.
     */
    Map<String, Object> next();

    /**
     * Closes the result, freeing up any resources held by the result.
     *
     * This is an idempotent operation, invoking it multiple times has the same effect as invoking it exactly once.
     * It is thus safe (and even encouraged, for style and simplicity) to invoke this method even after consuming all
     * rows in the result through the {@link #next() next-method}.
     */
    void close();

    /**
     * Statistics about the effects of the query.
     *
     * @return statistics about the effects of the query.
     */
    QueryStatistics getQueryStatistics();

    /**
     * Returns a description of the query plan used to produce this result.
     *
     * Retrieving a description of the execution plan that was executed is always possible, regardless of whether the
     * query requested a plan or not. For implementing a client with the ability to present the plan to the user, it is
     * useful to be able to tell if the query requested a description of the plan or not. For these purposes the
     * {@link QueryExecutionType#requestedExecutionPlanDescription()}-method is used.
     *
     * Being able to invoke this method, regardless of whether the user requested the plan or not is useful for
     * purposes of debugging queries in applications.
     *
     * @return a description of the query plan used to produce this result.
     */
    ExecutionPlanDescription getExecutionPlanDescription();

    /**
     * Provides a textual representation of the query result.
     * <p><b>
     * The execution result represented by this object will be consumed in its entirety after this method is called.
     * Calling any of the other iterating methods on it should not be expected to return any results.
     * </b></p>
     *
     * @return the execution result formatted as a string
     */
    String resultAsString();

    /**
     * Provides a textual representation of the query result to the provided {@link java.io.PrintWriter}.
     * <p><b>
     * The execution result represented by this object will be consumed in its entirety after this method is called.
     * Calling any of the other iterating methods on it should not be expected to return any results.
     * </b></p>
     * @param writer the {@link java.io.PrintWriter} to receive the textual representation of the query result.
     */
    void writeAsStringTo( PrintWriter writer );

    /** Removing rows from the result is not supported. */
    void remove();

    /**
     * Provides notifications about the query producing this result.
     *
     * Notifications can be warnings about problematic queries or other valuable information that can be
     * presented in a client.
     *
     * @return an iterable of all notifications created when running the query.
     */
    Iterable<Notification> getNotifications();
}
