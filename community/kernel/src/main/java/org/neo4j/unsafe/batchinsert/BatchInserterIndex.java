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
package org.neo4j.unsafe.batchinsert;

import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;

/**
 * The {@link BatchInserter} version of {@link Index}. Additions/updates to a
 * {@link BatchInserterIndex} doesn't necessarily gets added to the actual index
 * immediately, but are instead forced to be written when the index is shut
 * down, {@link BatchInserterIndexProvider#shutdown()}.
 *
 * To guarantee additions/updates are seen by {@link #updateOrAdd(long, Map)},
 * {@link #get(String, Object)}, {@link #query(String, Object)} and
 * {@link #query(Object)} a call to {@link #flush()} must be made prior to
 * calling such a method. This enables implementations more flexibility in
 * making for performance optimizations.
 */
public interface BatchInserterIndex
{
    /**
     * Adds key/value pairs for {@code entity} to the index. If there's a
     * previous index for {@code entity} it will co-exist with this new one.
     * This behavior is because of performance reasons, to not being forced to
     * check if indexing for {@code entity} already exists or not. If you need
     * to update indexing for {@code entity} and it's ok with a slower indexing
     * process use {@link #updateOrAdd(long, Map)} instead.
     *
     * Entries added to the index aren't necessarily written to the index and to
     * disk until {@link BatchInserterIndexProvider#shutdown()} has been called.
     * Entries added to the index isn't necessarily seen by other methods:
     * {@link #updateOrAdd(long, Map)}, {@link #get(String, Object)},
     * {@link #query(String, Object)} and {@link #query(Object)} until a call to
     * {@link #flush()} has been made.
     *
     * @param entityId the entity (i.e id of {@link Node} or
     *            {@link Relationship}) to associate the key/value pairs with.
     * @param properties key/value pairs to index for {@code entity}.
     */
    void add( long entityId, Map<String, Object> properties );

    /**
     * Adds key/value pairs for {@code entity} to the index. If there's any
     * previous index for {@code entity} all such indexed key/value pairs will
     * be deleted first. This method can be considerably slower than
     * {@link #add(long, Map)} because it must check if there are properties
     * already indexed for {@code entity}. So if you know that there's no
     * previous indexing for {@code entity} use {@link #add(long, Map)} instead.
     *
     * Entries added to the index aren't necessarily written to the index and to
     * disk until {@link BatchInserterIndexProvider#shutdown()} has been called.
     * Entries added to the index isn't necessarily seen by other methods:
     * {@link #updateOrAdd(long, Map)}, {@link #get(String, Object)},
     * {@link #query(String, Object)} and {@link #query(Object)} until a call to
     * {@link #flush()} has been made. So only entries added before the most
     * recent {@link #flush()} are guaranteed to be found by this method.
     *
     * @param entityId the entity (i.e id of {@link Node} or
     *            {@link Relationship}) to associate the key/value pairs with.
     * @param properties key/value pairs to index for {@code entity}.
     */
    void updateOrAdd( long entityId, Map<String, Object> properties );

    /**
     * Returns exact matches from this index, given the key/value pair. Matches
     * will be for key/value pairs just as they were added by the
     * {@link #add(long, Map)} or {@link #updateOrAdd(long, Map)} method.
     *
     * Entries added to the index aren't necessarily written to the index and to
     * disk until {@link BatchInserterIndexProvider#shutdown()} has been called.
     * Entries added to the index isn't necessarily seen by other methods:
     * {@link #updateOrAdd(long, Map)}, {@link #get(String, Object)},
     * {@link #query(String, Object)} and {@link #query(Object)} until a call to
     * {@link #flush()} has been made. So only entries added before the most
     * recent {@link #flush()} are guaranteed to be found by this method.
     *
     * @param key the key in the key/value pair to match.
     * @param value the value in the key/value pair to match.
     * @return the result wrapped in an {@link IndexHits} object. If the entire
     *         result set isn't looped through, {@link IndexHits#close()} must
     *         be called before disposing of the result.
     */
    IndexHits<Long> get( String key, Object value );

    /**
     * Returns matches from this index based on the supplied {@code key} and
     * query object, which can be a query string or an implementation-specific
     * query object.
     *
     * Entries added to the index aren't necessarily written to the index and
     * to disk until {@link BatchInserterIndexProvider#shutdown()} has been
     * called. Entries added to the index isn't necessarily seen by other
     * methods: {@link #updateOrAdd(long, Map)}, {@link #get(String, Object)},
     * {@link #query(String, Object)} and {@link #query(Object)} until a call
     * to {@link #flush()} has been made. So only entries added before the most
     * recent {@link #flush()} are guaranteed to be found by this method.
     *
     * @param key the key in this query.
     * @param queryOrQueryObject the query for the {@code key} to match.
     * @return the result wrapped in an {@link IndexHits} object. If the entire
     * result set isn't looped through, {@link IndexHits#close()} must be
     * called before disposing of the result.
     */
    IndexHits<Long> query( String key, Object queryOrQueryObject );

    /**
     * Returns matches from this index based on the supplied query object,
     * which can be a query string or an implementation-specific query object.
     *
     * Entries added to the index aren't necessarily written to the index and
     * to disk until {@link BatchInserterIndexProvider#shutdown()} has been
     * called. Entries added to the index isn't necessarily seen by other
     * methods: {@link #updateOrAdd(long, Map)}, {@link #get(String, Object)},
     * {@link #query(String, Object)} and {@link #query(Object)} until a call
     * to {@link #flush()} has been made. So only entries added before the most
     * recent {@link #flush()} are guaranteed to be found by this method.
     *
     * @param queryOrQueryObject the query to match.
     * @return the result wrapped in an {@link IndexHits} object. If the entire
     * result set isn't looped through, {@link IndexHits#close()} must be
     * called before disposing of the result.
     */
    IndexHits<Long> query( Object queryOrQueryObject );

    /**
     * Makes sure additions/updates can be seen by {@link #get(String, Object)},
     * {@link #query(String, Object)} and {@link #query(Object)} so that they
     * are guaranteed to return correct results. Also
     * {@link #updateOrAdd(long, Map)} will find previous indexing correctly
     * after a flush.
     */
    void flush();

    /**
     * Sets the cache size for key/value pairs for the given {@code key}.
     * Caching values may increase {@link #get(String, Object)} lookups significantly,
     * but may at the same time slow down insertion of data some.
     *
     * Be sure to call this method to enable caching for keys that will be
     * used a lot in lookups. It's also best to call this method for your keys
     * right after the index has been created.
     *
     * @param key the key to set cache capacity for.
     * @param size the number of values to cache results for.
     */
    void setCacheCapacity( String key, int size );
}
