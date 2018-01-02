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
package org.neo4j.graphdb.index;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.PropertyContainer;

/**
 * An index that allows for read only operations. Can also be seen as a chopped
 * down version of {@link Index} that disallows mutating operations.
 *
 * @param <T> The Primitive this Index holds
 */
public interface ReadableIndex<T extends PropertyContainer>
{
    /**
     * @return the name of the index, i.e. the name this index was
     * created with.
     */
    String getName();

    /**
     * @return the type of entities are managed by this index.
     */
    Class<T> getEntityType();

    /**
     * Returns exact matches from this index, given the key/value pair. Matches
     * will be for key/value pairs just as they were added by the
     * {@link Index#add(PropertyContainer, String, Object)} method.
     * 
     * @param key the key in the key/value pair to match.
     * @param value the value in the key/value pair to match.
     * @return the result wrapped in an {@link IndexHits} object. If the entire
     *         result set isn't looped through, {@link IndexHits#close()} must
     *         be called before disposing of the result.
     */
    IndexHits<T> get( String key, Object value );

    /**
     * Returns matches from this index based on the supplied {@code key} and
     * query object, which can be a query string or an implementation-specific
     * query object.
     *
     * @param key the key in this query.
     * @param queryOrQueryObject the query for the {@code key} to match.
     * @return the result wrapped in an {@link IndexHits} object. If the entire
     * result set isn't looped through, {@link IndexHits#close()} must be
     * called before disposing of the result.
     */
    IndexHits<T> query( String key, Object queryOrQueryObject );

    /**
     * Returns matches from this index based on the supplied query object,
     * which can be a query string or an implementation-specific query object.
     *
     * @param queryOrQueryObject the query to match.
     * @return the result wrapped in an {@link IndexHits} object. If the entire
     * result set isn't looped through, {@link IndexHits#close()} must be
     * called before disposing of the result.
     */
    IndexHits<T> query( Object queryOrQueryObject );

    /**
     * A ReadableIndex is possible to support mutating operations as well. This
     * method returns true iff such operations are supported by the
     * implementation.
     * 
     * @return true iff mutating operations are supported.
     */
    boolean isWriteable();

    /**
     * Get the {@link GraphDatabaseService graph database} that owns this index.
     * @return the {@link GraphDatabaseService graph database} that owns this index.
     */
    GraphDatabaseService getGraphDatabase();
}
