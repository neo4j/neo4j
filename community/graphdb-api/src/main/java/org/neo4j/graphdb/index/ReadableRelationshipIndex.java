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

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

/**
 * Extends the {@link ReadableIndex} interface with additional get/query methods
 * which
 * are specific to {@link Relationship}s. Each of {@link #get(String, Object)},
 * {@link #query(String, Object)} and {@link #query(Object)} have an additional
 * method which allows efficient filtering on start/end node of the
 * relationships.
 *
 * @author Mattias Persson
 */
public interface ReadableRelationshipIndex extends ReadableIndex<Relationship>
{
    /**
     * Returns exact matches from this index, given the key/value pair. Matches
     * will be for key/value pairs just as they were added by the
     * {@link Index#add(org.neo4j.graphdb.PropertyContainer, String, Object)} method.
     * 
     * @param key the key in the key/value pair to match.
     * @param valueOrNull the value in the key/value pair to match.
     * @param startNodeOrNull filter so that only {@link Relationship}s with
     *            that given start node will be returned.
     * @param endNodeOrNull filter so that only {@link Relationship}s with that
     *            given end node will be returned.
     * @return the result wrapped in an {@link IndexHits} object. If the entire
     *         result set isn't looped through, {@link IndexHits#close()} must
     *         be called before disposing of the result.
     */
    IndexHits<Relationship> get( String key, Object valueOrNull, Node startNodeOrNull,
            Node endNodeOrNull );

    /**
     * Returns matches from this index based on the supplied {@code key} and
     * query object, which can be a query string or an implementation-specific
     * query object.
     *
     * @param key the key in this query.
     * @param queryOrQueryObjectOrNull the query for the {@code key} to match.
     * @param startNodeOrNull filter so that only {@link Relationship}s with
     *            that given start node will be returned.
     * @param endNodeOrNull filter so that only {@link Relationship}s with that
     *            given end node will be returned.
     * @return the result wrapped in an {@link IndexHits} object. If the entire
     *         result set isn't looped through, {@link IndexHits#close()} must
     *         be called before disposing of the result.
     */
    IndexHits<Relationship> query( String key, Object queryOrQueryObjectOrNull,
            Node startNodeOrNull, Node endNodeOrNull );

    /**
     * Returns matches from this index based on the supplied query object, which
     * can be a query string or an implementation-specific query object.
     *
     * @param queryOrQueryObjectOrNull the query to match.
     * @param startNodeOrNull filter so that only {@link Relationship}s with
     *            that given start node will be returned.
     * @param endNodeOrNull filter so that only {@link Relationship}s with that
     *            given end node will be returned.
     * @return the result wrapped in an {@link IndexHits} object. If the entire
     *         result set isn't looped through, {@link IndexHits#close()} must
     *         be called before disposing of the result.
     */
    IndexHits<Relationship> query( Object queryOrQueryObjectOrNull, Node startNodeOrNull,
            Node endNodeOrNull );
}
