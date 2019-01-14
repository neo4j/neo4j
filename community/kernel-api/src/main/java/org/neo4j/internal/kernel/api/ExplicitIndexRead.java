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
package org.neo4j.internal.kernel.api;

import java.util.Map;

import org.neo4j.internal.kernel.api.exceptions.explicitindex.ExplicitIndexNotFoundKernelException;

/**
 * Operations for querying and seeking in explicit indexes.
 */
public interface ExplicitIndexRead
{
    /**
     * Finds item from explicit index
     *
     * @param cursor the cursor to use for consuming the result
     * @param index the name of the explicit index
     * @param key the key to find
     * @param value the value corresponding to the key
     * @throws ExplicitIndexNotFoundKernelException if index is not there
     */
    void nodeExplicitIndexLookup( NodeExplicitIndexCursor cursor, String index, String key, Object value )
            throws ExplicitIndexNotFoundKernelException;

    /**
     * Queries explicit index
     *
     * @param cursor the cursor to use for consuming the result
     * @param index the name of the explicit index
     * @param query the query object
     * @throws ExplicitIndexNotFoundKernelException if index is not there
     */
    void nodeExplicitIndexQuery( NodeExplicitIndexCursor cursor, String index, Object query )
            throws ExplicitIndexNotFoundKernelException;

    /**
     * Queries explicit index
     *
     * @param cursor the cursor to use for consuming the result
     * @param index the name of the explicit index
     * @param key the key to find
     * @param query the query object
     * @throws ExplicitIndexNotFoundKernelException if index is not there
     */
    void nodeExplicitIndexQuery( NodeExplicitIndexCursor cursor, String index, String key, Object query )
            throws ExplicitIndexNotFoundKernelException;

    /**
     * Check whether a node index with the given name exists.
     *
     * @param indexName name of node index to check for existence.
     * @param customConfiguration if {@code null} the configuration of existing won't be matched, otherwise it will
     * be matched and a mismatch will throw {@link IllegalArgumentException}.
     * @return whether or not node explicit index with name {@code indexName} exists.
     * @throws IllegalArgumentException on index existence with provided name, but mismatching {@code customConfiguration}.
     */
    boolean nodeExplicitIndexExists( String indexName, Map<String,String> customConfiguration );

    /**
     * Return the configuration of the given index
     * @param indexName the name of the index
     * @return the configuration of the index with the given name
     * @throws ExplicitIndexNotFoundKernelException if the index is not there
     */
    Map<String, String> nodeExplicitIndexGetConfiguration( String indexName )
            throws ExplicitIndexNotFoundKernelException;

    /**
     * Finds item from explicit index
     *
     * @param cursor the cursor to use for consuming the result
     * @param index the name of the explicit index
     * @param key the key to find
     * @param value the value corresponding to the key
     * @throws ExplicitIndexNotFoundKernelException if index is not there
     */
    void relationshipExplicitIndexLookup(
            RelationshipExplicitIndexCursor cursor, String index, String key, Object value, long source, long target )
            throws ExplicitIndexNotFoundKernelException;

    /**
     * Queries explicit index
     *
     * @param cursor the cursor to use for consuming the result
     * @param index the name of the explicit index
     * @param query the query object
     * @param source the source node or <code>-1</code> if any
     * @param target the source node or <code>-1</code> if any
     * @throws ExplicitIndexNotFoundKernelException if index is not there
     */
    void relationshipExplicitIndexQuery(
            RelationshipExplicitIndexCursor cursor, String index, Object query, long source, long target )
            throws ExplicitIndexNotFoundKernelException;

    /**
     * Queries explicit index
     *
     * @param cursor the cursor to use for consuming the result
     * @param index the name of the explicit index
     * @param key the key to find
     * @param query the query object
     * @param source the source node or <code>-1</code> if any
     * @param target the source node or <code>-1</code> if any
     * @throws ExplicitIndexNotFoundKernelException if index is not there
     */
    void relationshipExplicitIndexQuery(
            RelationshipExplicitIndexCursor cursor, String index, String key, Object query, long source, long target )
            throws ExplicitIndexNotFoundKernelException;

    /**
     * Check whether a relationship index with the given name exists.
     *
     * @param indexName name of relationship index to check for existence.
     * @param customConfiguration if {@code null} the configuration of existing won't be matched, otherwise it will
     * be matched and a mismatch will throw {@link IllegalArgumentException}.
     * @return whether or not relationship explicit index with name {@code indexName} exists.
     * @throws IllegalArgumentException on index existence with provided name, but mismatching {@code customConfiguration}.
     */
    boolean relationshipExplicitIndexExists( String indexName, Map<String,String> customConfiguration );

    /**
     * Retrieve all node explicit indexes
     * @return the names of all node explicit indexes
     */
    String[] nodeExplicitIndexesGetAll();

    /**
     * Retrieve all relationship explicit indexes
     * @return the names of all relationship explicit indexes
     */
    String[] relationshipExplicitIndexesGetAll();

    /**
     * Return the configuration of the given index
     * @param indexName the name of the index
     * @return the configuration of the index with the given name
     * @throws ExplicitIndexNotFoundKernelException if the index doesn't exist
     */
    Map<String, String> relationshipExplicitIndexGetConfiguration( String indexName )
            throws ExplicitIndexNotFoundKernelException;
}
