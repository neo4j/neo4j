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

import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.explicitindex.ExplicitIndexNotFoundKernelException;

/**
 * Operations for creating and modifying explicit indexes.
 */
public interface ExplicitIndexWrite
{
    /**
     * Adds node to explicit index.
     *
     * @param indexName The name of the index
     * @param node The id of the node to add
     * @param key The key to associate with the node
     * @param value The value to associate with the node an key
     * @throws ExplicitIndexNotFoundKernelException If there is no explicit index with the given name
     */
    void nodeAddToExplicitIndex( String indexName, long node, String key, Object value )
            throws ExplicitIndexNotFoundKernelException;

    /**
     * Removes a node from an explicit index
     *
     * @param indexName The name of the index
     * @param node The id of the node to remove
     * @param key The key associated with the node
     * @param value The value associated with the node and key
     * @throws ExplicitIndexNotFoundKernelException If there is no explicit index with the given name
     */
    void nodeRemoveFromExplicitIndex( String indexName, long node, String key, Object value )
            throws ExplicitIndexNotFoundKernelException;

    /**
     * Removes a node from an explicit index
     *
     * @param indexName The name of the index
     * @param node The id of the node to remove
     * @param key The key associated with the node
     * @throws ExplicitIndexNotFoundKernelException If there is no explicit index with the given name
     */
    void nodeRemoveFromExplicitIndex( String indexName, long node, String key ) throws
            ExplicitIndexNotFoundKernelException;

    /**
     * Removes a given node from an explicit index
     *
     * @param indexName The name of the index from which the node is to be removed.
     * @param node The node id of the node to remove
     * @throws ExplicitIndexNotFoundKernelException If there is no explicit index with the given name
     */
    void nodeRemoveFromExplicitIndex( String indexName, long node ) throws ExplicitIndexNotFoundKernelException;

    /**
     * Drops the explicit index with the given name
     * @param indexName the index to drop
     */
    void nodeExplicitIndexDrop( String indexName ) throws ExplicitIndexNotFoundKernelException;

    /**
     * Updates configuration of the given index
     * @param indexName the name of the index
     * @param key the configuration key
     * @param value the value to be associated with the key
     * @return The old value associated with the key or <tt>null</tt> if nothing associated with the key.
     * @throws ExplicitIndexNotFoundKernelException if no such index exists
     */
    String nodeExplicitIndexSetConfiguration( String indexName, String key, String value )
            throws ExplicitIndexNotFoundKernelException;

    /**
     * Remove a configuration of the given index
     * @param indexName the name of the index
     * @param key the configuration key
     * @return The old value associated with the key or <tt>null</tt> if nothing associated with the key.
     * @throws ExplicitIndexNotFoundKernelException if no such index exists
     */
    String nodeExplicitIndexRemoveConfiguration( String indexName, String key )
            throws ExplicitIndexNotFoundKernelException;

    /**
     * Adds relationship to explicit index.
     *
     * @param indexName The name of the index
     * @param relationship The id of the relationship to add
     * @param key The key to associate with the relationship
     * @param value The value to associate with the relationship and key
     * @throws ExplicitIndexNotFoundKernelException If there is no explicit index with the given name
     */
    void relationshipAddToExplicitIndex( String indexName, long relationship, String key, Object value )
            throws ExplicitIndexNotFoundKernelException, EntityNotFoundException;

    /**
     * Removes relationship from explicit index.
     *
     * @param indexName The name of the index
     * @param relationship The id of the relationship to remove
     * @param key The key associated with the relationship
     * @param value The value associated with the relationship and key
     * @throws ExplicitIndexNotFoundKernelException If there is no explicit index with the given name
     */
    void relationshipRemoveFromExplicitIndex( String indexName, long relationship, String key, Object value )
            throws ExplicitIndexNotFoundKernelException;

    /**
     * Removes relationship to explicit index.
     *
     * @param indexName The name of the index
     * @param relationship The id of the relationship to remove
     * @param key The key associated with the relationship
     * @throws ExplicitIndexNotFoundKernelException If there is no explicit index with the given name
     */
    void relationshipRemoveFromExplicitIndex( String indexName, long relationship, String key )
            throws ExplicitIndexNotFoundKernelException;

    /**
     * Removes relationship to explicit index.
     *
     * @param indexName The name of the index
     * @param relationship The id of the relationship to remove
     * @throws ExplicitIndexNotFoundKernelException If there is no explicit index with the given name
     */
    void relationshipRemoveFromExplicitIndex( String indexName, long relationship )
            throws ExplicitIndexNotFoundKernelException;

    /**
     * Creates an explicit index in a separate transaction if not yet available.
     *
     * @param indexName The name of the index to create.
     * @param customConfig The configuration of the explicit index.
     */
    void nodeExplicitIndexCreateLazily( String indexName, Map<String,String> customConfig );

    /**
     * Creates an explicit index.
     *
     * @param indexName The name of the index to create.
     * @param customConfig The configuration of the explicit index.
     */
    void nodeExplicitIndexCreate( String indexName, Map<String,String> customConfig );

    /**
     * Creates an explicit index in a separate transaction if not yet available.
     *
     * @param indexName The name of the index to create.
     * @param customConfig The configuration of the explicit index.
     */
    void relationshipExplicitIndexCreateLazily( String indexName, Map<String,String> customConfig );

    /**
     * Creates an explicit index.
     *
     * @param indexName The name of the index to create.
     * @param customConfig The configuration of the explicit index.
     */
    void relationshipExplicitIndexCreate( String indexName, Map<String,String> customConfig );

    /**
     * Drops the explicit index with the given name
     * @param indexName the index to drop
     */
    void relationshipExplicitIndexDrop( String indexName ) throws ExplicitIndexNotFoundKernelException;

    /**
     * Updates configuration of the given index
     * @param indexName the name of the index
     * @param key the configuration key
     * @param value the value to be associated with the key
     * @return The old value associated with the key or <tt>null</tt> if nothing associated with the key.
     * @throws ExplicitIndexNotFoundKernelException if no such index exists
     */
    String relationshipExplicitIndexSetConfiguration( String indexName, String key, String value )
            throws ExplicitIndexNotFoundKernelException;

    /**
     * Remove a configuration of the given index
     * @param indexName the name of the index
     * @param key the configuration key
     * @return The old value associated with the key or <tt>null</tt> if nothing associated with the key.
     * @throws ExplicitIndexNotFoundKernelException if no such index exists
     */
    String relationshipExplicitIndexRemoveConfiguration( String indexName, String key )
            throws ExplicitIndexNotFoundKernelException;

}
