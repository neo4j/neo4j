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

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;

/**
 * A one stop shop for accessing {@link Index}s for {@link Node}s
 * and {@link Relationship}s. An {@link IndexManager} is paired with a
 * {@link GraphDatabaseService} via {@link GraphDatabaseService#index()} so that
 * indexes can be accessed directly from the graph database.
 */
public interface IndexManager
{
    /**
     * The configuration key to use for specifying which provider an index
     * will have, i.e. which implementation will be used to back that index.
     */
    String PROVIDER = "provider";

    /**
     * Returns whether or not there exists a node index with the name
     * {@code indexName}. Indexes are created when needed in calls to
     * {@link #forNodes(String)} and {@link #forNodes(String, Map)}.
     * @param indexName the name of the index to check.
     * @return whether or not there exists a node index with the name
     * {@code indexName}.
     */
    boolean existsForNodes( String indexName );

    /**
     * Returns an {@link Index} for {@link Node}s with the name {@code indexName}.
     * If such an index doesn't exist it will be created with default configuration.
     * Indexes created with {@link #forNodes(String, Map)} can be returned by this
     * method also, so that you don't have to supply and match its configuration
     * for consecutive accesses.
     *
     * This is the prefered way of accessing indexes, whether they were created with
     * {@link #forNodes(String)} or {@link #forNodes(String, Map)}.
     *
     * @param indexName the name of the node index.
     * @return the {@link Index} corresponding to the {@code indexName}.
     */
    Index<Node> forNodes( String indexName );

    /**
     * Returns an {@link Index} for {@link Node}s with the name {@code indexName}.
     * If the index exists it will be returned if the provider and customConfiguration
     * matches, else an {@link IllegalArgumentException} will be thrown.
     * If the index doesn't exist it will be created with the given
     * provider (given in the configuration map).
     *
     * @param indexName the name of the index to create.
     * @param customConfiguration configuration for the index being created.
     * Use the <b>provider</b> key to control which index implementation,
     * i.e. the {@link IndexImplementation} to use for this index if it's created. The
     * value represents the service name corresponding to the {@link IndexImplementation}.
     * Other options can f.ex. say that the index will be a fulltext index and that it
     * should be case insensitive. The parameters given here (except "provider") are
     * only interpreted by the implementation represented by the provider.
     * @return a named {@link Index} for {@link Node}s
     */
    Index<Node> forNodes( String indexName, Map<String, String> customConfiguration );

    /**
     * Returns the names of all existing {@link Node} indexes.
     * Those names can then be used to get to the actual {@link Index}
     * instances.
     *
     * @return the names of all existing {@link Node} indexes.
     */
    String[] nodeIndexNames();

    /**
     * Returns whether or not there exists a relationship index with the name
     * {@code indexName}. Indexes are created when needed in calls to
     * {@link #forRelationships(String)} and {@link #forRelationships(String, Map)}.
     * @param indexName the name of the index to check.
     * @return whether or not there exists a relationship index with the name
     * {@code indexName}.
     */
    boolean existsForRelationships( String indexName );

    /**
     * Returns an {@link Index} for {@link Relationship}s with the name {@code indexName}.
     * If such an index doesn't exist it will be created with default configuration.
     * Indexes created with {@link #forRelationships(String, Map)} can be returned by this
     * method also, so that you don't have to supply and match its configuration
     * for consecutive accesses.
     *
     * This is the prefered way of accessing indexes, whether they were created with
     * {@link #forRelationships(String)} or {@link #forRelationships(String, Map)}.
     *
     * @param indexName the name of the node index.
     * @return the {@link Index} corresponding to the {@code indexName}.
     */
    RelationshipIndex forRelationships( String indexName );

    /**
     * Returns an {@link Index} for {@link Relationship}s with the name {@code indexName}.
     * If the index exists it will be returned if the provider and customConfiguration
     * matches, else an {@link IllegalArgumentException} will be thrown.
     * If the index doesn't exist it will be created with the given
     * provider (given in the configuration map).
     *
     * @param indexName the name of the index to create.
     * @param customConfiguration configuration for the index being created.
     * Use the <b>provider</b> key to control which index implementation,
     * i.e. the {@link IndexImplementation} to use for this index if it's created. The
     * value represents the service name corresponding to the {@link IndexImplementation}.
     * Other options can f.ex. say that the index will be a fulltext index and that it
     * should be case insensitive. The parameters given here (except "provider") are
     * only interpreted by the implementation represented by the provider.
     * @return a named {@link Index} for {@link Relationship}s
     */
    RelationshipIndex forRelationships( String indexName,
            Map<String, String> customConfiguration );

    /**
     * Returns the names of all existing {@link Relationship} indexes.
     * Those names can then be used to get to the actual {@link Index}
     * instances.
     *
     * @return the names of all existing {@link Relationship} indexes.
     */
    String[] relationshipIndexNames();

    /**
     * Returns the configuration for {@code index}. Configuration can be
     * set when creating an index, with f.ex {@link #forNodes(String, Map)}
     * or with {@link #setConfiguration(Index, String, String)} or
     * {@link #removeConfiguration(Index, String)}.
     *
     * @param index the index to get the configuration for
     * @return configuration for the {@code index}.
     */
    Map<String, String> getConfiguration( Index<? extends PropertyContainer> index );

    /**
     * EXPERT: Sets a configuration parameter for an index. If a configuration
     * parameter with the given {@code key} it will be overwritten.
     *
     * WARNING: Overwriting parameters which controls the storage format of index
     * data may lead to existing index data being unusable.
     *
     * The key "provider" is a reserved parameter and cannot be overwritten,
     * if key is "provider" then an {@link IllegalArgumentException} will be thrown.
     *
     * @param index the index to set a configuration parameter for.
     * @param key the configuration parameter key.
     * @param value the new value of the configuration parameter.
     * @return the overwritten value if any.
     */
    String setConfiguration( Index<? extends PropertyContainer> index, String key, String value );

    /**
     * EXPERT: Removes a configuration parameter from an index. If there's no
     * value for the given {@code key} nothing will happen and {@code null}
     * will be returned.
     *
     * WARNING: Removing parameters which controls the storage format of index
     * data may lead to existing index data being unusable.
     *
     * The key "provider" is a reserved parameter and cannot be removed,
     * if key is "provider" then an {@link IllegalArgumentException} will be thrown.
     *
     * @param index the index to remove a configuration parameter from.
     * @param key the configuration parameter key.
     * @return the removed value if any.
     */
    String removeConfiguration( Index<? extends PropertyContainer> index, String key );

    /**
     * @deprecated this feature will be removed in a future release, please consider using schema indexes instead
     * @return the auto indexing manager for nodes
     */
    @Deprecated
    AutoIndexer<Node> getNodeAutoIndexer();

    /**
     * @deprecated this feature will be removed in a future release, please consider using schema indexes instead
     * @return the auto indexing manager for relationships
     */
    @Deprecated
    RelationshipAutoIndexer getRelationshipAutoIndexer();
}
