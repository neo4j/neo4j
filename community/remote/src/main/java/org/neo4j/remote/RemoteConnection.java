/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.remote;

import org.neo4j.graphdb.Direction;

/**
 * Represents a connection to a remote site.
 * 
 * @author Tobias Ivarsson
 */
public interface RemoteConnection
{
    /**
     * Co-configure the client and the server for this connection.
     * 
     * @param config
     *            An object that represents the configuration of the client.
     * @return An object that can set up the client according to the agreed
     *         configuration.
     */
    ClientConfigurator configure( Configuration config );

    /**
     * Close the remote connection, rolling back all active transactions.
     */
    void close();

    /**
     * Start a new transaction.
     * 
     * @return an id that represents the transaction.
     */
    int beginTransaction();

    /**
     * Commit a transaction.
     * 
     * @param transactionId
     *            The id that represents the transaction to be committed.
     */
    void commit( int transactionId );

    /**
     * Roll back a transaction.
     * 
     * @param transactionId
     *            The id that represents the transaction to be rolled back.
     */
    void rollback( int transactionId );

    /**
     * Get the relationship types that are registered with the server.
     * 
     * @param transactionId
     *            The id that represents the transaction the operation is
     *            executed in.
     * @return A serialized iterator containing the names of the relationship
     *         types.
     */
    RemoteResponse<IterableSpecification<String>> getRelationshipTypes(
        int transactionId );

    /**
     * Get the next chunk of the lazy iterator of relationship types.
     * 
     * @param transactionId
     *            The id that represents the transaction the operation is
     *            executed in.
     * @param requestToken
     *            An id that represents the request.
     * @return A serialized iterator containing the names of the relationship
     *         types.
     */
    RemoteResponse<IterableSpecification<String>> getMoreRelationshipTypes(
        int transactionId, int requestToken );

    /**
     * Close an iterator over relationship types.
     * 
     * @param transactionId
     *            The id that represents the transaction the operation is
     *            executed in.
     * @param requestToken
     *            An id that represents the request.
     * @return nothing.
     */
    RemoteResponse<Void> closeRelationshipTypeIterator( int transactionId,
        int requestToken );

    /**
     * Create a new node.
     * 
     * @param transactionId
     *            the id of the transaction to create the node in.
     * @return A serialized representation of the created node.
     */
    RemoteResponse<NodeSpecification> createNode( int transactionId );

    /**
     * Get the reference node.
     * 
     * @param transactionId
     *            the id of the transaction to get the reference node in.
     * @return A serialized representation of the reference node.
     */
    RemoteResponse<NodeSpecification> getReferenceNode( int transactionId );

    /**
     * Check if a node with the specified id exists.
     * 
     * @param transactionId
     *            the transaction to check for the node in.
     * @param nodeId
     *            the id of the node to check for.
     * @return <code>true</code> if the node exists, <code>false</code>
     *         otherwise.
     */
    RemoteResponse<Boolean> hasNodeWithId( int transactionId, long nodeId );

    /**
     * Delete a node.
     * 
     * @param transactionId
     *            the id of the transaction to delete the node in.
     * @param nodeId
     *            the id of the node to delete.
     * @return nothing.
     */
    RemoteResponse<Void> deleteNode( int transactionId, long nodeId );

    /**
     * Get all nodes.
     * 
     * @param transactionId
     *            the id of the transaction to get all nodes in.
     * @return A serialized iterator containing nodes.
     */
    RemoteResponse<IterableSpecification<NodeSpecification>> getAllNodes(
        int transactionId );

    /**
     * Get the next chunk of the lazy iterator of nodes.
     * 
     * @param transactionId
     *            The id that represents the transaction the operation is
     *            executed in.
     * @param requestToken
     *            An id that represents the request.
     * @return A serialized iterator containing nodes.
     */
    RemoteResponse<IterableSpecification<NodeSpecification>> getMoreNodes(
        int transactionId, int requestToken );

    /**
     * Close an iterator over nodes.
     * 
     * @param transactionId
     *            The id that represents the transaction the operation is
     *            executed in.
     * @param requestToken
     *            An id that represents the request.
     * @return nothing.
     */
    RemoteResponse<Void> closeNodeIterator( int transactionId, int requestToken );

    /**
     * Create a new relationship.
     * 
     * @param transactionId
     *            the id of the transaction to create the relationship in.
     * @param relationshipTypeName
     *            the type name of the relationship to create.
     * @param startNodeId
     *            the id of the start node for the relationship.
     * @param endNodeId
     *            the id of the end node of the relationship.
     * @return A serialized representation of the created relationship.
     */
    RemoteResponse<RelationshipSpecification> createRelationship(
        int transactionId, String relationshipTypeName, long startNodeId,
        long endNodeId );

    /**
     * Get a relationship.
     * 
     * @param transactionId
     *            the id of the transaction to get the relationship in.
     * @param relationshipId
     *            the id of the relationship to get.
     * @return A serialized representation of the requested relationship.
     */
    RemoteResponse<RelationshipSpecification> getRelationshipById(
        int transactionId, long relationshipId );

    /**
     * Get all relationships from a given node.
     * 
     * @param transactionId
     *            the id of the transaction to get the relationships in.
     * @param nodeId
     *            the id of the node to get the relationships from.
     * @param direction
     *            the direction of the relationships from the node.
     * @return A serialized iterator of relationships.
     */
    RemoteResponse<IterableSpecification<RelationshipSpecification>> getAllRelationships(
        int transactionId, long nodeId, Direction direction );

    /**
     * Get relationships from a given node.
     * 
     * @param transactionId
     *            the id of the transaction to get the relationships in.
     * @param nodeId
     *            the id of the node to get the relationships from.
     * @param direction
     *            the direction of the relationships from the node.
     * @param relationshipTypeNames
     *            the names of the relationship types to get.
     * @return A serialized iterator of relationships.
     */
    RemoteResponse<IterableSpecification<RelationshipSpecification>> getRelationships(
        int transactionId, long nodeId, Direction direction,
        String[] relationshipTypeNames );

    /**
     * Get the next chunk of the lazy iterator of relationships.
     * 
     * @param transactionId
     *            The id that represents the transaction the operation is
     *            executed in.
     * @param requestToken
     *            An id that represents the request.
     * @return A serialized iterator containing relationships.
     */
    RemoteResponse<IterableSpecification<RelationshipSpecification>> getMoreRelationships(
        int transactionId, int requestToken );

    /**
     * Close an iterator over relationships.
     * 
     * @param transactionId
     *            The id that represents the transaction the operation is
     *            executed in.
     * @param requestToken
     *            An id that represents the request.
     * @return nothing.
     */
    RemoteResponse<Void> closeRelationshipIterator( int transactionId,
        int requestToken );

    /**
     * Delete a relationship.
     * 
     * @param transactionId
     *            the id of the transaction to delete the relationship in.
     * @param relationshipId
     *            the id of the relationship to delete.
     * @return nothing.
     */
    RemoteResponse<Void> deleteRelationship( int transactionId,
        long relationshipId );

    /**
     * Get a property from a node.
     * 
     * @param transactionId
     *            the id of the transaction to get the property in.
     * @param nodeId
     *            the id of the node to get the property from.
     * @param key
     *            the key for the property.
     * @return the property value.
     */
    RemoteResponse<Object> getNodeProperty( int transactionId, long nodeId,
        String key );

    /**
     * Get a property from a relationship.
     * 
     * @param transactionId
     *            the id of the transaction to get the property in.
     * @param relationshipId
     *            the id of the relationship to get the property from.
     * @param key
     *            the key for the property.
     * @return the property value.
     */
    RemoteResponse<Object> getRelationshipProperty( int transactionId,
        long relationshipId, String key );

    /**
     * Set a property from a node.
     * 
     * @param transactionId
     *            the id of the transaction to get the property in.
     * @param nodeId
     *            the id of the node to get the property from.
     * @param key
     *            the key for the property.
     * @param value
     *            the new value for the property.
     * @return nothing.
     */
    RemoteResponse<Object> setNodeProperty( int transactionId, long nodeId,
        String key, Object value );

    /**
     * Set a property from a relationship.
     * 
     * @param transactionId
     *            the id of the transaction to get the property in.
     * @param relationshipId
     *            the id of the relationship to get the property from.
     * @param key
     *            the key for the property.
     * @param value
     *            the new value for the property.
     * @return nothing.
     */
    RemoteResponse<Object> setRelationshipProperty( int transactionId,
        long relationshipId, String key, Object value );

    /**
     * Get the property keys for a node.
     * 
     * @param transactionId
     *            the id of the transaction to get the property keys in.
     * @param nodeId
     *            the id of the node to get the property keys from.
     * @return a serialized iterator of property keys.
     */
    RemoteResponse<IterableSpecification<String>> getNodePropertyKeys(
        int transactionId, long nodeId );

    /**
     * Get the property keys for a relationship.
     * 
     * @param transactionId
     *            the id of the transaction to get the property keys in.
     * @param relationshipId
     *            the id of the relationship to get the property keys from.
     * @return a serialized iterator of property keys.
     */
    RemoteResponse<IterableSpecification<String>> getRelationshipPropertyKeys(
        int transactionId, long relationshipId );

    /**
     * Get the next chunk of the lazy iterator of property keys.
     * 
     * @param transactionId
     *            The id that represents the transaction the operation is
     *            executed in.
     * @param requestToken
     *            An id that represents the request.
     * @return A serialized iterator of property keys.
     */
    RemoteResponse<IterableSpecification<String>> getMorePropertyKeys(
        int transactionId, int requestToken );

    /**
     * Close an iterator over property keys.
     * 
     * @param transactionId
     *            The id that represents the transaction the operation is
     *            executed in.
     * @param requestToken
     *            An id that represents the request.
     * @return nothing.
     */
    RemoteResponse<Void> closePropertyKeyIterator( int transactionId,
        int requestToken );

    /**
     * Check if a node has a specific property.
     * 
     * @param transactionId
     *            the id of the transaction to check for the property in.
     * @param nodeId
     *            the id of the node where the property should be sought after.
     * @param key
     *            the property key.
     * @return <code>true</code> if the node has ha property with the given key,
     *         <code>false</code> otherwise.
     */
    RemoteResponse<Boolean> hasNodeProperty( int transactionId, long nodeId,
        String key );

    /**
     * Check if a relationship has a specific property.
     * 
     * @param transactionId
     *            the id of the transaction to check for the property in.
     * @param relationshipId
     *            the id of the relationship where the property should be sought
     *            after.
     * @param key
     *            the property key.
     * @return <code>true</code> if the relationship has ha property with the
     *         given key, <code>false</code> otherwise.
     */
    RemoteResponse<Boolean> hasRelationshipProperty( int transactionId,
        long relationshipId, String key );

    /**
     * Remove a property from a node.
     * 
     * @param transactionId
     *            the id of the transaction to remove the property in.
     * @param nodeId
     *            the id of the node to remove the property from.
     * @param key
     *            the property key.
     * @return the value of the property.
     */
    RemoteResponse<Object> removeNodeProperty( int transactionId, long nodeId,
        String key );

    /**
     * Remove a property from a relationship.
     * 
     * @param transactionId
     *            the id of the transaction to remove the property in.
     * @param relationshipId
     *            the id of the relationship to remove the property from.
     * @param key
     *            the property key.
     * @return the value of the property.
     */
    RemoteResponse<Object> removeRelationshipProperty( int transactionId,
        long relationshipId, String key );

    // Indexing

    /**
     * Get the id of an index service.
     * 
     * @param indexName
     *            a string token that identifies the index service.
     * @return the id of the identified index service.
     */
    RemoteResponse<Integer> getIndexServiceId( String indexName );

    /**
     * Get all nodes stored under a specific value in a specific index.
     * 
     * @param transactionId
     *            the id of the transaction to get the nodes in.
     * @param indexId
     *            the id of the index service to get the nodes from.
     * @param key
     *            the key for the index to get the nodes from.
     * @param value
     *            the value that the nodes are stored under.
     * @return A serialized iterator containing nodes.
     */
    RemoteResponse<IterableSpecification<NodeSpecification>> getIndexNodes(
        int transactionId, int indexId, String key, Object value );

    /**
     * Store a node in an index.
     * 
     * @param transactionId
     *            the id of the transaction modify the index in.
     * @param indexId
     *            the id of the index service to modify.
     * @param nodeId
     *            the id of the node to store in the index.
     * @param key
     *            the key for the index to store the node in.
     * @param value
     *            the value that the node should be stored under.
     * @return nothing.
     */
    RemoteResponse<Void> indexNode( int transactionId, int indexId,
        long nodeId, String key, Object value );

    /**
     * Remove a node from an index.
     * 
     * @param transactionId
     *            the id of the transaction modify the index in.
     * @param indexId
     *            the id of the index service to modify.
     * @param nodeId
     *            the id of the node to remove from the index.
     * @param key
     *            the key for the index the node is stored in.
     * @param value
     *            the value that the node is stored under.
     * @return nothing.
     */
    RemoteResponse<Void> removeIndexNode( int transactionId, int indexId,
        long nodeId, String key, Object value );

    /**
     * Remove a node from an index.
     * 
     * @param transactionId
     *            the id of the transaction modify the index in.
     * @param indexId
     *            the id of the index service to modify.
     * @param nodeId
     *            the id of the node to remove from the index.
     * @param key
     *            the key for the index the node is stored in.
     * @return nothing.
     */
    RemoteResponse<Void> removeIndexNode( int transactionId, int indexId,
            long nodeId, String key );

    /**
     * Clear an index.
     * 
     * @param transactionId
     *            the id of the transaction modify the index in.
     * @param indexId
     *            the id of the index service to modify.
     * @param key
     *            the key for the index the node is stored in.
     * @return nothing.
     */
    RemoteResponse<Void> removeIndexNode( int transactionId, int indexId,
            String key );
}
