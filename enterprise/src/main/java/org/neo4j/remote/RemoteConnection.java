/*
 * Copyright 2008 Network Engine for Objects in Lund AB [neotechnology.com]
 * 
 * This program is free software: you can redistribute it and/or modify
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.remote;

import org.neo4j.api.core.Direction;

/**
 * Represents a connection to a remote site.
 * @author Tobias Ivarsson
 */
public interface RemoteConnection
{
    /**
     * Co-configure the client and the server for this connection.
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
     * @return an id that represents the transaction.
     */
    int beginTransaction();

    /**
     * Commit a transaction.
     * @param transactionId
     *            The id that represents the transaction to be committed.
     */
    void commit( int transactionId );

    /**
     * Roll back a transaction.
     * @param transactionId
     *            The id that represents the transaction to be rolled back.
     */
    void rollback( int transactionId );

    RemoteResponse<IterableSpecification<String>> getRelationshipTypes(
        int transactionId );

    RemoteResponse<IterableSpecification<String>> getMoreRelationshipTypes(
        int transactionId, int requestToken );

    RemoteResponse<NodeSpecification> createNode( int transactionId );

    RemoteResponse<NodeSpecification> getReferenceNode( int transactionId );

    RemoteResponse<Boolean> hasNodeWithId( int transactionId, long nodeId );

    RemoteResponse<Void> deleteNode( int transactionId, long nodeId );

    RemoteResponse<IterableSpecification<NodeSpecification>> getMoreNodes(
        int transactionId, int requestToken );

    RemoteResponse<RelationshipSpecification> createRelationship(
        int transactionId, String relationshipTypeName, long startNodeId,
        long endNodeId );

    RemoteResponse<RelationshipSpecification> getRelationshipById(
        int transactionId, long relationshipId );

    RemoteResponse<IterableSpecification<RelationshipSpecification>> getAllRelationships(
        int transactionId, long nodeId, Direction direction );

    RemoteResponse<IterableSpecification<RelationshipSpecification>> getRelationships(
        int transactionId, long nodeId, Direction direction,
        String[] relationshipTypeNames );

    RemoteResponse<IterableSpecification<RelationshipSpecification>> getMoreRelationships(
        int transactionId, int requestToken );

    RemoteResponse<Void> deleteRelationship( int transactionId,
        long relationshipId );

    RemoteResponse<Object> getNodeProperty( int transactionId, long nodeId,
        String key );

    RemoteResponse<Object> getRelationshipProperty( int transactionId,
        long relationshipId, String key );

    RemoteResponse<Object> setNodeProperty( int transactionId, long nodeId,
        String key, Object value );

    RemoteResponse<Object> setRelationshipProperty( int transactionId,
        long relationshipId, String key, Object value );

    RemoteResponse<IterableSpecification<String>> getNodePropertyKeys(
        int transactionId, long nodeId );

    RemoteResponse<IterableSpecification<String>> getRelationshipPropertyKeys(
        int transactionId, long relationshipId );

    RemoteResponse<IterableSpecification<String>> getMorePropertyKeys(
        int transactionId, int requestToken );

    RemoteResponse<Boolean> hasNodeProperty( int transactionId, long nodeId,
        String key );

    RemoteResponse<Boolean> hasRelationshipProperty( int transactionId,
        long relationshiId, String key );

    RemoteResponse<Object> removeNodeProperty( int transactionId, long nodeId,
        String key );

    RemoteResponse<Object> removeRelationshipProperty( int transactionId,
        long relationshipId, String key );

    // Indexing
    
    RemoteResponse<Integer> getIndexId(String indexName);

    RemoteResponse<IterableSpecification<NodeSpecification>> getIndexNodes(
        int transactionId, int indexId, String key, Object value );

    RemoteResponse<Void> indexNode( int transactionId, int indexId,
        long nodeId, String key, Object value );

    RemoteResponse<Void> removeIndexNode( int transactionId, int indexId,
        long nodeId, String key, Object value );
}
