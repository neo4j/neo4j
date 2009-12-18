/*
 * Copyright (c) 2008-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.remote.inspect;

import org.neo4j.api.core.Direction;
import org.neo4j.remote.RemoteResponse;

/**
 * A listener for events in the {@link InspectionSite}.
 * @author Tobias Ivarsson
 */
public interface Inspector
{
    /**
     * Invoked when a new connection is opened.
     * @return A callback object to get the resulting status of the event call.
     */
    CallBack<Void> open();

    /**
     * Invoked when a connection is closed.
     * @return A callback object to get the resulting status of the event call.
     */
    CallBack<Void> close();

    /**
     * Invoked when a new transaction starts.
     * @return A callback object to get the resulting status of the event call.
     */
    CallBack<Integer> beginTransaction();

    /**
     * Invoked when a new node is created.
     * @param transactionId
     *            the id of the transaction.
     * @return A callback object to get the resulting status of the event call.
     */
    CallBack<RemoteResponse> createNode( int transactionId );

    /**
     * Invoked when a new relationship is created.
     * @param transactionId
     *            the id of the transaction.
     * @param startNodeId
     *            the id of the node where the relationship starts.
     * @param endNodeId
     *            the id of the node where the relationship ends.
     * @param relationshipTypeName
     *            the name of the type of the relationship.
     * @return A callback object to get the resulting status of the event call.
     */
    CallBack<RemoteResponse> createRelationship( int transactionId,
        long startNodeId, long endNodeId, String relationshipTypeName );

    /**
     * Invoked when a relationship is requested (by id).
     * @param transactionId
     *            the id of the transaction.
     * @param relationshipId
     *            the id of the relationship.
     * @return A callback object to get the resulting status of the event call.
     */
    CallBack<RemoteResponse> fetchRelationship( int transactionId,
        long relationshipId );

    /**
     * Invoked when the properties are fetched for a node.
     * @param transactionId
     *            the id of the transaction.
     * @param nodeId
     *            the id of the node.
     * @return A callback object to get the resulting status of the event call.
     */
    CallBack<RemoteResponse> fetchNodeProperties( int transactionId, long nodeId );

    /**
     * Invoked when the properites are fetched for a relationship.
     * @param transactionId
     *            the id of the transaction.
     * @param relationshipId
     *            the id of the relationship.
     * @return A callback object to get the resulting status of the event call.
     */
    CallBack<RemoteResponse> fetchRelationshipProperties( int transactionId,
        long relationshipId );

    /**
     * Invoked when the relationships are fetched for a node.
     * @param transactionId
     *            the id of the transaction.
     * @param rootNodeId
     *            the id of the root node.
     * @param direction
     *            the direction of the relationships.
     * @param typeNames
     *            the name of the types of the relationships.
     * @return A callback object to get the resulting status of the event call.
     */
    CallBack<RemoteResponse> fetchRelationships( int transactionId,
        long rootNodeId, Direction direction, String[] typeNames );

    /**
     * Invoked when a transaction is committed. Actually this method is called
     * when the commit starts, after that some data transmission methods are
     * called ({@link #deleteNode(long)},{@link #deleteRelationship(long)},
     * {@link #setNodeProperty(long, String, Object)},
     * {@link #setRelationshipProperty(long, String, Object)}), and after that
     * the {@link CallBack#success(Object)} method is called on the callback
     * returned from this method.
     * @param transactionId
     *            the id of the transaction.
     * @return A callback object to get the resulting status of the event call.
     */
    CallBack<Void> commit( int transactionId );

    /**
     * Invoked when a transaction is rolled back.
     * @param transactionId
     *            the id of the transaction.
     * @return A callback object to get the resulting status of the event call.
     */
    CallBack<Void> rollback( int transactionId );

    /**
     * Invoked for a removed node.
     * @param nodeId
     *            the id of the node.
     * @return A callback object to get the resulting status of the event call.
     */
    CallBack<Void> deleteNode( long nodeId );

    /**
     * Invoked for a removed relationship.
     * @param relationshipId
     *            the id of the relationship.
     * @return A callback object to get the resulting status of the event call.
     */
    CallBack<Void> deleteRelationship( long relationshipId );

    /**
     * Invoked for a changed node property.
     * @param id
     *            the id of the node.
     * @param key
     *            the key of the property.
     * @param value
     *            the value of the property (null if the property is removed).
     * @return A callback object to get the resulting status of the event call.
     */
    CallBack<Void> setNodeProperty( long id, String key, Object value );

    /**
     * Invoked for a changed relationship property.
     * @param id
     *            the id of the relationship.
     * @param key
     *            the key of the property.
     * @param value
     *            the value of the property (null if the property is removed).
     * @return A callback object to get the resulting status of the event call.
     */
    CallBack<Void> setRelationshipProperty( long id, String key, Object value );
}
