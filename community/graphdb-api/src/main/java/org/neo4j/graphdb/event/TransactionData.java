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
package org.neo4j.graphdb.event;

import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

/**
 * Represents the data that has changed during the course of one transaction.
 * It consists of what has happened in the transaction compared to how
 * it was before the transaction started. This implies f.ex. that a node which
 * is created, modified and then deleted in the same transaction won't be seen
 * in the transaction data at all.
 *
 * @author Tobias Ivarsson
 * @author Mattias Persson
 */
public interface TransactionData
{

    /**
     * Get the nodes that were created during the transaction.
     *
     * @return all nodes that were created during the transaction.
     */
    Iterable<Node> createdNodes();

    /**
     * Get the nodes that were deleted during the transaction.
     *
     * @return all nodes that were deleted during the transaction.
     */
    Iterable<Node> deletedNodes();

    /**
     * Returns whether or not {@code node} is deleted in this transaction.
     * @param node the {@link Node} to check whether or not it is deleted
     * in this transaction.
     * @return whether or not {@code node} is deleted in this transaction.
     */
    boolean isDeleted( Node node );

    /**
     * Get the properties that had a value assigned or overwritten on a node
     * during the transaction. All the properties of nodes created
     * during the transaction will be returned by this method as well. Only the
     * values that are present at the end of the transaction will be returned,
     * whereas all previously assigned values of properties that have been
     * assigned multiple times during the transaction will not be returned.
     *
     * @return all properties that have been assigned on nodes.
     */
    Iterable<PropertyEntry<Node>> assignedNodeProperties();

    /**
     * Get the properties that had a value removed from a node during the
     * transaction. Values are considered to be removed if the value is
     * overwritten by calling {@link Node#setProperty(String, Object)} with a
     * property that has a previous value, or if the property is explicitly
     * removed by calling {@link Node#removeProperty(String)}. Only the values
     * that were present before the transaction are returned by this method, all
     * previous values of properties that have been assigned multiple times
     * during the transaction will not be returned. This is also true for
     * properties that had no value before the transaction, was assigned during
     * the transaction, and then removed during the same transaction. Deleting
     * a node will cause all its currently assigned properties to be added to
     * this list as well.
     *
     * @return all properties that have been removed from nodes.
     */
    Iterable<PropertyEntry<Node>> removedNodeProperties();

    /**
     * Get all new labels that have been assigned during the transaction. This
     * will return one entry for each label added to each node. All labels assigned
     * to nodes that were created in the transaction will also be included.
     *
     * @return all labels assigned on nodes.
     */
    Iterable<LabelEntry> assignedLabels();

    /**
     * Get all labels that have been removed from nodes during the transaction.
     *
     * @return all labels removed from nodes.
     */
    Iterable<LabelEntry> removedLabels();

    /**
     * Get the relationships that were created during the transaction.
     *
     * @return all relationships that were created during the transaction.
     */
    Iterable<Relationship> createdRelationships();

    /**
     * Get the relationships that were deleted during the transaction.
     *
     * @return all relationships that were deleted during the transaction.
     */
    Iterable<Relationship> deletedRelationships();

    /**
     * Returns whether or not {@code relationship} is deleted in this
     * transaction.
     *
     * @param relationship the {@link Relationship} to check whether or not it
     *            is deleted in this transaction.
     * @return whether or not {@code relationship} is deleted in this
     *         transaction.
     */
    boolean isDeleted( Relationship relationship );

    /**
     * Get the properties that had a value assigned on a relationship during the
     * transaction. If the property had a value on that relationship before the
     * transaction started the previous value will be returned by
     * {@link #removedRelationshipProperties()}. All the properties of
     * relationships created during the transaction will be returned by this
     * method as well. Only the values that are present at the end of the
     * transaction will be returned by this method, all previously assigned
     * values of properties that have been assigned multiple times during the
     * transaction will not be returned.
     *
     * @return all properties that have been assigned on relationships.
     */
    Iterable<PropertyEntry<Relationship>> assignedRelationshipProperties();

    /**
     * Get the properties that had a value removed from a relationship during
     * the transaction. Values are considered to be removed if the value is
     * overwritten by calling {@link Relationship#setProperty(String, Object)}
     * with a property that has a previous value, or if the property is
     * explicitly removed by calling {@link Relationship#removeProperty(String)}
     * . Only the values that were present before the transaction are returned
     * by this method, all previous values of properties that have been assigned
     * multiple times during the transaction will not be returned. This is also
     * true for properties that had no value before the transaction, was
     * assigned during the transaction, and then removed during the same
     * transaction. Deleting a relationship will cause all its currently
     * assigned properties to be added to this list as well.
     *
     * @return all properties that have been removed from relationships.
     */
    Iterable<PropertyEntry<Relationship>> removedRelationshipProperties();

    /**
     * Get the username under which authorization state this transaction is running.
     *
     * @return the username of the user who initiated the transaction.
     */
    String username();

    /**
     * Applications that start transactions may attach additional application specific meta-data to each transaction.
     *
     * @return The application specific meta-data map associated with this transaction.
     */
    Map<String,Object> metaData();

    /**
     * Return transaction id that assigned during transaction commit process.
     * @return transaction id.
     * @throws IllegalStateException if transaction id is not assigned yet
     */
    default long getTransactionId()
    {
        throw new IllegalStateException( "Transaction id is not available." );
    }

    /**
     * Return transaction commit time (in millis) that assigned during transaction commit process.
     * @return transaction commit time
     * @throws IllegalStateException if commit time is not assigned yet
     */
    default long getCommitTime()
    {
        throw new IllegalStateException( "Transaction commit time it not available." );
    }
}
