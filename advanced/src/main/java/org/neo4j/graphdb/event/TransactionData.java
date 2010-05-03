package org.neo4j.graphdb.event;

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
}
