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
package org.neo4j.graphdb;

import java.util.Map;

import org.neo4j.annotations.api.PublicApi;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.graphdb.traversal.BidirectionalTraversalDescription;
import org.neo4j.graphdb.traversal.TraversalDescription;

/**
 * A programmatically handled transaction.
 * <p>
 * <em>All database operations that access the graph, indexes, or the schema must be performed in a transaction.</em>
 * <p>
 * If you attempt to access the graph outside of a transaction, those operations will throw
 * {@link NotInTransactionException}.
 * <p>
 * Here's the idiomatic use of programmatic transactions in Neo4j:
 *
 * <pre>
 * <code>
 * try ( Transaction tx = graphDb.beginTx() )
 * {
 *     // operations on the graph
 *     // ...
 *
 *     tx.commit();
 * }
 * </code>
 * </pre>
 *
 * <p>
 * Let's walk through this example line by line. First we retrieve a Transaction
 * object by invoking the {@link GraphDatabaseService#beginTx()} factory method.
 * This creates a new transaction which has internal state to keep
 * track of whether the current transaction is successful. Then we wrap all
 * operations that modify the graph in a try-finally block with the transaction
 * as resource. At the end of the block, we invoke the {@link #commit() tx.commit()}
 * method to commit that the transaction.
 * <p>
 * If an exception is raised in the try-block, {@link #commit()} will never be
 * invoked and the transaction will be roll backed. This is very important:
 * unless {@link #commit()} is invoked, the transaction will fail upon
 * {@link #close()}. A transaction can be explicitly rolled back by
 * invoking the {@link #rollback()} method.
 * <p>
 * Read operations inside of a transaction will also read uncommitted data from
 * the same transaction.
 * <p>
 * All {@link ResourceIterable ResourceIterables} that were returned from operations executed inside a transaction
 * will be automatically closed when the transaction is committed or rolled back.
 * Note however, that the {@link ResourceIterator} should be {@link ResourceIterator#close() closed} as soon as
 * possible if you don't intend to exhaust the iterator.
 *
 * <p>
 * <strong>Note that transactions should be used by a single thread only.</strong>
 * It is generally not safe to use a transaction from multiple threads.
 * Doing so will lead to undefined behavior.
 */
@PublicApi
public interface Transaction extends AutoCloseable
{

    /**
     * Creates a new node.
     *
     * @return the created node.
     */
    Node createNode();

    /**
     * Creates a new node and adds the provided labels to it.
     *
     * @param labels {@link Label labels} to add to the created node.
     * @return the created node.
     */
    Node createNode( Label... labels );

    /**
     * Looks up a node by id. Please note: Neo4j reuses its internal ids when
     * nodes and relationships are deleted, which means it's bad practice to
     * refer to them this way. Instead, use application generated ids.
     *
     * @param id the id of the node
     * @return the node with id <code>id</code> if found
     * @throws NotFoundException if not found
     */
    Node getNodeById( long id );

    /**
     * Looks up a relationship by id. Please note: Neo4j reuses its internal ids
     * when nodes and relationships are deleted, which means it's bad practice
     * to refer to them this way. Instead, use application generated ids.
     *
     * @param id the id of the relationship
     * @return the relationship with id <code>id</code> if found
     * @throws NotFoundException if not found
     */
    Relationship getRelationshipById( long id );

    /**
     * Factory method for bidirectional traversal descriptions.
     *
     * @return a new {@link BidirectionalTraversalDescription}
     * @deprecated Not part of public API, can be removed at any moment
     */
    @Deprecated
    BidirectionalTraversalDescription bidirectionalTraversalDescription();

    /**
     * Factory method for unidirectional traversal descriptions.
     *
     * @deprecated Not part of public API, can be removed at any moment
     * @return a new {@link TraversalDescription}
     */
    @Deprecated
    TraversalDescription traversalDescription();

    /**
     * Executes a query and returns an iterable that contains the result set.
     *
     * This method is the same as {@link #execute(String, java.util.Map)} with an empty parameters-map.
     *
     * @param query The query to execute
     * @return A {@link org.neo4j.graphdb.Result} that contains the result set.
     * @throws QueryExecutionException If the Query contains errors
     */
    Result execute( String query ) throws QueryExecutionException;

    /**
     * Executes a query and returns an iterable that contains the result set.
     *
     * @param query      The query to execute
     * @param parameters Parameters for the query
     * @return A {@link org.neo4j.graphdb.Result} that contains the result set
     * @throws QueryExecutionException If the Query contains errors
     */
    Result execute( String query, Map<String,Object> parameters ) throws QueryExecutionException;

    /**
     * Returns all labels currently in the underlying store. Labels are added to the store the first time
     * they are used. This method guarantees that it will return all labels currently in use.
     *
     * Please take care that the returned {@link ResourceIterable} is closed correctly and as soon as possible
     * inside your transaction to avoid potential blocking of write operations.
     *
     * @return all labels in the underlying store.
     */
    Iterable<Label> getAllLabelsInUse();

    /**
     * Returns all relationship types currently in the underlying store.
     * Relationship types are added to the underlying store the first time they
     * are used in a successfully committed {@link Node#createRelationshipTo
     * node.createRelationshipTo(...)}. This method guarantees that it will
     * return all relationship types currently in use.
     *
     * @return all relationship types in the underlying store
     */
    Iterable<RelationshipType> getAllRelationshipTypesInUse();

    /**
     * Returns all labels currently in the underlying store. Labels are added to the store the first time
     * they are used. This method guarantees that it will return all labels currently in use. However,
     * it may also return <i>more</i> than that (e.g. it can return "historic" labels that are no longer used).
     *
     * Please take care that the returned {@link ResourceIterable} is closed correctly and as soon as possible
     * inside your transaction to avoid potential blocking of write operations.
     *
     * @return all labels in the underlying store.
     */
    Iterable<Label> getAllLabels();

    /**
     * Returns all relationship types currently in the underlying store.
     * Relationship types are added to the underlying store the first time they
     * are used in a successfully committed {@link Node#createRelationshipTo
     * node.createRelationshipTo(...)}. Note that this method is guaranteed to
     * return all known relationship types, but it does not guarantee that it
     * won't return <i>more</i> than that (e.g. it can return "historic"
     * relationship types that no longer have any relationships in the node
     * space).
     *
     * @return all relationship types in the underlying store
     */
    Iterable<RelationshipType> getAllRelationshipTypes();

    /**
     * Returns all property keys currently in the underlying store. This method guarantees that it will return all
     * property keys currently in use. However, it may also return <i>more</i> than that (e.g. it can return "historic"
     * labels that are no longer used).
     *
     * Please take care that the returned {@link ResourceIterable} is closed correctly and as soon as possible
     * inside your transaction to avoid potential blocking of write operations.
     *
     * @return all property keys in the underlying store.
     */
    Iterable<String> getAllPropertyKeys();

    /**
     * Returns all nodes having a given label, and a property value of type String or Character matching the
     * given value template and search mode.
     * <p>
     * If an online index is found, it will be used to look up the requested nodes.
     * If no indexes exist for the label/property combination, the database will
     * scan all labeled nodes looking for matching property values.
     * <p>
     * The search mode and value template are used to select nodes of interest. The search mode can
     * be one of
     * <ul>
     *   <li>EXACT: The value has to match the template exactly. This is the same behavior as {@link Transaction#findNode(Label, String, Object)}.</li>
     *   <li>PREFIX: The value must have a prefix matching the template.</li>
     *   <li>SUFFIX: The value must have a suffix matching the template.</li>
     *   <li>CONTAINS: The value must contain the template. Only exact matches are supported.</li>
     * </ul>
     * Note that in Neo4j the Character 'A' will be treated the same way as the String 'A'.
     * <p>
     * Please ensure that the returned {@link ResourceIterator} is closed correctly and as soon as possible
     * inside your transaction to avoid potential blocking of write operations.
     *
     * @param label      consider nodes with this label
     * @param key        required property key
     * @param template   required property value template
     * @param searchMode required property value template
     * @return an iterator containing all matching nodes. See {@link ResourceIterator} for responsibilities.
     */
    ResourceIterator<Node> findNodes( Label label, String key, String template, StringSearchMode searchMode );

    /**
     * Returns all nodes having the label, and the wanted property values.
     * If an online index is found, it will be used to look up the requested
     * nodes.
     * <p>
     * If no indexes exist for the label with all provided properties, the database will
     * scan all labeled nodes looking for matching nodes.
     * <p>
     * Note that equality for values do not follow the rules of Java. This means that the number 42 is equals to all
     * other 42 numbers, regardless of whether they are encoded as Integer, Long, Float, Short, Byte or Double.
     * <p>
     * Same rules follow Character and String - the Character 'A' is equal to the String 'A'.
     * <p>
     * Finally - arrays also follow these rules. An int[] {1,2,3} is equal to a double[] {1.0, 2.0, 3.0}
     * <p>
     * Please ensure that the returned {@link ResourceIterator} is closed correctly and as soon as possible
     * inside your transaction to avoid potential blocking of write operations.
     *
     * @param label          consider nodes with this label
     * @param propertyValues required property key-value combinations
     * @return an iterator containing all matching nodes. See {@link ResourceIterator} for responsibilities.
     */
    ResourceIterator<Node> findNodes( Label label, Map<String, Object> propertyValues );

    /**
     * Returns all nodes having the label, and the wanted property values.
     * If an online index is found, it will be used to look up the requested
     * nodes.
     * <p>
     * If no indexes exist for the label with all provided properties, the database will
     * scan all labeled nodes looking for matching nodes.
     * <p>
     * Note that equality for values do not follow the rules of Java. This means that the number 42 is equals to all
     * other 42 numbers, regardless of whether they are encoded as Integer, Long, Float, Short, Byte or Double.
     * <p>
     * Same rules follow Character and String - the Character 'A' is equal to the String 'A'.
     * <p>
     * Finally - arrays also follow these rules. An int[] {1,2,3} is equal to a double[] {1.0, 2.0, 3.0}
     * <p>
     * Please ensure that the returned {@link ResourceIterator} is closed correctly and as soon as possible
     * inside your transaction to avoid potential blocking of write operations.
     *
     * @param label  consider nodes with this label
     * @param key1   required property key1
     * @param value1 required property value of key1
     * @param key2   required property key2
     * @param value2 required property value of key2
     * @param key3   required property key3
     * @param value3 required property value of key3
     * @return an iterator containing all matching nodes. See {@link ResourceIterator} for responsibilities.
     */
    ResourceIterator<Node> findNodes( Label label, String key1, Object value1, String key2, Object value2, String key3, Object value3 );

    /**
     * Returns all nodes having the label, and the wanted property values.
     * If an online index is found, it will be used to look up the requested
     * nodes.
     * <p>
     * If no indexes exist for the label with all provided properties, the database will
     * scan all labeled nodes looking for matching nodes.
     * <p>
     * Note that equality for values do not follow the rules of Java. This means that the number 42 is equals to all
     * other 42 numbers, regardless of whether they are encoded as Integer, Long, Float, Short, Byte or Double.
     * <p>
     * Same rules follow Character and String - the Character 'A' is equal to the String 'A'.
     * <p>
     * Finally - arrays also follow these rules. An int[] {1,2,3} is equal to a double[] {1.0, 2.0, 3.0}
     * <p>
     * Please ensure that the returned {@link ResourceIterator} is closed correctly and as soon as possible
     * inside your transaction to avoid potential blocking of write operations.
     *
     * @param label  consider nodes with this label
     * @param key1   required property key1
     * @param value1 required property value of key1
     * @param key2   required property key2
     * @param value2 required property value of key2
     * @return an iterator containing all matching nodes. See {@link ResourceIterator} for responsibilities.
     */
    ResourceIterator<Node> findNodes( Label label, String key1, Object value1, String key2, Object value2 );

    /**
     * Equivalent to {@link #findNodes(Label, String, Object)}, however it must find no more than one
     * {@link Node node} or it will throw an exception.
     *
     * @param label consider nodes with this label
     * @param key   required property key
     * @param value required property value
     * @return the matching node or <code>null</code> if none could be found
     * @throws MultipleFoundException if more than one matching {@link Node node} is found
     */
    Node findNode( Label label, String key, Object value );

    /**
     * Returns all nodes having the label, and the wanted property value.
     * If an online index is found, it will be used to look up the requested
     * nodes.
     * <p>
     * If no indexes exist for the label/property combination, the database will
     * scan all labeled nodes looking for the property value.
     * <p>
     * Note that equality for values do not follow the rules of Java. This means that the number 42 is equals to all
     * other 42 numbers, regardless of whether they are encoded as Integer, Long, Float, Short, Byte or Double.
     * <p>
     * Same rules follow Character and String - the Character 'A' is equal to the String 'A'.
     * <p>
     * Finally - arrays also follow these rules. An int[] {1,2,3} is equal to a double[] {1.0, 2.0, 3.0}
     * <p>
     * Please ensure that the returned {@link ResourceIterator} is closed correctly and as soon as possible
     * inside your transaction to avoid potential blocking of write operations.
     *
     * @param label consider nodes with this label
     * @param key   required property key
     * @param value required property value
     * @return an iterator containing all matching nodes. See {@link ResourceIterator} for responsibilities.
     */
    ResourceIterator<Node> findNodes( Label label, String key, Object value );

    /**
     * Returns all {@link Node nodes} with a specific {@link Label label}.
     *
     * Please take care that the returned {@link ResourceIterator} is closed correctly and as soon as possible
     * inside your transaction to avoid potential blocking of write operations.
     *
     * @param label the {@link Label} to return nodes for.
     * @return an iterator containing all nodes matching the label. See {@link ResourceIterator} for responsibilities.
     */
    ResourceIterator<Node> findNodes( Label label );

    /**
     * Marks this transaction as terminated, which means that it will be, much like in the case of failure,
     * unconditionally rolled back when {@link #close()} is called. Once this method has been invoked, it doesn't matter
     * if {@link #commit()} ()} is invoked afterwards -- the transaction will still be rolled back.
     *
     * Additionally, terminating a transaction causes all subsequent operations carried out within that
     * transaction to throw a {@link TransactionTerminatedException}.
     *
     * Note that, unlike the other transaction operations, this method can be called from different thread.
     * When this method is called, it signals to terminate the transaction and returns immediately.
     *
     * Calling this method on an already closed transaction has no effect.
     */
    void terminate();

    /**
     * Returns all nodes in the graph.
     *
     * @return all nodes in the graph.
     */
    ResourceIterable<Node> getAllNodes();

    /**
     * Returns all relationships in the graph.
     *
     * @return all relationships in the graph.
     */
    ResourceIterable<Relationship> getAllRelationships();

    /**
     * Acquires a write lock for {@code entity} for this transaction.
     * The lock (returned from this method) can be released manually, but
     * if not it's released automatically when the transaction finishes.
     *
     * @param entity the entity to acquire a lock for. If another transaction
     * currently holds a write lock to that entity this call will wait until
     * it's released.
     *
     * @return a {@link Lock} which optionally can be used to release this
     * lock earlier than when the transaction finishes. If not released
     * (with {@link Lock#release()} it's going to be released when the
     * transaction finishes.
     */
    Lock acquireWriteLock( Entity entity );

    /**
     * Acquires a read lock for {@code entity} for this transaction.
     * The lock (returned from this method) can be released manually, but
     * if not it's released automatically when the transaction finishes.
     * @param entity the entity to acquire a lock for. If another transaction
     * currently hold a write lock to that entity this call will wait until
     * it's released.
     *
     * @return a {@link Lock} which optionally can be used to release this
     * lock earlier than when the transaction finishes. If not released
     * (with {@link Lock#release()} it's going to be released with the
     * transaction finishes.
     */
    Lock acquireReadLock( Entity entity );

    /**
     * Returns the {@link Schema schema manager} where all things related to schema,
     * for example constraints and indexing on {@link Label labels}.
     *
     * @return the {@link Schema schema manager} for this database.
     */
    Schema schema();

    /**
     * Commit and close current transaction.
     * <p>
     * When {@code commit()} is completed, all resources are released and no more changes are possible in this transaction.
     */
    void commit();

    /**
     * Roll back and close current transaction.
     * When {@code rollback()} is completed, all resources are released and no more changes are possible in this transaction
     */
    void rollback();

    /**
     * Close transaction. If {@link #commit()} or {@link #rollback()} have been called this does nothing.
     * If none of them are called, the transaction will be rolled back.
     *
     * <p>All {@link ResourceIterable ResourceIterables} that where returned from operations executed inside this
     * transaction will be automatically closed by this method in they were not closed before.
     *
     * <p>This method comes from {@link AutoCloseable} so that a {@link Transaction} can participate
     * in try-with-resource statements.
     */
    @Override
    void close();
}
