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
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.graphdb.traversal.BidirectionalTraversalDescription;
import org.neo4j.graphdb.traversal.TraversalDescription;

/**
 * The main access point to a running Neo4j instance. The most common way to instantiate a {@link GraphDatabaseService}
 * is as follows:
 * <pre>
 * <code>GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( new File("var/graphDb") );
 * // ... use Neo4j
 * graphDb.{@link #shutdown() shutdown()};</code>
 * </pre>
 * <p>
 * GraphDatabaseService provides operations to {@link #createNode() create
 * nodes}, {@link #getNodeById(long) get nodes given an id} and ultimately {@link #shutdown()
 * shutdown Neo4j}.
 * <p>
 * Please note that all operations on the graph must be invoked in a
 * {@link Transaction transactional context}. Failure to do so will result in a
 * {@link NotInTransactionException} being thrown.
 */
public interface GraphDatabaseService
{
    /**
     * Creates a new node.
     *
     * @return the created node.
     */
    Node createNode();

    /**
     * Creates a new node and returns it id.
     * Please note: Neo4j reuses its internal ids when
     * nodes and relationships are deleted, which means it's bad practice to
     * refer to them this way. Instead, use application generated ids.
     *
     * @return the created nodes id.
     * @deprecated This method will be removed in a future major release.
     */
    @Deprecated
    Long createNodeId();

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
    default ResourceIterator<Node> findNodes( Label label, String key1, Object value1, String key2, Object value2 )
    {
        throw new UnsupportedOperationException( "findNodes by multiple property names and values is not supported." );
    }

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
    default ResourceIterator<Node> findNodes( Label label,
                                      String key1, Object value1,
                                      String key2, Object value2,
                                      String key3, Object value3 )
    {
        throw new UnsupportedOperationException( "findNodes by multiple property names and values is not supported." );
    }

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
    default ResourceIterator<Node> findNodes( Label label, Map<String, Object> propertyValues )
    {
        throw new UnsupportedOperationException( "findNodes by multiple property names and values is not supported." );
    }

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
     *   <li>EXACT: The value has to match the template exactly. This is the same behavior as {@link GraphDatabaseService#findNode(Label, String, Object)}.</li>
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
    default ResourceIterator<Node> findNodes( Label label, String key, String template, StringSearchMode searchMode )
    {
        throw new UnsupportedOperationException( "Specialized string queries are not supported" );
    }

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
     * Returns all labels currently in the underlying store. Labels are added to the store the first time
     * they are used. This method guarantees that it will return all labels currently in use.
     *
     * Please take care that the returned {@link ResourceIterable} is closed correctly and as soon as possible
     * inside your transaction to avoid potential blocking of write operations.
     *
     * @return all labels in the underlying store.
     */
    ResourceIterable<Label> getAllLabelsInUse();

    /**
     * Returns all relationship types currently in the underlying store.
     * Relationship types are added to the underlying store the first time they
     * are used in a successfully committed {@link Node#createRelationshipTo
     * node.createRelationshipTo(...)}. This method guarantees that it will
     * return all relationship types currently in use.
     *
     * @return all relationship types in the underlying store
     */
    ResourceIterable<RelationshipType> getAllRelationshipTypesInUse();

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
    ResourceIterable<Label> getAllLabels();

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
    ResourceIterable<RelationshipType> getAllRelationshipTypes();

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
    ResourceIterable<String> getAllPropertyKeys();

    /**
     * Use this method to check if the database is currently in a usable state.
     *
     * @param timeout timeout (in milliseconds) to wait for the database to become available.
     *   If the database has been shut down {@code false} is returned immediately.
     * @return the state of the database: {@code true} if it is available, otherwise {@code false}
     */
    boolean isAvailable( long timeout );

    /**
     * Shuts down Neo4j. After this method has been invoked, it's invalid to
     * invoke any methods in the Neo4j API and all references to this instance
     * of GraphDatabaseService should be discarded.
     */
    void shutdown();

    /**
     * Starts a new {@link Transaction transaction} and associates it with the current thread.
     * <p>
     * <em>All database operations must be wrapped in a transaction.</em>
     * <p>
     * If you attempt to access the graph outside of a transaction, those operations will throw
     * {@link NotInTransactionException}.
     * <p>
     * Please ensure that any returned {@link ResourceIterable} is closed correctly and as soon as possible
     * inside your transaction to avoid potential blocking of write operations.
     *
     * @return a new transaction instance
     */
    Transaction beginTx();

    /**
     * Starts a new {@link Transaction transaction} with custom timeout and associates it with the current thread.
     * Timeout will be taken into account <b>only</b> when execution guard is enabled.
     * <p>
     * <em>All database operations must be wrapped in a transaction.</em>
     * <p>
     * If you attempt to access the graph outside of a transaction, those operations will throw
     * {@link NotInTransactionException}.
     * <p>
     * Please ensure that any returned {@link ResourceIterable} is closed correctly and as soon as possible
     * inside your transaction to avoid potential blocking of write operations.
     *
     * @param timeout transaction timeout
     * @param unit time unit of timeout argument
     * @return a new transaction instance
     */
    Transaction beginTx( long timeout, TimeUnit unit );

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
     * If query will not gonna be able to complete within specified timeout time interval it will be terminated.
     *
     * This method is the same as {@link #execute(String, java.util.Map)} with an empty parameters-map.
     *
     * @param query The query to execute
     * @param timeout The maximum time interval within which query should be completed.
     * @param unit time unit of timeout argument
     * @return A {@link org.neo4j.graphdb.Result} that contains the result set.
     * @throws QueryExecutionException If the Query contains errors
     */
    Result execute( String query, long timeout, TimeUnit unit ) throws QueryExecutionException;

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
     * Executes a query and returns an iterable that contains the result set.
     * If query will not gonna be able to complete within specified timeout time interval it will be terminated.
     *
     * @param query      The query to execute
     * @param parameters Parameters for the query
     * @param timeout The maximum time interval within which query should be completed.
     * @param unit time unit of timeout argument
     * @return A {@link org.neo4j.graphdb.Result} that contains the result set
     * @throws QueryExecutionException If the Query contains errors
     */
    Result execute( String query, Map<String,Object> parameters, long timeout, TimeUnit unit ) throws QueryExecutionException;

    /**
     * Registers {@code handler} as a handler for transaction events which
     * are generated from different places in the lifecycle of each
     * transaction. To guarantee that the handler gets all events properly
     * it shouldn't be registered when the application is running (i.e. in the
     * middle of one or more transactions). If the specified handler instance
     * has already been registered this method will do nothing.
     *
     * @param <T>     the type of state object used in the handler, see more
     *                documentation about it at {@link TransactionEventHandler}.
     * @param handler the handler to receive events about different states
     *                in transaction lifecycles.
     * @return the handler passed in as the argument.
     */
    <T> TransactionEventHandler<T> registerTransactionEventHandler( TransactionEventHandler<T> handler );

    /**
     * Unregisters {@code handler} from the list of transaction event handlers.
     * If {@code handler} hasn't been registered with
     * {@link #registerTransactionEventHandler(TransactionEventHandler)} prior
     * to calling this method an {@link IllegalStateException} will be thrown.
     * After a successful call to this method the {@code handler} will no
     * longer receive any transaction events.
     *
     * @param <T>     the type of state object used in the handler, see more
     *                documentation about it at {@link TransactionEventHandler}.
     * @param handler the handler to receive events about different states
     *                in transaction lifecycles.
     * @return the handler passed in as the argument.
     * @throws IllegalStateException if {@code handler} wasn't registered prior
     *                               to calling this method.
     */
    <T> TransactionEventHandler<T> unregisterTransactionEventHandler( TransactionEventHandler<T> handler );

    /**
     * Registers {@code handler} as a handler for kernel events which
     * are generated from different places in the lifecycle of the kernel.
     * To guarantee proper behavior the handler should be registered right
     * after the graph database has been started. If the specified handler
     * instance has already been registered this method will do nothing.
     *
     * @param handler the handler to receive events about different states
     *                in the kernel lifecycle.
     * @return the handler passed in as the argument.
     */
    KernelEventHandler registerKernelEventHandler( KernelEventHandler handler );

    /**
     * Unregisters {@code handler} from the list of kernel event handlers.
     * If {@code handler} hasn't been registered with
     * {@link #registerKernelEventHandler(KernelEventHandler)} prior to calling
     * this method an {@link IllegalStateException} will be thrown.
     * After a successful call to this method the {@code handler} will no
     * longer receive any kernel events.
     *
     * @param handler the handler to receive events about different states
     *                in the kernel lifecycle.
     * @return the handler passed in as the argument.
     * @throws IllegalStateException if {@code handler} wasn't registered prior
     *                               to calling this method.
     */
    KernelEventHandler unregisterKernelEventHandler( KernelEventHandler handler );

    /**
     * Returns the {@link Schema schema manager} where all things related to schema,
     * for example constraints and indexing on {@link Label labels}.
     *
     * @return the {@link Schema schema manager} for this database.
     */
    Schema schema();

    /**
     * Returns the {@link IndexManager} paired with this graph database service
     * and is the entry point for managing indexes coupled with this database.
     *
     * @return the {@link IndexManager} for this database.
     */
    IndexManager index();

    /**
     * Factory method for unidirectional traversal descriptions.
     *
     * @return a new {@link TraversalDescription}
     */
    TraversalDescription traversalDescription();

    /**
     * Factory method for bidirectional traversal descriptions.
     *
     * @return a new {@link BidirectionalTraversalDescription}
     */
    BidirectionalTraversalDescription bidirectionalTraversalDescription();
}
