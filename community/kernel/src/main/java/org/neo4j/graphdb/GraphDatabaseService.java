/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.graphdb;

import java.util.Map;

import org.neo4j.embedded.GraphDatabase;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.graphdb.traversal.BidirectionalTraversalDescription;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.tooling.GlobalGraphOperations;

/**
 * The main access point for working with a Graph Database. Most implementations will be
 * based on {@link GraphDatabase}, which is used to embed Neo4j in an application.
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
     * @deprecated this operation can be found in {@link GlobalGraphOperations} instead.
     */
    @Deprecated
    Iterable<Node> getAllNodes();

    /**
     * Returns all nodes having the label, and the wanted property value.
     * If an online index is found, it will be used to look up the requested
     * nodes.
     * <p>
     * If no indexes exist for the label/property combination, the database will
     * scan all labeled nodes looking for the property value.
     * <p>
     * Note that equality for values do not follow the rules of Java. This means that the number 42 is equals to all
     * other 42 numbers, indifferently of if they are encoded as Integer, Long, Float, Short, Byte or Double.
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
     * Returns all nodes having the label, and the wanted property value.
     * If an online index is found, it will be used to look up the requested
     * nodes.
     * <p>
     * If no indexes exist for the label/property combination, the database will
     * scan all labeled nodes looking for the property value.
     * <p>
     * Note that equality for values do not follow the rules of Java. This means that the number 42 is equals to all
     * other 42 numbers, indifferently of if they are encoded as Integer, Long, Float, Short, Byte or Double.
     * <p>
     * Same rules follow Character and String - the Character 'A' is equal to the String 'A'.
     * <p>
     * Finally - arrays also follow these rules. An int[] {1,2,3} is equal to a double[] {1.0, 2.0, 3.0}
     * <p>
     * Please ensure that the returned {@link ResourceIterable} is closed correctly and as soon as possible
     * inside your transaction to avoid potential blocking of write operations.
     *
     * @param label consider nodes with this label
     * @param key   required property key
     * @param value required property value
     * @return an iterable containing all matching nodes. See {@link ResourceIterable} for responsibilities.
     * @deprecated Use {@link #findNodes(Label, String, Object)}
     */
    @Deprecated
    ResourceIterable<Node> findNodesByLabelAndProperty( Label label, String key, Object value );

    /**
     * Returns all relationship types currently in the underlying store.
     * Relationship types are added to the underlying store the first time they
     * are used in a successfully commited {@link Node#createRelationshipTo
     * node.createRelationshipTo(...)}. Note that this method is guaranteed to
     * return all known relationship types, but it does not guarantee that it
     * won't return <i>more</i> than that (e.g. it can return "historic"
     * relationship types that no longer have any relationships in the node
     * space).
     *
     * @return all relationship types in the underlying store
     * @deprecated this operation can be found in {@link GlobalGraphOperations} instead.
     */
    @Deprecated
    Iterable<RelationshipType> getRelationshipTypes();

    /**
     * Use this method to check if the database is currently in a usable state.
     *
     * @param timeout timeout (in milliseconds) to wait for the database to become available. If the database has been
     *                shutdown this immediately returns false.
     */
    boolean isAvailable( long timeout );

    /**
     * @deprecated use {@link GraphDatabase#shutdown()} instead
     */
    @Deprecated
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
     * @deprecated use {@link GraphDatabase#registerTransactionEventHandler(TransactionEventHandler)} instead
     */
    @Deprecated
    <T> TransactionEventHandler<T> registerTransactionEventHandler( TransactionEventHandler<T> handler );

    /**
     * @deprecated use {@link GraphDatabase#unregisterTransactionEventHandler(TransactionEventHandler)} instead
     */
    @Deprecated
    <T> TransactionEventHandler<T> unregisterTransactionEventHandler( TransactionEventHandler<T> handler );

    /**
     * @deprecated use {@link GraphDatabase#registerKernelEventHandler(KernelEventHandler)} instead
     */
    @Deprecated
    KernelEventHandler registerKernelEventHandler( KernelEventHandler handler );

    /**
     * @deprecated use {@link GraphDatabase#unregisterKernelEventHandler(KernelEventHandler)} instead
     */
    @Deprecated
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
     * Factory method for unidirectional traversal descriptions
     */
    TraversalDescription traversalDescription();

    /**
     * Factory method for bidirectional traversal descriptions
     */
    BidirectionalTraversalDescription bidirectionalTraversalDescription();
}
