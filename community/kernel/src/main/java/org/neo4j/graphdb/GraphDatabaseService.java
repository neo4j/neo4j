/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.tooling.GlobalGraphOperations;

/**
 * The main access point to a running Neo4j instance. The most common
 * implementation is the {@link EmbeddedGraphDatabase} class, which is used to
 * embed Neo4j in an application. Typically, you would create an
 * <code>EmbeddedGraphDatabase</code> instance as follows:
 * 
 * <pre>
 * <code>GraphDatabaseService graphDb = new EmbeddedGraphDatabase( "var/graphDb" );
 * // ... use Neo4j
 * graphDb.{@link #shutdown() shutdown()};</code>
 * </pre>
 * 
 * GraphDatabaseService provides operations to {@link #createNode() create
 * nodes}, {@link #getNodeById(long) get nodes given an id}, get the
 * {@link #getReferenceNode() reference node} and ultimately {@link #shutdown()
 * shutdown Neo4j}.
 * <p>
 * Please note that all operations that write to the graph must be invoked in a
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
    public Node createNode();

    /**
     * Creates a new node and adds the provided labels to it.
     *
     * @param labels {@link Label labels} to add to the created node.
     * @return the created node.
     */
    public Node createNode( Label... labels );

    /**
     * Looks up a node by id. Please note: Neo4j reuses its internal ids when
     * nodes and relationships are deleted, which means it's bad practice to
     * refer to them this way. Instead, use application generated ids.
     * 
     * @param id the id of the node
     * @return the node with id <code>id</code> if found
     * @throws NotFoundException if not found
     */
    public Node getNodeById( long id );

    /**
     * Looks up a relationship by id. Please note: Neo4j reuses its internal ids
     * when nodes and relationships are deleted, which means it's bad practice
     * to refer to them this way. Instead, use application generated ids.
     * 
     * @param id the id of the relationship
     * @return the relationship with id <code>id</code> if found
     * @throws NotFoundException if not found
     */
    public Relationship getRelationshipById( long id );

    /**
     * Returns the reference node, which is a "starting point" in the node
     * space. Usually, a client attaches relationships to this node that leads
     * into various parts of the node space.
     *
     * @return the reference node
     * @throws NotFoundException if unable to get the reference node
     * @deprecated The reference node concept is obsolete - indexes are the
     *              canonical way of getting hold of entry points in the graph.
     */
    @Deprecated
    public Node getReferenceNode();
    
    /**
     * Returns all nodes in the graph.
     * 
     * @return all nodes in the graph.
     * @deprecated this operation can be found in {@link GlobalGraphOperations} instead.
     */
    @Deprecated
    public Iterable<Node> getAllNodes();

    /**
     * Returns all nodes having the label, and the wanted property value. If no
     * indexes exist for the label/property combination, a scan of all labeled
     * nodes will be done. If an useful index is found, it will be used to
     * return the nodes faster than scanning all would.
     *
     * @param label the label to look for
     * @param propertyName the property name used in the search
     * @param value the value looked for.
     * @return an iterable containing all matching nodes.
     */
    ResourceIterable<Node> findNodesByLabelAndProperty( Label label, String propertyName, Object value );
    
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
    public Iterable<RelationshipType> getRelationshipTypes();

    /**
     * Shuts down Neo4j. After this method has been invoked, it's invalid to
     * invoke any methods in the Neo4j API and all references to this instance
     * of GraphDatabaseService should be discarded.
     */
    public void shutdown();

    /**
     * Starts a new transaction and associates it with the current thread.
     * 
     * @return a new transaction instance
     */
    public Transaction beginTx();
    
    /**
     * Registers {@code handler} as a handler for transaction events which
     * are generated from different places in the lifecycle of each
     * transaction. To guarantee that the handler gets all events properly
     * it shouldn't be registered when the application is running (i.e. in the
     * middle of one or more transactions). If the specified handler instance
     * has already been registered this method will do nothing.
     * 
     * @param <T> the type of state object used in the handler, see more
     * documentation about it at {@link TransactionEventHandler}.
     * @param handler the handler to receive events about different states
     * in transaction lifecycles.
     * @return the handler passed in as the argument.
     */
    public <T> TransactionEventHandler<T> registerTransactionEventHandler(
            TransactionEventHandler<T> handler );
    
    /**
     * Unregisters {@code handler} from the list of transaction event handlers.
     * If {@code handler} hasn't been registered with
     * {@link #registerTransactionEventHandler(TransactionEventHandler)} prior
     * to calling this method an {@link IllegalStateException} will be thrown.
     * After a successful call to this method the {@code handler} will no
     * longer receive any transaction events.
     * 
     * @param <T> the type of state object used in the handler, see more
     * documentation about it at {@link TransactionEventHandler}.
     * @param handler the handler to receive events about different states
     * in transaction lifecycles.
     * @return the handler passed in as the argument.
     * @throws IllegalStateException if {@code handler} wasn't registered prior
     * to calling this method.
     */
    public <T> TransactionEventHandler<T> unregisterTransactionEventHandler(
            TransactionEventHandler<T> handler );
    
    /**
     * Registers {@code handler} as a handler for kernel events which
     * are generated from different places in the lifecycle of the kernel.
     * To guarantee proper behaviour the handler should be registered right
     * after the graph database has been started. If the specified handler
     * instance has already been registered this method will do nothing.
     * 
     * @param handler the handler to receive events about different states
     * in the kernel lifecycle.
     * @return the handler passed in as the argument.
     */
    public KernelEventHandler registerKernelEventHandler(
            KernelEventHandler handler );

    /**
     * Unregisters {@code handler} from the list of kernel event handlers.
     * If {@code handler} hasn't been registered with
     * {@link #registerKernelEventHandler(KernelEventHandler)} prior to calling
     * this method an {@link IllegalStateException} will be thrown.
     * After a successful call to this method the {@code handler} will no
     * longer receive any kernel events.
     * 
     * @param handler the handler to receive events about different states
     * in the kernel lifecycle.
     * @return the handler passed in as the argument.
     * @throws IllegalStateException if {@code handler} wasn't registered prior
     * to calling this method.
     */
    public KernelEventHandler unregisterKernelEventHandler(
            KernelEventHandler handler );

    /**
     * Returns the {@link Schema schema manager} where all things related to schema,
     * for example constraints and indexing on {@link Label labels}.
     * 
     * @return the {@link Schema schema manager} for this database.
     */
    public Schema schema();

    /**
     * Returns the {@link IndexManager} paired with this graph database service
     * and is the entry point for managing indexes coupled with this database.
     * @return the {@link IndexManager} for this database.
     */
    public IndexManager index();
}
