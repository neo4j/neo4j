/*
 * Copyright (c) 2002-2009 "Neo Technology,"
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
package org.neo4j.api.core;

import java.io.Serializable;
import java.util.Map;

/**
 * The main access point to a running Neo4j instance. The most common
 * implementation is the {@link EmbeddedNeo EmbeddedNeo} class, which is used to
 * embed Neo4j in an application. Typically, you would create an
 * <code>EmbeddedNeo</code> instance as follows:
 * <code>
 * <pre>NeoService neo = new EmbeddedNeo( "var/neo" );
 * // ... use neo
 * neo.shutdown();</pre>
 * </code>
 * NeoService provides operations to {@link #enableRemoteShell enable the shell},
 * {@link #createNode() create nodes}, {@link #getNodeById(long) get nodes
 * given an id}, get the {@link #getReferenceNode() reference node} and
 * ultimately {@link #shutdown() shutdown Neo4j}.
 * <p>
 * Please note that all operations that read or write to the node space must be
 * invoked in a {@link Transaction transactional context}. Failure to do so 
 * will result in a {@link NotInTransactionException} being thrown.
 */
public interface NeoService
{
    /**
     * Creates a new node.
     * @return the created node.
     */
    public Node createNode();

    /**
     * Looks up a node by id.
     * @param id the id of the node 
     * @return the node with id <code>id</code> if found
     * @throws NotFoundException if not found
     */
    public Node getNodeById( long id );
    
    /**
     * Looks up a relationship by id.
     * @param id the id of the relationship
     * @return the relationship with id <code>id</code> if found
     * @throws NotFoundException if not found
     */
    public Relationship getRelationshipById(long id);

    /**
     * Returns the reference node, which is a "starting point" in the node
     * space. Usually, a client attaches relationships to this node that leads
     * into various parts of the node space. For more information about common
     * node space organizational patterns, see the design guide at
     * <a href="http://neo4j.org/doc">http://neo4j.org/doc</a>.
     * @return the reference node
     * @throws NotFoundException if unable to get the reference node
     */
    public Node getReferenceNode();
    
    /**
     * Returns all nodes in the node space. 
     * @return all nodes in the node space
     */
    public Iterable<Node> getAllNodes();
    
    
    /**
     * Returns all relationship types currently in the underlying store. Relationship
     * types are added to the underlying store the first time they are used in
     * a successfully commited {@link Node#createRelationshipTo node.createRelationshipTo(...)}. Note that
     * this method is guaranteed to return all known relationship types, but
     * it does not guarantee that it won't return <i>more</i> than that (e.g.
     * it can return "historic" relationship types that no longer have any
     * relationships in the node space).
     * @return all relationship types in the underlying store
     */
    public Iterable<RelationshipType> getRelationshipTypes();

    /**
     * Shuts down Neo4j. After this method has been invoked,  it's invalid to
     * invoke any methods in the Neo4j API and all references to this instance
     * of NeoService should be discarded.
     */
    public void shutdown();
    
    /**
     * Enables remote shell access (with default configuration) to this Neo4j
     * instance, if the Neo4j <code>shell</code> component is available on the
     * classpath. This method is identical to invoking
     * {@link #enableRemoteShell(Map) enableRemoteShell( null )}.
     * @return <code>true</code> if the shell has been enabled,
     * <code>false</code> otherwise (<code>false</code> usually indicates that
     * the <code>shell</code> jar dependency is not on the classpath)
     */
    public boolean enableRemoteShell();

    /**
     * Enables remote shell access to this Neo4j instance, if the Neo4j
     * <code>shell</code> component is available on the classpath. This will
     * publish a shell access interface on an RMI registry on localhost (with
     * configurable port and RMI binding name). It can be accessed by a
     * client that implements <code>org.neo4j.util.shell.ShellClient</code>
     * from the Neo4J <code>shell</code> project. Typically, the
     * <code>shell</code> binary package is used (see
     * <a href="http://neo4j.org/download">neo4j.org/download</a>).
     * <p>
     * The shell is parameterized by a map of properties passed in to this
     * method. Currently, two properties are used:
     * <ul>
     *	<li><code>port</code>, an {@link Integer} describing the port of the RMI
     * registry where the Neo4j shell will be bound, defaults to
     * <code>1337</code> <li><code>name</code>, the {@link String} under which
     * the Neo4j shell will be bound in the RMI registry, defaults to
     * <code>shell</code> </ul>
     * @param initialProperties a set of properties that will be used to
     * configure the remote shell, or <code>null</code> if the default
     * properties should be used
     * @return <code>true</code> if the shell has been enabled,
     * <code>false</code> otherwise (<code>false</code> usually indicates that
     * the <code>shell</code> jar dependency is not on the classpath)
     * @throws ClassCastException if the shell library is available, but one
     * (or more) of the configuration properties have an unexpected type
     * @throws IllegalStateException if the shell library is available, but
     * the remote shell can't be enabled anyway
     */
    public boolean enableRemoteShell(
        Map<String, Serializable> initialProperties );
 
    /**
     * Starts a new transaction.
     * @return a new transaction instance
     */
    public Transaction beginTx();
}