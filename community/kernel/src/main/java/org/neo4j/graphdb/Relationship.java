/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

/**
 * A relationship between two nodes in the graph. A relationship has a start
 * node, an end node and a {@link RelationshipType type}. You can attach
 * properties to relationships with the API specified in
 * {@link PropertyContainer}.
 * <p>
 * Relationships are created by invoking the
 * {@link Node#createRelationshipTo(Node, RelationshipType)
 * Node.createRelationshipTo()} method on a node as follows:
 * <p>
 * <code>
 * Relationship rel = node.{@link Node#createRelationshipTo(Node, RelationshipType) createRelationshipTo}( otherNode, MyRels.REL_TYPE );
 * </code>
 * <p>
 * The fact that the relationship API gives meaning to {@link #getStartNode()
 * start} and {@link #getEndNode() end} nodes implicitly means that all
 * relationships have a direction. In the example above, <code>rel</code> would
 * be directed <i>from</i> <code>node</code> <i>to</i> <code>otherNode</code>. A
 * relationship's start node and end node and their relation to
 * {@link Direction#OUTGOING} and {@link Direction#INCOMING} are defined so that
 * the assertions in the following code are <code>true</code>:
 * 
 * <pre>
 * <code>
 * {@link Node} a = graphDb.{@link GraphDatabaseService#createNode() createNode}(), b = graphDb.{@link GraphDatabaseService#createNode() createNode}();
 * {@link Relationship} rel = a.{@link Node#createRelationshipTo(Node, RelationshipType) createRelationshipTo}( b, {@link RelationshipType MyRels.REL_TYPE} );
 * // Now we have: (a) --- REL_TYPE ---&gt; (b)
 * 
 * assert rel.{@link Relationship#getStartNode() getStartNode}().equals( a );
 * assert rel.{@link Relationship#getEndNode() getEndNode}().equals( b );
 * assert rel.{@link Relationship#getNodes() getNodes}()[0].equals( a ) &amp;&amp; rel.{@link Relationship#getNodes() getNodes}()[1].equals( b );
 * </code>
 * </pre>
 * 
 * Even though all relationships have a direction they are equally well
 * traversed in both directions so there's no need to create duplicate
 * relationships in the opposite direction (with regard to traversal or
 * performance).
 * <p>
 * Furthermore, Neo4j guarantees that a relationship is never "hanging freely,"
 * i.e. {@link #getStartNode()}, {@link #getEndNode()},
 * {@link #getOtherNode(Node)} and {@link #getNodes()} are guaranteed to always
 * return valid, non-null nodes.
 * <p>
 * A node's id is unique, but note the following: Neo4j reuses its internal ids
 * when nodes and relationships are deleted, which means it's bad practice to
 * refer to them this way. Instead, use application generated ids.
 */
public interface Relationship extends PropertyContainer
{
    /**
     * Returns the unique id of this relationship. Ids are garbage collected
     * over time so they are only guaranteed to be unique during a specific time
     * span: if the relationship is deleted, it's likely that a new relationship
     * at some point will get the old id. This makes relationship ids brittle as
     * public APIs.
     *
     * @return the id of this node
     */
     long getId();

    /**
     * Deletes this relationship. Invoking any methods on this relationship
     * after <code>delete()</code> has returned is invalid and will lead to
     * {@link NotFoundException} being thrown.
     */
     void delete();

    // Node accessors
    /**
     * Returns the start node of this relationship. For a definition of how
     * start node relates to {@link Direction directions} as arguments to the
     * {@link Node#getRelationships() relationship accessors} in Node, see the
     * class documentation of Relationship.
     *
     * @return the start node of this relationship
     */
    Node getStartNode();

    /**
     * Returns the end node of this relationship. For a definition of how end
     * node relates to {@link Direction directions} as arguments to the
     * {@link Node#getRelationships() relationship accessors} in Node, see the
     * class documentation of Relationship.
     *
     * @return the end node of this relationship
     */
    Node getEndNode();

    /**
     * A convenience operation that, given a node that is attached to this
     * relationship, returns the other node. For example if <code>node</code> is
     * a start node, the end node will be returned, and vice versa. This is a
     * very convenient operation when you're manually traversing the graph
     * by invoking one of the {@link Node#getRelationships() getRelationships()}
     * operations on a node. For example, to get the node "at the other end" of
     * a relationship, use the following:
     * <p>
     * <code>
	 * Node endNode = node.getSingleRelationship( MyRels.REL_TYPE ).getOtherNode( node );
	 * </code>
     * <p>
     * This operation will throw a runtime exception if <code>node</code> is
     * neither this relationship's start node nor its end node.
     *
     * @param node the start or end node of this relationship
     * @return the other node
     * @throws RuntimeException if the given node is neither the start nor end
     *             node of this relationship
     */
    Node getOtherNode( Node node );

    /**
     * Returns the two nodes that are attached to this relationship. The first
     * element in the array will be the start node, the second element the end
     * node.
     *
     * @return the two nodes that are attached to this relationship
     */
    Node[] getNodes();

    /**
     * Returns the type of this relationship. A relationship's type is an
     * immutable attribute that is specified at Relationship
     * {@link Node#createRelationshipTo creation}. Remember that relationship
     * types are semantically equivalent if their
     * {@link RelationshipType#name() names} are {@link Object#equals(Object)
     * equal}. This is NOT the same as checking for identity equality using the
     * == operator. If you want to know whether this relationship is of a
     * certain type, use the {@link #isType(RelationshipType) isType()}
     * operation.
     *
     * @return the type of this relationship
     */
    RelationshipType getType();

    /**
     * Indicates whether this relationship is of the type <code>type</code>.
     * This is a convenience method that checks for equality using the contract
     * specified by RelationshipType, i.e. by checking for equal
     * {@link RelationshipType#name() names}.
     *
     * @param type the type to check
     * @return <code>true</code> if this relationship is of the type
     *         <code>type</code>, <code>false</code> otherwise or if
     *         <code>null</code>
     */
    boolean isType( RelationshipType type );
}
