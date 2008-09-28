/*
 * Copyright (c) 2002-2008 "Neo Technology,"
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

/**
 * A relationship between two nodes in the graph. A relationship has a start
 * node, an end node and a {@link RelationshipType type}. You can attach
 * properties to relationships with the API specified in
 * {@link PropertyContainer}.
 * <p>
 * Relationships are created by invoking the {@link Node#createRelationshipTo
 * Node.createRelationshipTo()} method on a node as follows:
 * <p>
 * <code>
 * Relationship rel = node.createRelationshipTo( otherNode, MyRels.REL_TYPE );
 * </code>
 * <p>
 * The fact that the relationship API gives meaning to {@link #getStartNode()
 * start} and {@link #getEndNode() end} nodes implicitly means that all
 * relationships have a direction. In the example above, <code>rel</code>
 * would be directed <i>from</i> <code>node</code> <i>to</i>
 * <code>otherNode</code>. A relationship's start node and end node and their
 * relation to {@link Direction#OUTGOING} and {@link Direction#INCOMING} are
 * defined so that the assertions in the following code are <code>true</code>:
 * <code><pre>
 * Node a = neo.createNode(), b = neo.createNode();
 * Relationship rel = a.createRelationshipTo( b, MyRels.REL_TYPE );
 * // Now we have: (a) --- REL_TYPE ---&gt; (b)
 * 
 * assert rel.getStartNode().equals( a );
 * assert rel.getEndNode().equals( b );
 * assert rel.getNodes()[0].equals( a ) &amp;&amp; rel.getNodes()[1].equals( b );
 * </pre></code>
 * 
 * Furthermore, Neo guarantees that a relationship is never "hanging freely,"
 * i.e. {@link #getStartNode()}, {@link #getEndNode()},
 * {@link #getOtherNode(Node)} and {@link #getNodes()} are guaranteed to always
 * return valid, non-null nodes.
 */
public interface Relationship extends PropertyContainer
{
	/**
	 * Returns the unique id of this relationship. Ids are garbage collected
	 * over time so are only guaranteed to be unique at a specific set of time:
	 * if the relationship is deleted, it's likely that a new relationship at
	 * some point will get the old id. This make relationship ids brittle as
	 * public APIs.
	 * @return the id of this node
	 */
	public long getId();
	
	/**
	 * Deletes this relationship. Invoking any methods on this relationship
	 * after <code>delete()</code> has returned is invalid and will lead to
	 * unspecified behavior.
	 */
	public void delete();	

	// Node acessors
	/**
	 * Returns the start node of this relationship. For a definition of how
	 * start node relates to {@link Direction directions} as arguments to the
	 * {@link Node#getRelationships() relationship accessors} in Node, see the
	 * class documentation of Relationship.
	 * @return the start node of this relationship
	 */
	public Node getStartNode();
	
	/**
	 * Returns the end node of this relationship. For a definition of how
	 * end node relates to {@link Direction directions} as arguments to the
	 * {@link Node#getRelationships() relationship accessors} in Node, see the
	 * class documentation of Relationship.
	 * @return the end node of this relationship
	 */
	public Node getEndNode();
	
	/**
	 * A convenience operation that, given a node that is attached to this
	 * relationship, returns the other node. For example if <code>node</code>
	 * is a start node, the end node will be returned, and vice versa. This
	 * is a very convenient operation when you're manually traversing the
	 * node space by invoking one of the {@link Node#getRelationships()
	 * getRelationships()} operations on node. For example, to get the node
	 * "at the other end" of a relationship, use the following:
	 * <p>
	 * <code>
	 * Node endNode = node.getSingleRelationship( MyRels.REL_TYPE ).getOtherNode ( node );
	 * </code>
	 * <p>
	 * This operation will throw a runtime exception if <code>node</code> is
	 * neither this relationship's start node nor its end node.
	 * @param node the start or end node of this relationship
	 * @return the other node
	 * @throws RuntimeException if the given node is neither the start nor
	 * end node of this relationship
	 */
	public Node getOtherNode( Node node );
	
	/**
	 * Returns the two nodes that are attached to this relationship. First
	 * element in the array will be the start node, the second element the
	 * end node.
	 * @return the two nodes that are attached to this relationship
	 */
	public Node[] getNodes();
	
	/**
	 * Returns the type of this relationship. A relationship's type is an
	 * immutable property that is specified at Relationship
	 *  {@link Node#createRelationshipTo creation}. Remember that relationship
	 *  types are semantically equivalent if their
	 *  {@link RelationshipType#name() names} are
	 *  {@link Object#equals(Object) equal}. This is NOT the same as checking
	 *  for identity equality using the == operator. If you want to know
	 *  whether this relationship is of a certain type, use the
	 *  {@link #isType(RelationshipType) isType()} operation.
	 * @return the type of this relationship
	 */	
	public RelationshipType getType();
	
	/**
	 * Indicates whether this relationship is of the type <code>type</code>.
	 * This is a convenience method that checks for equality using the
	 * contract specified by RelationshipType, i.e. by checking for equal
	 * {@link RelationshipType#name() names}.
	 * @param type the type to check
	 * @return <code>true</code> if this relationship is of the type
	 * <code>type</code>, <code>false</code> otherwise or if <code>null</code>
	 */
	public boolean isType( RelationshipType type );
}
