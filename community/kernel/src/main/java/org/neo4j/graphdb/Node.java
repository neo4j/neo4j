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

import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.Traversal;

/**
 * A node in the graph with properties and relationships to other entities.
 * Along with {@link Relationship relationships}, nodes are the core building
 * blocks of the Neo4j data representation model. Nodes are created by invoking
 * the {@link GraphDatabaseService#createNode} method.
 * <p>
 * Node has three major groups of operations: operations that deal with
 * relationships, operations that deal with properties (see
 * {@link PropertyContainer}) and operations that create {@link Traverser
 * traversers}.
 * <p>
 * The relationship operations provide a number of overloaded accessors (such as
 * <code>getRelationships(...)</code> with "filters" for type, direction, etc),
 * as well as the factory method {@link #createRelationshipTo
 * createRelationshipTo(...)} that connects two nodes with a relationship. It
 * also includes the convenience method {@link #getSingleRelationship
 * getSingleRelationship(...)} for accessing the commonly occurring
 * one-to-zero-or-one association.
 * <p>
 * The property operations give access to the key-value property pairs. Property
 * keys are always strings. Valid property value types are all the Java
 * primitives (<code>int</code>, <code>byte</code>, <code>float</code>, etc),
 * <code>java.lang.String</code>s and arrays of primitives and Strings.
 * <p>
 * <b>Please note</b> that Neo4j does NOT accept arbitrary objects as property
 * values. {@link #setProperty(String, Object) setProperty()} takes a
 * <code>java.lang.Object</code> only to avoid an explosion of overloaded
 * <code>setProperty()</code> methods. For further documentation see
 * {@link PropertyContainer}.
 * <p>
 * The traversal factory methods instantiate a {@link Traverser traverser} that
 * starts traversing from this node.
 * <p>
 * A node's id is unique, but note the following: Neo4j reuses its internal ids
 * when nodes and relationships are deleted, which means it's bad practice to
 * refer to them this way. Instead, use application generated ids.
 */
public interface Node extends PropertyContainer
{
    /**
     * Returns the unique id of this node. Ids are garbage collected over time
     * so they are only guaranteed to be unique during a specific time span: if
     * the node is deleted, it's likely that a new node at some point will get
     * the old id. <b>Note</b>: this makes node ids brittle as public APIs.
     *
     * @return the id of this node
     */
    long getId();

    /**
     * Deletes this node if it has no relationships attached to it. If
     * <code>delete()</code> is invoked on a node with relationships, an
     * unchecked exception will be raised when the transaction is committing.
     * Invoking any methods on this node after <code>delete()</code> has
     * returned is invalid and will lead to {@link NotFoundException} being thrown.
     */
    void delete();

    /**
     * Returns all the relationships attached to this node. If no relationships
     * are attached to this node, an empty iterable will be returned.
     *
     * @return all relationships attached to this node
     */
    Iterable<Relationship> getRelationships();

    /**
     * Returns <code>true</code> if there are any relationships attached to this
     * node, <code>false</code> otherwise.
     *
     * @return <code>true</code> if there are any relationships attached to this
     *         node, <code>false</code> otherwise
     */
    boolean hasRelationship();

    /**
     * Returns all the relationships of any of the types in <code>types</code>
     * that are attached to this node, regardless of direction. If no
     * relationships of the given types are attached to this node, an empty
     * iterable will be returned.
     *
     * @param types the given relationship type(s)
     * @return all relationships of the given type(s) that are attached to this
     *         node
     */
    Iterable<Relationship> getRelationships( RelationshipType... types );

    /**
     * Returns all the relationships of any of the types in <code>types</code>
     * that are attached to this node and have the given <code>direction</code>.
     * If no relationships of the given types are attached to this node, an empty
     * iterable will be returned.
     *
     * @param types the given relationship type(s)
     * @param direction the direction of the relationships to return.
     * @return all relationships of the given type(s) that are attached to this
     *         node
     */
    Iterable<Relationship> getRelationships( Direction direction, RelationshipType... types );

    /**
     * Returns <code>true</code> if there are any relationships of any of the
     * types in <code>types</code> attached to this node (regardless of
     * direction), <code>false</code> otherwise.
     *
     * @param types the given relationship type(s)
     * @return <code>true</code> if there are any relationships of any of the
     *         types in <code>types</code> attached to this node,
     *         <code>false</code> otherwise
     */
    boolean hasRelationship( RelationshipType... types );

    /**
     * Returns <code>true</code> if there are any relationships of any of the
     * types in <code>types</code> attached to this node (for the given
     * <code>direction</code>), <code>false</code> otherwise.
     *
     * @param types the given relationship type(s)
     * @param direction the direction to check relationships for
     * @return <code>true</code> if there are any relationships of any of the
     *         types in <code>types</code> attached to this node,
     *         <code>false</code> otherwise
     */
    boolean hasRelationship( Direction direction, RelationshipType... types );

    /**
     * Returns all {@link Direction#OUTGOING OUTGOING} or
     * {@link Direction#INCOMING INCOMING} relationships from this node. If
     * there are no relationships with the given direction attached to this
     * node, an empty iterable will be returned. If {@link Direction#BOTH BOTH}
     * is passed in as a direction, relationships of both directions are
     * returned (effectively turning this into <code>getRelationships()</code>).
     *
     * @param dir the given direction, where <code>Direction.OUTGOING</code>
     *            means all relationships that have this node as
     *            {@link Relationship#getStartNode() start node} and <code>
	 * Direction.INCOMING</code>
     *            means all relationships that have this node as
     *            {@link Relationship#getEndNode() end node}
     * @return all relationships with the given direction that are attached to
     *         this node
     */
    Iterable<Relationship> getRelationships( Direction dir );

    /**
     * Returns <code>true</code> if there are any relationships in the given
     * direction attached to this node, <code>false</code> otherwise. If
     * {@link Direction#BOTH BOTH} is passed in as a direction, relationships of
     * both directions are matched (effectively turning this into
     * <code>hasRelationships()</code>).
     *
     * @param dir the given direction, where <code>Direction.OUTGOING</code>
     *            means all relationships that have this node as
     *            {@link Relationship#getStartNode() start node} and <code>
	 * Direction.INCOMING</code>
     *            means all relationships that have this node as
     *            {@link Relationship#getEndNode() end node}
     * @return <code>true</code> if there are any relationships in the given
     *         direction attached to this node, <code>false</code> otherwise
     */
    boolean hasRelationship( Direction dir );

    /**
     * Returns all relationships with the given type and direction that are
     * attached to this node. If there are no matching relationships, an empty
     * iterable will be returned.
     *
     * @param type the given type
     * @param dir the given direction, where <code>Direction.OUTGOING</code>
     *            means all relationships that have this node as
     *            {@link Relationship#getStartNode() start node} and <code>
	 * Direction.INCOMING</code>
     *            means all relationships that have this node as
     *            {@link Relationship#getEndNode() end node}
     * @return all relationships attached to this node that match the given type
     *         and direction
     */
    Iterable<Relationship> getRelationships( RelationshipType type, Direction dir );

    /**
     * Returns <code>true</code> if there are any relationships of the given
     * relationship type and direction attached to this node, <code>false</code>
     * otherwise.
     *
     * @param type the given type
     * @param dir the given direction, where <code>Direction.OUTGOING</code>
     *            means all relationships that have this node as
     *            {@link Relationship#getStartNode() start node} and <code>
	 * Direction.INCOMING</code>
     *            means all relationships that have this node as
     *            {@link Relationship#getEndNode() end node}
     * @return <code>true</code> if there are any relationships of the given
     *         relationship type and direction attached to this node,
     *         <code>false</code> otherwise
     */
    boolean hasRelationship( RelationshipType type, Direction dir );

    /**
     * Returns the only relationship of a given type and direction that is
     * attached to this node, or <code>null</code>. This is a convenience method
     * that is used in the commonly occuring situation where a node has exactly
     * zero or one relationships of a given type and direction to another node.
     * Typically this invariant is maintained by the rest of the code: if at any
     * time more than one such relationships exist, it is a fatal error that
     * should generate an unchecked exception. This method reflects that
     * semantics and returns either:
     * <ol>
     * <li><code>null</code> if there are zero relationships of the given type
     * and direction,</li>
     * <li>the relationship if there's exactly one, or
     * <li>throws an unchecked exception in all other cases.</li>
     * </ol>
     * <p>
     * This method should be used only in situations with an invariant as
     * described above. In those situations, a "state-checking" method (e.g.
     * <code>hasSingleRelationship(...)</code>) is not required, because this
     * method behaves correctly "out of the box."
     *
     * @param type the type of the wanted relationship
     * @param dir the direction of the wanted relationship (where
     *            <code>Direction.OUTGOING</code> means a relationship that has
     *            this node as {@link Relationship#getStartNode() start node}
     *            and <code>
	 * Direction.INCOMING</code> means a relationship that has
     *            this node as {@link Relationship#getEndNode() end node}) or
     *            {@link Direction#BOTH} if direction is irrelevant
     * @return the single relationship matching the given type and direction if
     *         exactly one such relationship exists, or <code>null</code> if
     *         exactly zero such relationships exists
     * @throws RuntimeException if more than one relationship matches the given
     *             type and direction
     */
    Relationship getSingleRelationship( RelationshipType type, Direction dir );

    /**
     * Creates a relationship between this node and another node. The
     * relationship is of type <code>type</code>. It starts at this node and
     * ends at <code>otherNode</code>.
     * <p>
     * A relationship is equally well traversed in both directions so there's no
     * need to create another relationship in the opposite direction (in regards
     * to traversal or performance).
     *
     * @param otherNode the end node of the new relationship
     * @param type the type of the new relationship
     * @return the newly created relationship
     */
    Relationship createRelationshipTo( Node otherNode, RelationshipType type );

    /**
     * Returns relationship types which this node has one more relationships
     * for. If this node doesn't have any relationships an empty {@link Iterable}
     * will be returned.
     * @return relationship types which this node has one more relationships for.
     */
    public Iterable<RelationshipType> getRelationshipTypes();

    /**
     * Returns the number of relationships connected to this node regardless of
     * direction or type. This operation is always O(1).
     * @return the number of relationships connected to this node.
     */
    public int getDegree();

    /**
     * Returns the number of relationships of a given {@code type} connected to this node.
     *
     * @param type the type of relationships to get the degree for
     * @return the number of relationships of a given {@code type} connected to this node.
     */
    public int getDegree( RelationshipType type );

    /**
     * Returns the number of relationships of a given {@code direction} connected to this node.
     *
     * @param direction the direction of the relationships
     * @return the number of relationships of a given {@code direction} for this node.
     */
    public int getDegree( Direction direction );

    /**
     * Returns the number of relationships of a given {@code type} and {@code direction}
     * connected to this node.
     *
     * @param type the type of relationships to get the degree for
     * @param direction the direction of the relationships
     * @return the number of relationships of a given {@code type} and {@code direction}
     * for this node.
     */
    public int getDegree( RelationshipType type, Direction direction );

    /**
     * Instantiates a traverser that will start at this node and traverse
     * according to the given order and evaluators along the specified
     * relationship type and direction. If the traverser should traverse more
     * than one <code>RelationshipType</code>/<code>Direction</code> pair, use
     * one of the overloaded variants of this method. The created traverser will
     * iterate over each node that can be reached from this node by the spanning
     * tree formed by the given relationship types (with direction) exactly
     * once. For more information about traversal, see the {@link Traverser}
     * documentation.
     *
     *
     * @param traversalOrder the traversal order
     * @param stopEvaluator an evaluator instructing the new traverser about
     *            when to stop traversing, either a predefined evaluator such as
     *            {@link StopEvaluator#END_OF_GRAPH} or a custom-written
     *            evaluator
     * @param returnableEvaluator an evaluator instructing the new traverser
     *            about whether a specific node should be returned from the
     *            traversal, either a predefined evaluator such as
     *            {@link ReturnableEvaluator#ALL} or a customer-written
     *            evaluator
     * @param relationshipType the relationship type that the traverser will
     *            traverse along
     * @param direction the direction in which the relationships of type
     *            <code>relationshipType</code> will be traversed
     * @return a new traverser, configured as above
     * @deprecated because of an unnatural and too tight coupling with
     *             {@link Node}. Also because of the introduction of a new
     *             traversal framework. The new way of doing traversals is by
     *             creating a new {@link TraversalDescription} from
     *             {@link Traversal#traversal()}, add rules and behaviors to it
     *             and then calling
     *             {@link TraversalDescription#traverse(Node...)}
     */
    @Deprecated
    Traverser traverse( Traverser.Order traversalOrder,
            StopEvaluator stopEvaluator,
            ReturnableEvaluator returnableEvaluator,
            RelationshipType relationshipType, Direction direction );

    /**
     * Instantiates a traverser that will start at this node and traverse
     * according to the given order and evaluators along the two specified
     * relationship type and direction pairs. If the traverser should traverse
     * more than two <code>RelationshipType</code>/<code>Direction</code> pairs,
     * use the overloaded
     * {@link #traverse(Traverser.Order, StopEvaluator, ReturnableEvaluator, Object...)
     * varargs variant} of this method. The created traverser will iterate over
     * each node that can be reached from this node by the spanning tree formed
     * by the given relationship types (with direction) exactly once. For more
     * information about traversal, see the {@link Traverser} documentation.
     *
     * @param traversalOrder the traversal order
     * @param stopEvaluator an evaluator instructing the new traverser about
     *            when to stop traversing, either a predefined evaluator such as
     *            {@link StopEvaluator#END_OF_GRAPH} or a custom-written
     *            evaluator
     * @param returnableEvaluator an evaluator instructing the new traverser
     *            about whether a specific node should be returned from the
     *            traversal, either a predefined evaluator such as
     *            {@link ReturnableEvaluator#ALL} or a customer-written
     *            evaluator
     * @param firstRelationshipType the first of the two relationship types that
     *            the traverser will traverse along
     * @param firstDirection the direction in which the first relationship type
     *            will be traversed
     * @param secondRelationshipType the second of the two relationship types
     *            that the traverser will traverse along
     * @param secondDirection the direction that the second relationship type
     *            will be traversed
     * @return a new traverser, configured as above
     * @deprecated because of an unnatural and too tight coupling with
     * {@link Node}. Also because of the introduction of a new traversal
     * framework. The new way of doing traversals is by creating a
     * new {@link TraversalDescription} from
     * {@link Traversal#traversal()}, add rules and
     * behaviors to it and then calling
     * {@link TraversalDescription#traverse(Node...)}
     */
    @Deprecated
    Traverser traverse( Traverser.Order traversalOrder,
            StopEvaluator stopEvaluator,
            ReturnableEvaluator returnableEvaluator,
            RelationshipType firstRelationshipType, Direction firstDirection,
            RelationshipType secondRelationshipType, Direction secondDirection );

    /**
     * Instantiates a traverser that will start at this node and traverse
     * according to the given order and evaluators along the specified
     * relationship type and direction pairs. Unlike the overloaded variants of
     * this method, the relationship types and directions are passed in as a
     * "varargs" variable-length argument which means that an arbitrary set of
     * relationship type and direction pairs can be specified. The
     * variable-length argument list should be every other relationship type and
     * direction, starting with relationship type, e.g:
     * <p>
     * <code>node.traverse( BREADTH_FIRST, stopEval, returnableEval,
     * MyRels.REL1, Direction.OUTGOING, MyRels.REL2, Direction.OUTGOING,
     * MyRels.REL3, Direction.BOTH, MyRels.REL4, Direction.INCOMING );</code>
     * <p>
     * Unfortunately, the compiler cannot enforce this so an unchecked exception
     * is raised if the variable-length argument has a different constitution.
     * <p>
     * The created traverser will iterate over each node that can be reached
     * from this node by the spanning tree formed by the given relationship
     * types (with direction) exactly once. For more information about
     * traversal, see the {@link Traverser} documentation.
     *
     * @param traversalOrder the traversal order
     * @param stopEvaluator an evaluator instructing the new traverser about
     *            when to stop traversing, either a predefined evaluator such as
     *            {@link StopEvaluator#END_OF_GRAPH} or a custom-written
     *            evaluator
     * @param returnableEvaluator an evaluator instructing the new traverser
     *            about whether a specific node should be returned from the
     *            traversal, either a predefined evaluator such as
     *            {@link ReturnableEvaluator#ALL} or a customer-written
     *            evaluator
     * @param relationshipTypesAndDirections a variable-length list of
     *            relationship types and their directions, where the first
     *            argument is a relationship type, the second argument the first
     *            type's direction, the third a relationship type, the fourth
     *            its direction, etc
     * @return a new traverser, configured as above
     * @throws RuntimeException if the variable-length relationship type /
     *             direction list is not as described above
     * @deprecated because of an unnatural and too tight coupling with
     * {@link Node}. Also because of the introduction of a new traversal
     * framework. The new way of doing traversals is by creating a
     * new {@link TraversalDescription} from
     * {@link Traversal#traversal()}, add rules and
     * behaviors to it and then calling
     * {@link TraversalDescription#traverse(Node...)}
     */
    @Deprecated
    Traverser traverse( Traverser.Order traversalOrder,
            StopEvaluator stopEvaluator,
            ReturnableEvaluator returnableEvaluator,
            Object... relationshipTypesAndDirections );

    /**
     * Adds a {@link Label} to this node. If this node doesn't already have
     * this label it will be added. If it already has the label, nothing will happen.
     *
     * @param label the label to add to this node.
     */
    void addLabel( Label label );

    /**
     * Removes a {@link Label} from this node. If this node doesn't have this label,
     * nothing will happen.
     *
     * @param label the label to remove from this node.
     */
    void removeLabel( Label label );

    /**
     * Checks whether or not this node has the given label.
     *
     * @param label the label to check for.
     * @return {@code true} if this node has the given label, otherwise {@code false}.
     */
    boolean hasLabel( Label label );

    /**
     * Lists all labels attached to this node. If this node has no
     * labels an empty {@link Iterable} will be returned.
     *
     * @return all labels attached to this node.
     */
    Iterable<Label> getLabels();
}
