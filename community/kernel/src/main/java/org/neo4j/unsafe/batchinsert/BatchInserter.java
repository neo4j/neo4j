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
package org.neo4j.unsafe.batchinsert;

import java.util.Map;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.schema.ConstraintCreator;
import org.neo4j.graphdb.schema.IndexCreator;

/**
 * The batch inserter drops support for transactions and concurrency in favor
 * of insertion speed. When done using the batch inserter {@link #shutdown()}
 * must be invoked and complete successfully for the Neo4j store to be in
 * consistent state.
 * <p>
 * Only one thread at a time may work against the batch inserter, multiple
 * threads performing concurrent access have to employ synchronization.
 * <p>
 * Transactions are not supported so if the JVM/machine crashes or you fail to
 * invoke {@link #shutdown()} before JVM exits the Neo4j store can be considered
 * being in non consistent state and the insertion has to be re-done from
 * scratch.
 */
public interface BatchInserter
{
    /**
     * Creates a node assigning next available id to id and also adds any
     * properties supplied.
     *
     * @param properties a map containing properties or <code>null</code> if no
     * properties should be added.
     * @param labels a list of labels to initially create the node with.
     *
     * @return The id of the created node.
     */
    long createNode( Map<String,Object> properties, Label... labels );

    /**
     * Creates a node with supplied id and properties. If a node with the given
     * id exist a runtime exception will be thrown.
     *
     * @param id the id of the node to create.
     * @param properties map containing properties or <code>null</code> if no
     * properties should be added.
     * @param labels a list of labels to initially create the node with.
     */
    void createNode( long id, Map<String,Object> properties, Label... labels );

    /**
     * Checks if a node with the given id exists.
     *
     * @param nodeId the id of the node.
     * @return <code>true</code> if the node exists.
     */
    boolean nodeExists( long nodeId );

    /**
     * Sets the properties of a node. This method will remove any properties
     * already existing and replace it with properties in the supplied map.
     * <p>
     * For best performance try supply all the nodes properties upon creation
     * of the node. This method will delete any existing properties so using it
     * together with {@link #getNodeProperties(long)} will have bad performance.
     *
     * @param node the id of the node.
     * @param properties map containing the properties or <code>null</code> to
     * clear all properties.
     */
    void setNodeProperties( long node, Map<String,Object> properties );

    /**
     * Returns true iff the node with id {@code node} has a property with name
     * {@code propertyName}.
     *
     * @param node The node id of the node to check.
     * @param propertyName The property name to check for
     * @return True if the node has the named property - false otherwise.
     */
    boolean nodeHasProperty( long node, String propertyName );

    /**
     * Replaces any existing labels for the given node with the supplied list of labels.
     *
     * @param node the node to set labels for.
     * @param labels the labels to set for the node.
     */
    void setNodeLabels( long node, Label... labels );

    /**
     * @param node the node to get labels for.
     * @return all labels for the given node.
     */
    Iterable<Label> getNodeLabels( long node );

    /**
     * @param node the node to check.
     * @param label the label to check.
     * @return {@code true} if a node has a given label, otherwise {@code false}.
     */
    boolean nodeHasLabel( long node, Label label );

    /**
     * Returns true iff the relationship with id {@code relationship} has a
     * property with name {@code propertyName}.
     *
     * @param relationship The relationship id of the relationship to check.
     * @param propertyName The property name to check for
     * @return True if the relationship has the named property - false
     *         otherwise.
     */
    boolean relationshipHasProperty( long relationship,
            String propertyName );

    /**
     * Sets the property with name {@code propertyName} of node with id
     * {@code node} to the value {@code propertyValue}. If the property exists
     * it is updated, otherwise created.
     *
     * @param node The node id of the node whose property is to be set
     * @param propertyName The name of the property to set
     * @param propertyValue The value of the property to set
     */
    void setNodeProperty( long node, String propertyName,
            Object propertyValue );

    /**
     * Sets the property with name {@code propertyName} of relationship with id
     * {@code relationship} to the value {@code propertyValue}. If the property
     * exists it is updated, otherwise created.
     *
     * @param relationship The node id of the relationship whose property is to
     *            be set
     * @param propertyName The name of the property to set
     * @param propertyValue The value of the property to set
     */
    void setRelationshipProperty( long relationship,
            String propertyName, Object propertyValue );
    /**
     * Returns a map containing all the properties of this node.
     *
     * @param nodeId the id of the node.
     *
     * @return map containing this node's properties.
     */
    Map<String,Object> getNodeProperties( long nodeId );

    /**
     * Returns an iterable over all the relationship ids connected to node with
     * supplied id.
     *
     * @param nodeId the id of the node.
     * @return iterable over the relationship ids connected to the node.
     */
    Iterable<Long> getRelationshipIds( long nodeId );

    /**
     * Returns an iterable of {@link BatchRelationship relationships} connected
     * to the node with supplied id.
     *
     * @param nodeId the id of the node.
     * @return iterable over the relationships connected to the node.
     */
    Iterable<BatchRelationship> getRelationships( long nodeId );

    /**
     * Creates a relationship between two nodes of a specific type.
     *
     * @param node1 the start node.
     * @param node2 the end node.
     * @param type relationship type.
     * @param properties map containing properties or <code>null</code> if no
     * properties should be added.
     * @return the id of the created relationship.
     */
    long createRelationship( long node1, long node2, RelationshipType
        type, Map<String,Object> properties );

    /**
     * Gets a relationship by id.
     *
     * @param relId the relationship id.
     * @return a simple relationship wrapper for the relationship.
     */
    BatchRelationship getRelationshipById( long relId );

    /**
     * Sets the properties of a relationship. This method will remove any
     * properties already existing and replace it with properties in the
     * supplied map.
     * <p>
     * For best performance try supply all the relationship properties upon
     * creation of the relationship. This method will delete any existing
     * properties so using it together with
     * {@link #getRelationshipProperties(long)} will have bad performance.
     *
     * @param rel the id of the relationship.
     * @param properties map containing the properties or <code>null</code> to
     * clear all properties.
     */
    void setRelationshipProperties( long rel,
        Map<String,Object> properties );

    /**
     * Returns a map containing all the properties of the relationships.
     *
     * @param relId the id of the relationship.
     * @return map containing the relationship's properties.
     */
    Map<String,Object> getRelationshipProperties( long relId );

    /**
     * Removes the property named {@code property} from the node with id
     * {@code id}, if present.
     *
     * @param node The id of the node from which to remove the property
     * @param property The name of the property
     */
    void removeNodeProperty( long node, String property );

    /**
     * Removes the property named {@code property} from the relationship with id
     * {@code id}, if present.
     *
     * @param relationship The id of the relationship from which to remove the
     *            property
     * @param property The name of the property
     */
    void removeRelationshipProperty( long relationship, String property );

    /**
     * Returns an {@link IndexCreator} where details about the index to create can be
     * specified. When all details have been entered {@link IndexCreator#create() create}
     * must be called for it to actually be created.
     *
     * Creating an index enables indexing for nodes with the specified label. The index will
     * have the details supplied to the {@link IndexCreator returned index creator}.
     *
     * Indexes created with the method are deferred until the batch inserter is shut down, at
     * which point a background job will populate all indexes, i.e. the index
     * is not available during the batch insertion itself. It is therefore advisable to
     * create deferred indexes just before shutting down the batch inserter.
     *
     * @param label {@link Label label} on nodes to be indexed
     *
     * @return an {@link IndexCreator} capable of providing details for, as well as creating
     * an index for the given {@link Label label}.
     */
    IndexCreator createDeferredSchemaIndex( Label label );

    /**
     * Returns a {@link ConstraintCreator} where details about the constraint can be
     * specified. When all details have been entered {@link ConstraintCreator#create()}
     * must be called for it to actually be created.
     *
     * Note that the batch inserter is not enforcing any constraint on the inserted data
     * (also the one created with the batch inserter itself).  Although the batch inserter
     * will verify all constraints on {@link #shutdown()} and fail in case of any constraint
     * violation.
     *
     * @param label the label this constraint is for.
     * @return a {@link ConstraintCreator} capable of providing details for, as well as creating
     * a constraint for the given {@linkplain Label label}.
     */
    ConstraintCreator createDeferredConstraint( Label label );

    /**
     * Shuts down this batch inserter syncing all changes that are still only
     * in memory to disk. Failing to invoke this method may leave the Neo4j
     * store in a inconsistent state.
     *
     * Note that this method will trigger population of all indexes, both
     * those created in the batch insertion session, as well as those that existed
     * previously. This may take a long time, depending on data size.
     *
     * <p>
     * After this method has been invoked any other method call to this batch
     * inserter is illegal.
     */
    void shutdown();

    /**
     * Returns the path to this Neo4j store.
     *
     * @return the path to this Neo4j store.
     */
    String getStoreDir();

}
