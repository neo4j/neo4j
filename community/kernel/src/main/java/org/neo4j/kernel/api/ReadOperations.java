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
package org.neo4j.kernel.api;

import java.util.Iterator;
import java.util.Map;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.function.Function;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.constraints.NodePropertyConstraint;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyConstraint;
import org.neo4j.kernel.api.cursor.NodeItem;
import org.neo4j.kernel.api.cursor.RelationshipItem;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.legacyindex.LegacyIndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.DuplicateIndexSchemaRuleException;
import org.neo4j.kernel.api.exceptions.schema.IndexBrokenKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.index.InternalIndexState;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.locking.Locks;

public interface ReadOperations
{
    void acquireExclusive( Locks.ResourceType type, long id );
    void acquireShared(    Locks.ResourceType type, long id );

    void releaseExclusive( Locks.ResourceType type, long id );
    void releaseShared(    Locks.ResourceType type, long id );

    /**
     * @param labelId the label id of the label that returned nodes are guaranteed to have
     * @return ids of all nodes that have the given label
     */
    PrimitiveLongIterator nodesGetForLabel( int labelId );

    /**
     * Returns an iterator with the matched nodes.
     *
     * @throws org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException if no such index found.
     */
    PrimitiveLongIterator nodesGetFromIndexSeek( IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException;

    /**
     * Returns an iterator with the matched nodes.
     *
     * @throws org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException if no such index found.
     */
    PrimitiveLongIterator nodesGetFromIndexRangeSeekByPrefix( IndexDescriptor index, String prefix )
            throws IndexNotFoundKernelException;

    /**
     * Returns an iterator with the matched nodes.
     *
     * @throws org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException if no such index found.
     */
    PrimitiveLongIterator nodesGetFromIndexRangeSeekByNumber( IndexDescriptor index, Number lower, boolean includeLower, Number upper, boolean includeUpper )
            throws IndexNotFoundKernelException;

    /**
     * Returns an iterator with the matched nodes.
     *
     * @throws org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException if no such index found.
     */
    PrimitiveLongIterator nodesGetFromIndexRangeSeekByString( IndexDescriptor index, String lower, boolean includeLower, String upper, boolean includeUpper )
            throws IndexNotFoundKernelException;

    /**
     * Returns an iterator with the matched nodes.
     *
     * @throws org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException if no such index found.
     */
    PrimitiveLongIterator nodesGetFromIndexScan( IndexDescriptor index )
            throws IndexNotFoundKernelException;

    /**
     * @return an iterator over all nodes in the database.
     */
    PrimitiveLongIterator nodesGetAll();

    /**
     * @return an iterator over all relationships in the database.
     */
    PrimitiveLongIterator relationshipsGetAll();

    RelationshipIterator nodeGetRelationships( long nodeId,
                                               Direction direction,
                                               int... relTypes ) throws EntityNotFoundException;

    RelationshipIterator nodeGetRelationships( long nodeId, Direction direction ) throws EntityNotFoundException;

    /**
     * Returns node id of unique node found in the given unique index for value or
     * {@link Statement#NO_SUCH_NODE} if the index does not contain a
     * matching node.
     * <p/>
     * If a node is found, a READ lock for the index entry will be held. If no node
     * is found (if {@link Statement#NO_SUCH_NODE} was returned), a WRITE
     * lock for the index entry will be held. This is to facilitate unique creation
     * of nodes, to build get-or-create semantics on top of this method.
     *
     * @throws org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException if no such index found.
     */
    long nodeGetFromUniqueIndexSeek( IndexDescriptor index, Object value ) throws IndexNotFoundKernelException,
            IndexBrokenKernelException;

    boolean nodeExists( long nodeId );

    boolean relationshipExists( long relId );

    /**
     * Checks if a node is labeled with a certain label or not. Returns
     * {@code true} if the node is labeled with the label, otherwise {@code false.}
     */
    boolean nodeHasLabel( long nodeId, int labelId ) throws EntityNotFoundException;

    int nodeGetDegree( long nodeId, Direction direction, int relType ) throws EntityNotFoundException;

    int nodeGetDegree( long nodeId, Direction direction ) throws EntityNotFoundException;

    boolean nodeIsDense(long nodeId) throws EntityNotFoundException;

    /**
     * Returns all labels set on node with id {@code nodeId}.
     * If the node has no labels an empty {@link Iterable} will be returned.
     */
    PrimitiveIntIterator nodeGetLabels( long nodeId ) throws EntityNotFoundException;

    PrimitiveIntIterator nodeGetPropertyKeys( long nodeId ) throws EntityNotFoundException;

    PrimitiveIntIterator relationshipGetPropertyKeys( long relationshipId ) throws EntityNotFoundException;

    PrimitiveIntIterator graphGetPropertyKeys();

    PrimitiveIntIterator nodeGetRelationshipTypes( long nodeId ) throws EntityNotFoundException;

    boolean nodeHasProperty( long nodeId, int propertyKeyId ) throws EntityNotFoundException;

    Object nodeGetProperty( long nodeId, int propertyKeyId ) throws EntityNotFoundException;

    boolean relationshipHasProperty( long relationshipId, int propertyKeyId ) throws EntityNotFoundException;

    Object relationshipGetProperty( long relationshipId, int propertyKeyId ) throws EntityNotFoundException;

    boolean graphHasProperty( int propertyKeyId );

    Object graphGetProperty( int propertyKeyId );

    <EXCEPTION extends Exception> void relationshipVisit( long relId, RelationshipVisitor<EXCEPTION> visitor )
            throws EntityNotFoundException, EXCEPTION;

    /** Returns the index rule for the given labelId and propertyKey. */
    IndexDescriptor indexesGetForLabelAndPropertyKey( int labelId, int propertyKey )
            throws SchemaRuleNotFoundException;

    /** Get all indexes for a label. */
    Iterator<IndexDescriptor> indexesGetForLabel( int labelId );

    /** Returns all indexes. */
    Iterator<IndexDescriptor> indexesGetAll();

    /** Returns the constraint index for the given labelId and propertyKey. */
    IndexDescriptor uniqueIndexGetForLabelAndPropertyKey( int labelId, int propertyKeyId )
            throws SchemaRuleNotFoundException, DuplicateIndexSchemaRuleException;

    /** Get all constraint indexes for a label. */
    Iterator<IndexDescriptor> uniqueIndexesGetForLabel( int labelId );

    /** Returns all constraint indexes. */
    Iterator<IndexDescriptor> uniqueIndexesGetAll();

    /** Retrieve the state of an index. */
    InternalIndexState indexGetState( IndexDescriptor descriptor ) throws IndexNotFoundKernelException;

    /** Get the index size. */
    long indexSize( IndexDescriptor descriptor ) throws IndexNotFoundKernelException;

    /** Calculate the index unique values percentage (range: {@code 0.0} exclusive to {@code 1.0} inclusive). */
    double indexUniqueValuesSelectivity( IndexDescriptor descriptor ) throws IndexNotFoundKernelException;

    /** Returns the failure description of a failed index. */
    String indexGetFailure( IndexDescriptor descriptor ) throws IndexNotFoundKernelException;

    /**
     * Get all constraints applicable to label and propertyKey. There are only {@link NodePropertyConstraint}
     * for the time being.
     */
    Iterator<NodePropertyConstraint> constraintsGetForLabelAndPropertyKey( int labelId, int propertyKeyId );

    /**
     * Get all constraints applicable to label. There are only {@link NodePropertyConstraint}
     * for the time being.
     */
    Iterator<NodePropertyConstraint> constraintsGetForLabel( int labelId );

    /**
     * Get all constraints applicable to relationship type. There are only {@link RelationshipPropertyConstraint}
     * for the time being.
     */
    Iterator<RelationshipPropertyConstraint> constraintsGetForRelationshipType( int typeId );

    /**
     * Get all constraints applicable to relationship type and propertyKey.
     * There are only {@link RelationshipPropertyConstraint} for the time being.
     */
    Iterator<RelationshipPropertyConstraint> constraintsGetForRelationshipTypeAndPropertyKey( int typeId, int propertyKeyId );

    /**
     * Get all constraints. There are only {@link PropertyConstraint}
     * for the time being.
     */
    Iterator<PropertyConstraint> constraintsGetAll();

    /**
     * Get the owning constraint for a constraint index. Returns null if the index does not have an owning constraint.
     */
    Long indexGetOwningUniquenessConstraintId( IndexDescriptor index ) throws SchemaRuleNotFoundException;

    Map<String, String> nodeLegacyIndexGetConfiguration( String indexName )
            throws LegacyIndexNotFoundKernelException;

    Map<String, String> relationshipLegacyIndexGetConfiguration( String indexName )
            throws LegacyIndexNotFoundKernelException;

    LegacyIndexHits nodeLegacyIndexGet( String indexName, String key, Object value )
            throws LegacyIndexNotFoundKernelException;

    LegacyIndexHits nodeLegacyIndexQuery( String indexName, String key, Object queryOrQueryObject )
            throws LegacyIndexNotFoundKernelException;

    LegacyIndexHits nodeLegacyIndexQuery( String indexName, Object queryOrQueryObject )
            throws LegacyIndexNotFoundKernelException;

    /**
     * @param startNode -1 if ignored.
     * @param endNode -1 if ignored.
     */
    LegacyIndexHits relationshipLegacyIndexGet( String name, String key, Object valueOrNull, long startNode,
                                                long endNode ) throws LegacyIndexNotFoundKernelException;

    /**
     * @param startNode -1 if ignored.
     * @param endNode -1 if ignored.
     */
    LegacyIndexHits relationshipLegacyIndexQuery( String indexName, String key, Object queryOrQueryObject,
                                                  long startNode, long endNode )
            throws LegacyIndexNotFoundKernelException;

    /**
     * @param startNode -1 if ignored.
     * @param endNode -1 if ignored.
     */
    LegacyIndexHits relationshipLegacyIndexQuery( String indexName, Object queryOrQueryObject,
                                                  long startNode, long endNode )
            throws LegacyIndexNotFoundKernelException;

    String[] nodeLegacyIndexesGetAll();

    String[] relationshipLegacyIndexesGetAll();


    /**
     * The number of nodes in the graph.
     *
     * If the label parameter is {@link Statement#ANY_LABEL}, this method returns the total number of nodes in the graph, i.e.
     * {@code MATCH (n) RETURN count(n)}.
     *
     * If the label parameter is set to any other value, this method returns the number of nodes that has that label,
     * i.e. {@code MATCH (n:LBL) RETURN count(n)}.
     *
     * @param labelId the label to get the count for, or {@link Statement#ANY_LABEL} to get the total number of nodes.
     * @return the number of matching nodes in the graph.
     */
    long countsForNode( int labelId );

    /**
     * The number of relationships in the graph.
     *
     * Returns the number of relationships in the graph that matches the specified pattern,
     * {@code (:startLabelId)-[:typeId]->(:endLabelId)}, like so:
     *
     * <table>
     * <thead>
     * <tr><th>{@code startLabelId}</th><th>{@code typeId}</th>                  <th>{@code endLabelId}</th>
     * <td></td>                 <th>Pattern</th>                       <td></td></tr>
     * </thead>
     * <tdata>
     * <tr>
     * <td>{@link Statement#ANY_LABEL}</td>      <td>{@link Statement#ANY_RELATIONSHIP_TYPE}</td>  <td>{@link Statement#ANY_LABEL}</td>
     * <td>{@code MATCH}</td>    <td>{@code ()-[r]->()}</td>            <td>{@code RETURN count(r)}</td>
     * </tr>
     * <tr>
     * <td>{@link Statement#ANY_LABEL}</td>      <td>{@code REL}</td>                     <td>{@link Statement#ANY_LABEL}</td>
     * <td>{@code MATCH}</td>    <td>{@code ()-[r:REL]->()}</td>        <td>{@code RETURN count(r)}</td>
     * </tr>
     * <tr>
     * <td>{@code LHS}</td>             <td>{@link Statement#ANY_RELATIONSHIP_TYPE}</td>  <td>{@link Statement#ANY_LABEL}</td>
     * <td>{@code MATCH}</td>    <td>{@code (:LHS)-[r]->()}</td>        <td>{@code RETURN count(r)}</td>
     * </tr>
     * <tr>
     * <td>{@link Statement#ANY_LABEL}</td>      <td>{@link Statement#ANY_RELATIONSHIP_TYPE}</td>  <td>{@code RHS}</td>
     * <td>{@code MATCH}</td>    <td>{@code ()-[r]->(:RHS)}</td>        <td>{@code RETURN count(r)}</td>
     * </tr>
     * <tr>
     * <td>{@code LHS}</td>             <td>{@code REL}</td>                     <td>{@link Statement#ANY_LABEL}</td>
     * <td>{@code MATCH}</td>    <td>{@code (:LHS)-[r:REL]->()}</td>    <td>{@code RETURN count(r)}</td>
     * </tr>
     * <tr>
     * <td>{@link Statement#ANY_LABEL}</td>      <td>{@code REL}</td>                     <td>{@code RHS}</td>
     * <td>{@code MATCH}</td>    <td>{@code ()-[r:REL]->(:RHS)}</td>    <td>{@code RETURN count(r)}</td>
     * </tr>
     * <tr>
     * <td>{@code LHS}</td>             <td>{@link Statement#ANY_RELATIONSHIP_TYPE}</td>  <td>{@code RHS}</td>
     * <td>{@code MATCH}</td>    <td>{@code (:LHS)-[r]->(:RHS)}</td>    <td>{@code RETURN count(r)}</td>
     * </tr>
     * <tr>
     * <td>{@code LHS}</td>             <td>{@code REL}</td>                     <td>{@code RHS}</td>
     * <td>{@code MATCH}</td>    <td>{@code (:LHS)-[r:REL]->(:RHS)}</td><td>{@code RETURN count(r)}</td>
     * </tr>
     * </tdata>
     * </table>
     *
     * @param startLabelId the label of the start node of relationships to get the count for, or {@link Statement#ANY_LABEL}.
     * @param typeId       the type of relationships to get a count for, or {@link Statement#ANY_RELATIONSHIP_TYPE}.
     * @param endLabelId   the label of the end node of relationships to get the count for, or {@link Statement#ANY_LABEL}.
     * @return the number of matching relationships in the graph.
     */
    long countsForRelationship( int startLabelId, int typeId, int endLabelId );


    /** Returns a label id for a label name. If the label doesn't exist, {@link Statement#NO_SUCH_LABEL} will be returned. */
    int labelGetForName( String labelName );

    /** Returns the label name for the given label id. */
    String labelGetName( int labelId ) throws LabelNotFoundKernelException;

    /**
     * Returns a property key id for the given property key. If the property key doesn't exist,
     * {@link Statement#NO_SUCH_PROPERTY_KEY} will be returned.
     */
    int propertyKeyGetForName( String propertyKeyName );

    /** Returns the name of a property given its property key id */
    String propertyKeyGetName( int propertyKeyId ) throws PropertyKeyIdNotFoundKernelException;

    /** Returns the property keys currently stored in the database */
    Iterator<Token> propertyKeyGetAllTokens();

    /** Returns the labels currently stored in the database * */
    Iterator<Token> labelsGetAllTokens(); // TODO: Token is a store level concern, should not make it this far up the stack

    int relationshipTypeGetForName( String relationshipTypeName );

    String relationshipTypeGetName( int relationshipTypeId ) throws RelationshipTypeIdNotFoundKernelException;

    Cursor<NodeItem> nodeCursor( long nodeId );

    Cursor<RelationshipItem> relationshipCursor( long relId );

    <K, V> V schemaStateGetOrCreate( K key, Function<K, V> creator );

    void schemaStateFlush();
}
