/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.explicitindex.ExplicitIndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.IndexBrokenKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.proc.ProcedureSignature;
import org.neo4j.kernel.api.proc.QualifiedName;
import org.neo4j.kernel.api.proc.UserFunctionSignature;
import org.neo4j.kernel.api.schema.IndexQuery;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.operations.KeyReadOperations;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.storageengine.api.Token;
import org.neo4j.storageengine.api.lock.ResourceType;
import org.neo4j.storageengine.api.schema.PopulationProgress;
import org.neo4j.values.storable.Value;

/**
 * Defines all types of read operations that can be done from the {@link KernelAPI}.
 */
public interface ReadOperations
{
    int ANY_LABEL = -1;
    int ANY_RELATIONSHIP_TYPE = -1;

    //===========================================
    //== TOKEN OPERATIONS =======================
    //===========================================

    /** Returns a label id for a label name. If the label doesn't exist, {@link KeyReadOperations#NO_SUCH_LABEL}
     * will be returned. */
    int labelGetForName( String labelName );

    /** Returns the label name for the given label id. */
    String labelGetName( int labelId ) throws LabelNotFoundKernelException;

    /** Returns the labels currently stored in the database * */
    Iterator<Token> labelsGetAllTokens(); // TODO: Token is a store level concern, should not make it this far up the stack

    /**
     * Returns a property key id for the given property key. If the property key doesn't exist,
     * {@link StatementConstants#NO_SUCH_PROPERTY_KEY} will be returned.
     */
    int propertyKeyGetForName( String propertyKeyName );

    /** Returns the name of a property given its property key id */
    String propertyKeyGetName( int propertyKeyId ) throws PropertyKeyIdNotFoundKernelException;

    /** Returns the property keys currently stored in the database */
    Iterator<Token> propertyKeyGetAllTokens();

    int relationshipTypeGetForName( String relationshipTypeName );

    String relationshipTypeGetName( int relationshipTypeId ) throws RelationshipTypeIdNotFoundKernelException;

    /** Returns the relationship types currently stored in the database */
    Iterator<Token> relationshipTypesGetAllTokens();

    int labelCount();

    int propertyKeyCount();

    int relationshipTypeCount();

    //===========================================
    //== DATA OPERATIONS ========================
    //===========================================

    /**
     * @param labelId the label id of the label that returned nodes are guaranteed to have
     * @return ids of all nodes that have the given label
     */
    PrimitiveLongIterator nodesGetForLabel( int labelId );

    /**
     * Queries the given index with the given index query.
     *
     * @param index the index to query against.
     * @param predicates array of the {@link IndexQuery} predicates to query for.
     * @return ids of the matching nodes
     * @throws org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException if no such index is found.
     */
    PrimitiveLongIterator indexQuery( IndexDescriptor index, IndexQuery... predicates )
            throws IndexNotFoundKernelException, IndexNotApplicableKernelException;

    /**
     * @return an iterator over all nodes in the database.
     */
    PrimitiveLongIterator nodesGetAll();

    /**
     * @return an iterator over all relationships in the database.
     */
    PrimitiveLongIterator relationshipsGetAll();

    RelationshipIterator nodeGetRelationships( long nodeId, Direction direction, int[] relTypes )
            throws EntityNotFoundException;

    RelationshipIterator nodeGetRelationships( long nodeId, Direction direction ) throws EntityNotFoundException;

    /**
     * Returns node id of unique node found in the given unique index for value or
     * {@link StatementConstants#NO_SUCH_NODE} if the index does not contain a
     * matching node.
     * <p/>
     * If a node is found, a READ lock for the index entry will be held. If no node
     * is found (if {@link StatementConstants#NO_SUCH_NODE} was returned), a WRITE
     * lock for the index entry will be held. This is to facilitate unique creation
     * of nodes, to build get-or-create semantics on top of this method.
     *
     * @throws org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException if no such index found.
     */
    long nodeGetFromUniqueIndexSeek( IndexDescriptor index, IndexQuery.ExactPredicate... predicates ) throws IndexNotFoundKernelException,
            IndexBrokenKernelException, IndexNotApplicableKernelException;

    long nodesCountIndexed( IndexDescriptor index, long nodeId, Value value )
            throws IndexNotFoundKernelException, IndexBrokenKernelException;

    boolean nodeExists( long nodeId );

    /**
     * Checks if a node is labeled with a certain label or not. Returns
     * {@code true} if the node is labeled with the label, otherwise {@code false.}
     */
    boolean nodeHasLabel( long nodeId, int labelId ) throws EntityNotFoundException;

    int nodeGetDegree( long nodeId, Direction direction, int relType ) throws EntityNotFoundException;

    int nodeGetDegree( long nodeId, Direction direction ) throws EntityNotFoundException;

    boolean nodeIsDense( long nodeId ) throws EntityNotFoundException;

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

    Value nodeGetProperty( long nodeId, int propertyKeyId ) throws EntityNotFoundException;

    boolean relationshipHasProperty( long relationshipId, int propertyKeyId ) throws EntityNotFoundException;

    Value relationshipGetProperty( long relationshipId, int propertyKeyId ) throws EntityNotFoundException;

    boolean graphHasProperty( int propertyKeyId );

    Value graphGetProperty( int propertyKeyId );

    <EXCEPTION extends Exception> void relationshipVisit( long relId, RelationshipVisitor<EXCEPTION> visitor )
            throws EntityNotFoundException, EXCEPTION;

    long nodesGetCount();

    long relationshipsGetCount();

    //===========================================
    //== CURSOR ACCESS OPERATIONS ===============
    //===========================================

    Cursor<NodeItem> nodeCursorById( long nodeId ) throws EntityNotFoundException;

    Cursor<RelationshipItem> relationshipCursorById( long relId ) throws EntityNotFoundException;

    Cursor<PropertyItem> nodeGetProperties( NodeItem node );

    Cursor<PropertyItem> relationshipGetProperties( RelationshipItem relationship );

    //===========================================
    //== SCHEMA OPERATIONS ======================
    //===========================================

    /** Returns the index rule for the given LabelSchemaDescriptor. */
    IndexDescriptor indexGetForSchema( LabelSchemaDescriptor descriptor )
            throws SchemaRuleNotFoundException;

    /** Get all indexes for a label. */
    Iterator<IndexDescriptor> indexesGetForLabel( int labelId );

    /** Returns all indexes. */
    Iterator<IndexDescriptor> indexesGetAll();

    /** Retrieve the state of an index. */
    InternalIndexState indexGetState( IndexDescriptor descriptor ) throws IndexNotFoundKernelException;

    /** Retrieve the population progress of an index. */
    PopulationProgress indexGetPopulationProgress( IndexDescriptor descriptor ) throws IndexNotFoundKernelException;

    /** Get the index size. */
    long indexSize( IndexDescriptor descriptor ) throws IndexNotFoundKernelException;

    /** Calculate the index unique values percentage (range: {@code 0.0} exclusive to {@code 1.0} inclusive). */
    double indexUniqueValuesSelectivity( IndexDescriptor descriptor ) throws IndexNotFoundKernelException;

    /** Returns the failure description of a failed index. */
    String indexGetFailure( IndexDescriptor descriptor ) throws IndexNotFoundKernelException;

    /**
     * Get all constraints applicable to label and propertyKey.
     */
    Iterator<ConstraintDescriptor> constraintsGetForSchema( SchemaDescriptor descriptor );

    /**
     * Get all constraints applicable to label.
     */
    Iterator<ConstraintDescriptor> constraintsGetForLabel( int labelId );

    /**
     * Get all constraints applicable to relationship type.
     */
    Iterator<ConstraintDescriptor> constraintsGetForRelationshipType( int typeId );

    /**
     * Get all constraints.
     */
    Iterator<ConstraintDescriptor> constraintsGetAll();

    /**
     * Get the owning constraint for a constraint index. Returns null if the index does not have an owning constraint.
     */
    Long indexGetOwningUniquenessConstraintId( IndexDescriptor index );

    <K, V> V schemaStateGetOrCreate( K key, Function<K, V> creator );

    <K, V> V schemaStateGet( K key );

    void schemaStateFlush();

    //===========================================
    //== LOCKING OPERATIONS =====================
    //===========================================

    void acquireExclusive( ResourceType type, long... ids );

    void acquireShared( ResourceType type, long... ids );

    void releaseExclusive( ResourceType type, long... ids );

    void releaseShared( ResourceType type, long... ids );

    //===========================================
    //== EXPLICIT INDEX OPERATIONS ================
    //===========================================

    /**
     * @param indexName name of node index to check for existence.
     * @param customConfiguration if {@code null} the configuration of existing won't be matched, otherwise it will
     * be matched and a mismatch will throw {@link IllegalArgumentException}.
     * @return whether or not node explicit index with name {@code indexName} exists.
     * @throws IllegalArgumentException on index existence with provided mismatching {@code customConfiguration}.
     */
    boolean nodeExplicitIndexExists( String indexName, Map<String,String> customConfiguration );

    /**
     * @param indexName name of relationship index to check for existence.
     * @param customConfiguration if {@code null} the configuration of existing won't be matched, otherwise it will
     * be matched and a mismatch will throw {@link IllegalArgumentException}.
     * @return whether or not relationship explicit index with name {@code indexName} exists.
     * @throws IllegalArgumentException on index existence with provided mismatching {@code customConfiguration}.
     */
    boolean relationshipExplicitIndexExists( String indexName, Map<String,String> customConfiguration );

    Map<String, String> nodeExplicitIndexGetConfiguration( String indexName )
            throws ExplicitIndexNotFoundKernelException;

    Map<String, String> relationshipExplicitIndexGetConfiguration( String indexName )
            throws ExplicitIndexNotFoundKernelException;

    ExplicitIndexHits nodeExplicitIndexGet( String indexName, String key, Object value )
            throws ExplicitIndexNotFoundKernelException;

    ExplicitIndexHits nodeExplicitIndexQuery( String indexName, String key, Object queryOrQueryObject )
            throws ExplicitIndexNotFoundKernelException;

    ExplicitIndexHits nodeExplicitIndexQuery( String indexName, Object queryOrQueryObject )
            throws ExplicitIndexNotFoundKernelException;

    /**
     * @param startNode -1 if ignored.
     * @param endNode -1 if ignored.
     */
    ExplicitIndexHits relationshipExplicitIndexGet( String name, String key, Object valueOrNull, long startNode,
            long endNode ) throws ExplicitIndexNotFoundKernelException;

    /**
     * @param startNode -1 if ignored.
     * @param endNode -1 if ignored.
     */
    ExplicitIndexHits relationshipExplicitIndexQuery( String indexName, String key, Object queryOrQueryObject,
            long startNode, long endNode )
            throws ExplicitIndexNotFoundKernelException;

    /**
     * @param startNode -1 if ignored.
     * @param endNode -1 if ignored.
     */
    ExplicitIndexHits relationshipExplicitIndexQuery( String indexName, Object queryOrQueryObject,
            long startNode, long endNode )
            throws ExplicitIndexNotFoundKernelException;

    String[] nodeExplicitIndexesGetAll();

    String[] relationshipExplicitIndexesGetAll();

    //===========================================
    //== COUNTS OPERATIONS ======================
    //===========================================

    /**
     * The number of nodes in the graph, including anything changed in the transaction state.
     *
     * If the label parameter is {@link #ANY_LABEL}, this method returns the total number of nodes in the graph, i.e.
     * {@code MATCH (n) RETURN count(n)}.
     *
     * If the label parameter is set to any other value, this method returns the number of nodes that has that label,
     * i.e. {@code MATCH (n:LBL) RETURN count(n)}.
     *
     * @param labelId the label to get the count for, or {@link #ANY_LABEL} to get the total number of nodes.
     * @return the number of matching nodes in the graph.
     */
    long countsForNode( int labelId );

    /**
     * The number of nodes in the graph, without taking into account anything in the transaction state.
     *
     * If the label parameter is {@link #ANY_LABEL}, this method returns the total number of nodes in the graph, i.e.
     * {@code MATCH (n) RETURN count(n)}.
     *
     * If the label parameter is set to any other value, this method returns the number of nodes that has that label,
     * i.e. {@code MATCH (n:LBL) RETURN count(n)}.
     *
     * @param labelId the label to get the count for, or {@link #ANY_LABEL} to get the total number of nodes.
     * @return the number of matching nodes in the graph.
     */
    long countsForNodeWithoutTxState( int labelId );

    /**
     * The number of relationships in the graph, including anything changed in the transaction state.
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
     * <td>{@link #ANY_LABEL}</td>      <td>{@link #ANY_RELATIONSHIP_TYPE}</td>  <td>{@link #ANY_LABEL}</td>
     * <td>{@code MATCH}</td>    <td>{@code ()-[r]->()}</td>            <td>{@code RETURN count(r)}</td>
     * </tr>
     * <tr>
     * <td>{@link #ANY_LABEL}</td>      <td>{@code REL}</td>                     <td>{@link #ANY_LABEL}</td>
     * <td>{@code MATCH}</td>    <td>{@code ()-[r:REL]->()}</td>        <td>{@code RETURN count(r)}</td>
     * </tr>
     * <tr>
     * <td>{@code LHS}</td>             <td>{@link #ANY_RELATIONSHIP_TYPE}</td>  <td>{@link #ANY_LABEL}</td>
     * <td>{@code MATCH}</td>    <td>{@code (:LHS)-[r]->()}</td>        <td>{@code RETURN count(r)}</td>
     * </tr>
     * <tr>
     * <td>{@link #ANY_LABEL}</td>      <td>{@link #ANY_RELATIONSHIP_TYPE}</td>  <td>{@code RHS}</td>
     * <td>{@code MATCH}</td>    <td>{@code ()-[r]->(:RHS)}</td>        <td>{@code RETURN count(r)}</td>
     * </tr>
     * <tr>
     * <td>{@code LHS}</td>             <td>{@code REL}</td>                     <td>{@link #ANY_LABEL}</td>
     * <td>{@code MATCH}</td>    <td>{@code (:LHS)-[r:REL]->()}</td>    <td>{@code RETURN count(r)}</td>
     * </tr>
     * <tr>
     * <td>{@link #ANY_LABEL}</td>      <td>{@code REL}</td>                     <td>{@code RHS}</td>
     * <td>{@code MATCH}</td>    <td>{@code ()-[r:REL]->(:RHS)}</td>    <td>{@code RETURN count(r)}</td>
     * </tr>
     * </tdata>
     * </table>
     *
     * @param startLabelId the label of the start node of relationships to get the count for, or {@link #ANY_LABEL}.
     * @param typeId       the type of relationships to get a count for, or {@link #ANY_RELATIONSHIP_TYPE}.
     * @param endLabelId   the label of the end node of relationships to get the count for, or {@link #ANY_LABEL}.
     * @return the number of matching relationships in the graph.
     */
    long countsForRelationship( int startLabelId, int typeId, int endLabelId );

    /**
     * The number of relationships in the graph, without taking into account anything in the transaction state.
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
     * <td>{@link #ANY_LABEL}</td>      <td>{@link #ANY_RELATIONSHIP_TYPE}</td>  <td>{@link #ANY_LABEL}</td>
     * <td>{@code MATCH}</td>    <td>{@code ()-[r]->()}</td>            <td>{@code RETURN count(r)}</td>
     * </tr>
     * <tr>
     * <td>{@link #ANY_LABEL}</td>      <td>{@code REL}</td>                     <td>{@link #ANY_LABEL}</td>
     * <td>{@code MATCH}</td>    <td>{@code ()-[r:REL]->()}</td>        <td>{@code RETURN count(r)}</td>
     * </tr>
     * <tr>
     * <td>{@code LHS}</td>             <td>{@link #ANY_RELATIONSHIP_TYPE}</td>  <td>{@link #ANY_LABEL}</td>
     * <td>{@code MATCH}</td>    <td>{@code (:LHS)-[r]->()}</td>        <td>{@code RETURN count(r)}</td>
     * </tr>
     * <tr>
     * <td>{@link #ANY_LABEL}</td>      <td>{@link #ANY_RELATIONSHIP_TYPE}</td>  <td>{@code RHS}</td>
     * <td>{@code MATCH}</td>    <td>{@code ()-[r]->(:RHS)}</td>        <td>{@code RETURN count(r)}</td>
     * </tr>
     * <tr>
     * <td>{@code LHS}</td>             <td>{@code REL}</td>                     <td>{@link #ANY_LABEL}</td>
     * <td>{@code MATCH}</td>    <td>{@code (:LHS)-[r:REL]->()}</td>    <td>{@code RETURN count(r)}</td>
     * </tr>
     * <tr>
     * <td>{@link #ANY_LABEL}</td>      <td>{@code REL}</td>                     <td>{@code RHS}</td>
     * <td>{@code MATCH}</td>    <td>{@code ()-[r:REL]->(:RHS)}</td>    <td>{@code RETURN count(r)}</td>
     * </tr>
     * </tdata>
     * </table>
     *
     * @param startLabelId the label of the start node of relationships to get the count for, or {@link #ANY_LABEL}.
     * @param typeId       the type of relationships to get a count for, or {@link #ANY_RELATIONSHIP_TYPE}.
     * @param endLabelId   the label of the end node of relationships to get the count for, or {@link #ANY_LABEL}.
     * @return the number of matching relationships in the graph.
     */
    long countsForRelationshipWithoutTxState( int startLabelId, int typeId, int endLabelId );

    DoubleLongRegister indexUpdatesAndSize( IndexDescriptor index, DoubleLongRegister target )
            throws IndexNotFoundKernelException;

    DoubleLongRegister indexSample( IndexDescriptor index, DoubleLongRegister target )
            throws IndexNotFoundKernelException;

    //===========================================
    //== PROCEDURE OPERATIONS ===================
    //===========================================

    /** Fetch a procedure given its signature. */
    ProcedureSignature procedureGet( QualifiedName name ) throws ProcedureException;

    /** Fetch a function given its signature, or <code>empty</code> if no such function exists*/
    Optional<UserFunctionSignature> functionGet( QualifiedName name );

    /** Fetch an aggregation function given its signature, or <code>empty</code> if no such function exists*/
    Optional<UserFunctionSignature> aggregationFunctionGet( QualifiedName name );

    /** Fetch all registered procedures */
    Set<UserFunctionSignature> functionsGetAll();

    /** Fetch all registered procedures */
    Set<ProcedureSignature> proceduresGetAll();
}
