/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.kernel.api;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

/**
 * Defines the graph read operations of the Kernel.
 */
public interface Read
{
    long NO_ID = -1;

    /**
     * Ensure there is an IndexReadSession for the given index bound to this transaction, and return it. Not Thread-safe.
     *
     * @param index descriptor for the index to read from
     * @return the IndexReadSession.
     */
    IndexReadSession indexReadSession( IndexDescriptor index ) throws IndexNotFoundKernelException;

    /**
     * Ensure this transaction is prepared for node label scans. This avoids concurrency issues. Not Thread-safe.
     */
    void prepareForLabelScans();

    /**
     * Seek all nodes matching the provided index query in an index.
     * @param index {@link IndexReadSession} referencing index to query. This must be an index of nodes.
     * @param cursor the cursor to use for consuming the results.
     * @param constraints The requested constraints on the query result, such as the {@link IndexOrder}, or whether the index should fetch property values
     * together with node ids for index queries. The constraints must be satisfiable given the capabilities of the index.
     * @param query Combination of {@link IndexQuery index queries} to run against referenced index.
     */
    void nodeIndexSeek( IndexReadSession index, NodeValueIndexCursor cursor, IndexQueryConstraints constraints, IndexQuery... query )
            throws KernelException;

    /**
     * Seek all relationships matching the provided index query in an index.
     * @param index {@link IndexReadSession} referencing index to query. This must be an index of relationships.
     * @param cursor the cursor to use for consuming the results.
     * @param constraints The requested constraints on the query result, such as the {@link IndexOrder}, or whether the index should fetch property values
     * together with relationship ids for index queries. The constraints must be satisfiable given the capabilities of the index.
     * @param query Combination of {@link IndexQuery index queries} to run against referenced index.
     */
    void relationshipIndexSeek( IndexReadSession index, RelationshipValueIndexCursor cursor, IndexQueryConstraints constraints, IndexQuery... query )
            throws KernelException;

    /**
     * Returns node id of node found in the unique index, or -1 if no node was found.
     *
     * Note that this is a very special method and should be use with caution. It has special locking semantics in
     * order to facilitate unique creation of nodes. If a node is found; a shared lock for the index entry will be
     * held whereas if no node is found we will hold onto an exclusive lock until the close of the transaction.
     *
     * Note: This method does not take an IndexReadSession, as it has to acquire a new index session internally to
     * ensure node uniqueness.
     *
     * @param index {@link IndexDescriptor} for the index to query.
     * @param cursor cursor to use for performing the index seek
     * @param predicates Combination of {@link IndexQuery.ExactPredicate index queries} to run against referenced index.
     */
    long lockingNodeUniqueIndexSeek( IndexDescriptor index,
                                     NodeValueIndexCursor cursor,
                                     IndexQuery.ExactPredicate... predicates )
            throws KernelException;

    /**
     * Scan all values in an index.
     *
     * @param index {@link IndexReadSession} index read session to query.
     * @param cursor the cursor to use for consuming the results.
     * @param constraints The requested constraints on the query result, such as the {@link IndexOrder}, or whether the index should fetch property values
     * together with node ids for index queries. The constraints must be satisfiable given the capabilities of the index.
     */
    void nodeIndexScan( IndexReadSession index, NodeValueIndexCursor cursor, IndexQueryConstraints constraints ) throws KernelException;

    /**
     * Scan all values in an index.
     *
     * @param index {@link IndexReadSession} index read session to query.
     * @param cursor the cursor to use for consuming the results.
     * @param constraints The requested constraints on the query result, such as the {@link IndexOrder}, or whether the index should fetch property values
     * together with relationship ids for index queries. The constraints must be satisfiable given the capabilities of the index.
     */
    void relationshipIndexScan( IndexReadSession index, RelationshipValueIndexCursor cursor, IndexQueryConstraints constraints ) throws KernelException;

    /**
     * Scan all nodes with a label.
     *
     * @param label the label
     * @param cursor the cursor to use for consuming the results.
     * @param order the requested order on the query result.
     */
    void nodeLabelScan( int label, NodeLabelIndexCursor cursor, IndexOrder order );

    Scan<NodeLabelIndexCursor> nodeLabelScan( int label );

    /**
     * Return all nodes in the graph.
     *
     * @param cursor Cursor to initialize for scanning.
     */
    void allNodesScan( NodeCursor cursor );

    Scan<NodeCursor> allNodesScan();

    /**
     * @param reference a reference from {@link NodeCursor#nodeReference()}, {@link
     * RelationshipDataAccessor#sourceNodeReference()},
     * {@link RelationshipDataAccessor#targetNodeReference()}, {@link NodeIndexCursor#nodeReference()},
     * {@link RelationshipIndexCursor#sourceNodeReference()}, or {@link RelationshipIndexCursor#targetNodeReference()}.
     * @param cursor the cursor to use for consuming the results.
     */
    void singleNode( long reference, NodeCursor cursor );

    /**
     * Checks if a node exists in the database
     *
     * @param reference The reference of the node to check
     * @return {@code true} if the node exists, otherwise {@code false}
     */
    boolean nodeExists( long reference );

    /**
     * The number of nodes in the graph, including anything changed in the transaction state.
     *
     * If the label parameter is {@link TokenRead#ANY_LABEL}, this method returns the total number of nodes in the graph, i.e.
     * {@code MATCH (n) RETURN count(n)}.
     *
     * If the label parameter is set to any other value, this method returns the number of nodes that has that label,
     * i.e. {@code MATCH (n:LBL) RETURN count(n)}.
     *
     * @param labelId the label to get the count for, or {@link TokenRead#ANY_LABEL} to get the total number of nodes.
     * @return the number of matching nodes in the graph.
     */
    long countsForNode( int labelId );

    /**
     * The number of nodes in the graph, without taking into account anything in the transaction state.
     *
     * If the label parameter is {@link TokenRead#ANY_LABEL}, this method returns the total number of nodes in the graph, i.e.
     * {@code MATCH (n) RETURN count(n)}.
     *
     * If the label parameter is set to any other value, this method returns the number of nodes that has that label,
     * i.e. {@code MATCH (n:LBL) RETURN count(n)}.
     *
     * @param labelId the label to get the count for, or {@link TokenRead#ANY_LABEL} to get the total number of nodes.
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
     * <td>{@link TokenRead#ANY_LABEL}</td>      <td>{@link TokenRead#ANY_RELATIONSHIP_TYPE}</td>  <td>{@link TokenRead#ANY_LABEL}</td>
     * <td>{@code MATCH}</td>    <td>{@code ()-[r]->()}</td>            <td>{@code RETURN count(r)}</td>
     * </tr>
     * <tr>
     * <td>{@link TokenRead#ANY_LABEL}</td>      <td>{@code REL}</td>                     <td>{@link TokenRead#ANY_LABEL}</td>
     * <td>{@code MATCH}</td>    <td>{@code ()-[r:REL]->()}</td>        <td>{@code RETURN count(r)}</td>
     * </tr>
     * <tr>
     * <td>{@code LHS}</td>             <td>{@link TokenRead#ANY_RELATIONSHIP_TYPE}</td>  <td>{@link TokenRead#ANY_LABEL}</td>
     * <td>{@code MATCH}</td>    <td>{@code (:LHS)-[r]->()}</td>        <td>{@code RETURN count(r)}</td>
     * </tr>
     * <tr>
     * <td>{@link TokenRead#ANY_LABEL}</td>      <td>{@link TokenRead#ANY_RELATIONSHIP_TYPE}</td>  <td>{@code RHS}</td>
     * <td>{@code MATCH}</td>    <td>{@code ()-[r]->(:RHS)}</td>        <td>{@code RETURN count(r)}</td>
     * </tr>
     * <tr>
     * <td>{@code LHS}</td>             <td>{@code REL}</td>                     <td>{@link TokenRead#ANY_LABEL}</td>
     * <td>{@code MATCH}</td>    <td>{@code (:LHS)-[r:REL]->()}</td>    <td>{@code RETURN count(r)}</td>
     * </tr>
     * <tr>
     * <td>{@link TokenRead#ANY_LABEL}</td>      <td>{@code REL}</td>                     <td>{@code RHS}</td>
     * <td>{@code MATCH}</td>    <td>{@code ()-[r:REL]->(:RHS)}</td>    <td>{@code RETURN count(r)}</td>
     * </tr>
     * </tdata>
     * </table>
     *
     * @param startLabelId the label of the start node of relationships to get the count for, or {@link TokenRead#ANY_LABEL}.
     * @param typeId       the type of relationships to get a count for, or {@link TokenRead#ANY_RELATIONSHIP_TYPE}.
     * @param endLabelId   the label of the end node of relationships to get the count for, or {@link TokenRead#ANY_LABEL}.
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
     * <td>{@link TokenRead#ANY_LABEL}</td>      <td>{@link TokenRead#ANY_RELATIONSHIP_TYPE}</td>  <td>{@link TokenRead#ANY_LABEL}</td>
     * <td>{@code MATCH}</td>    <td>{@code ()-[r]->()}</td>            <td>{@code RETURN count(r)}</td>
     * </tr>
     * <tr>
     * <td>{@link TokenRead#ANY_LABEL}</td>      <td>{@code REL}</td>                     <td>{@link TokenRead#ANY_LABEL}</td>
     * <td>{@code MATCH}</td>    <td>{@code ()-[r:REL]->()}</td>        <td>{@code RETURN count(r)}</td>
     * </tr>
     * <tr>
     * <td>{@code LHS}</td>             <td>{@link TokenRead#ANY_RELATIONSHIP_TYPE}</td>  <td>{@link TokenRead#ANY_LABEL}</td>
     * <td>{@code MATCH}</td>    <td>{@code (:LHS)-[r]->()}</td>        <td>{@code RETURN count(r)}</td>
     * </tr>
     * <tr>
     * <td>{@link TokenRead#ANY_LABEL}</td>      <td>{@link TokenRead#ANY_RELATIONSHIP_TYPE}</td>  <td>{@code RHS}</td>
     * <td>{@code MATCH}</td>    <td>{@code ()-[r]->(:RHS)}</td>        <td>{@code RETURN count(r)}</td>
     * </tr>
     * <tr>
     * <td>{@code LHS}</td>             <td>{@code REL}</td>                     <td>{@link TokenRead#ANY_LABEL}</td>
     * <td>{@code MATCH}</td>    <td>{@code (:LHS)-[r:REL]->()}</td>    <td>{@code RETURN count(r)}</td>
     * </tr>
     * <tr>
     * <td>{@link TokenRead#ANY_LABEL}</td>      <td>{@code REL}</td>                     <td>{@code RHS}</td>
     * <td>{@code MATCH}</td>    <td>{@code ()-[r:REL]->(:RHS)}</td>    <td>{@code RETURN count(r)}</td>
     * </tr>
     * </tdata>
     * </table>
     *
     * @param startLabelId the label of the start node of relationships to get the count for, or {@link TokenRead#ANY_LABEL}.
     * @param typeId       the type of relationships to get a count for, or {@link TokenRead#ANY_RELATIONSHIP_TYPE}.
     * @param endLabelId   the label of the end node of relationships to get the count for, or {@link TokenRead#ANY_LABEL}.
     * @return the number of matching relationships in the graph.
     */
    long countsForRelationshipWithoutTxState( int startLabelId, int typeId, int endLabelId );

    /**
     * Count of the total number of nodes in the database including changes in the current transaction.
     *
     * @return the total number of nodes in the database
     */
    long nodesGetCount( );

    /**
     * Count of the total number of relationships in the database including changes in the current transaction.
     *
     * @return the total number of relationships in the database
     */
    long relationshipsGetCount( );

    /**
     * @param reference
     *         a reference from {@link RelationshipDataAccessor#relationshipReference()}.
     * @param cursor
     *         the cursor to use for consuming the results.
     */
    void singleRelationship( long reference, RelationshipScanCursor cursor );

    /**
     * Checks if a relationship exists in the database
     *
     * @param reference The reference of the relationship to check
     * @return <tt>true</tt> if the relationship exists, otherwise <tt>false</tt>
     */
    boolean relationshipExists( long reference );

    void allRelationshipsScan( RelationshipScanCursor cursor );

    Scan<RelationshipScanCursor> allRelationshipsScan();

    void relationshipTypeScan( int type, RelationshipScanCursor cursor );

    /**
     * Scan all relationships of a type.
     *
     * @param type the relationship type
     * @param relationshipTypeIndexCursor the cursor to use for consuming the results.
     * @param order the requested order on the query result.
     */
    void relationshipTypeScan( int type, RelationshipTypeIndexCursor relationshipTypeIndexCursor, IndexOrder order );

    /**
     * @param nodeReference
     *         a reference from {@link NodeCursor#nodeReference()}.
     * @param reference
     *         a reference to start of relationships.
     * @param selection
     *         which relationships to select.
     * @param cursor
     *         the cursor to use for consuming the results.
     */
    // Used by APOC.
    void relationships( long nodeReference, long reference, RelationshipSelection selection, RelationshipTraversalCursor cursor );

    /**
     * @param nodeReference
     *         the owner of the properties.
     * @param reference
     *         a reference from {@link NodeCursor#propertiesReference()}.
     * @param cursor
     *         the cursor to use for consuming the results.
     */
    // Used by APOC and GDS.
    void nodeProperties( long nodeReference, long reference, PropertyCursor cursor );

    /**
     * @param relationshipReference
     *         the owner of the properties.
     * @param reference
     *         a reference from {@link RelationshipDataAccessor#propertiesReference()}.
     * @param cursor
     *         the cursor to use for consuming the results.
     */
    void relationshipProperties( long relationshipReference, long reference, PropertyCursor cursor );

    /**
     * Checks if a node was deleted in the current transaction
     * @param node the node to check
     * @return <code>true</code> if the node was deleted otherwise <code>false</code>
     */
    boolean nodeDeletedInTransaction( long node );

    /**
     * Checks if a relationship was deleted in the current transaction
     * @param relationship the relationship to check
     * @return <code>true</code> if the relationship was deleted otherwise <code>false</code>
     */
    boolean relationshipDeletedInTransaction( long relationship );

    /**
     * Returns the value of a node property if set in this transaction.
     * @param node the node
     * @param propertyKeyId the property key id of interest
     * @return <code>null</code> if the property has not been changed for the node in this transaction. Otherwise returns
     *         the new property value, or {@link Values#NO_VALUE} if the property has been removed in this transaction.
     */
    Value nodePropertyChangeInTransactionOrNull( long node, int propertyKeyId );

    /**
     * Returns the value of a relationship property if set in this transaction.
     * @param relationship the relationship
     * @param propertyKeyId the property key id of interest
     * @return <code>null</code> if the property has not been changed for the relationship in this transaction. Otherwise returns
     *         the new property value, or {@link Values#NO_VALUE} if the property has been removed in this transaction.
     */
    Value relationshipPropertyChangeInTransactionOrNull( long relationship, int propertyKeyId );

    /**
     * @return whether there are changes in the transaction state.
     */
    boolean transactionStateHasChanges();
}
