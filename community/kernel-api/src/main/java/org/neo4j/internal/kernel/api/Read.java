/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;

/**
 * Defines the graph read operations of the Kernel.
 */
public interface Read
{
    int ANY_LABEL = -1;
    int ANY_RELATIONSHIP_TYPE = -1;

    /**
     * Seek all nodes matching the provided index query in an index.
     *
     * @param index {@link IndexReference} referencing index to query.
     * @param cursor the cursor to use for consuming the results.
     * @param indexOrder requested {@link IndexOrder} of result. Must be among the capabilities of
     * {@link IndexReference referenced index}, or {@link IndexOrder#NONE}.
     * @param query Combination of {@link IndexQuery index queries} to run against referenced index.
     */
    void nodeIndexSeek( IndexReference index, NodeValueIndexCursor cursor, IndexOrder indexOrder, IndexQuery... query )
            throws KernelException;

    /**
     * Access all distinct counts in an index. Entries fed to the {@code cursor} will be (count,Value[]),
     * where the count (number of nodes having the particular value) will be accessed using {@link NodeValueIndexCursor#nodeReference()}
     * and the value (if the index can provide it) using {@link NodeValueIndexCursor#propertyValue(int)}.
     * Before accessing a property value the caller should check {@link NodeValueIndexCursor#hasValue()} to see
     * whether or not the index could yield values.
     *
     * For merely counting distinct values in an index, loop over and sum iterations.
     * For counting number of indexed nodes in an index, loop over and sum all counts.
     *
     * NOTE distinct values may not be 100% accurate for point values that are very close to each other. In those cases they can be
     * reported as a single distinct values with a higher count instead of several separate values.
     *
     * @param index {@link IndexReference} referencing index.
     * @param cursor {@link NodeValueIndexCursor} receiving distinct count data.
     */
    void nodeIndexDistinctValues( IndexReference index, NodeValueIndexCursor cursor ) throws IndexNotFoundKernelException;

    /**
     * Returns node id of node found in unique index or -1 if no node was found.
     *
     * Note that this is a very special method and should be use with caution. It has special locking semantics in
     * order to facilitate unique creation of nodes. If a node is found; a shared lock for the index entry will be
     * held whereas if no node is found we will hold onto an exclusive lock until the close of the transaction.
     *
     * @param index {@link IndexReference} referencing index to query.
     *              {@link IndexReference referenced index}, or {@link IndexOrder#NONE}.
     * @param predicates Combination of {@link IndexQuery.ExactPredicate index queries} to run against referenced index.
     */
    long lockingNodeUniqueIndexSeek( IndexReference index, IndexQuery.ExactPredicate... predicates )
            throws KernelException;

    /**
     * Scan all values in an index.
     *
     * @param index {@link IndexReference} referencing index to query.
     * @param cursor the cursor to use for consuming the results.
     * @param indexOrder requested {@link IndexOrder} of result. Must be among the capabilities of
     * {@link IndexReference referenced index}, or {@link IndexOrder#NONE}.
     */
    void nodeIndexScan( IndexReference index, NodeValueIndexCursor cursor, IndexOrder indexOrder ) throws KernelException;

    void nodeLabelScan( int label, NodeLabelIndexCursor cursor );

    /**
     * Scan for nodes that have a <i>disjunction</i> of the specified labels.
     * i.e. MATCH (n) WHERE n:Label1 OR n:Label2 OR ...
     */
    void nodeLabelUnionScan( NodeLabelIndexCursor cursor, int... labels );

    /**
     * Scan for nodes that have a <i>conjunction</i> of the specified labels.
     * i.e. MATCH (n) WHERE n:Label1 AND n:Label2 AND ...
     */
    void nodeLabelIntersectionScan( NodeLabelIndexCursor cursor, int... labels );

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

    Scan<RelationshipScanCursor> relationshipTypeScan( int type );

    /**
     * @param nodeReference
     *         a reference from {@link NodeCursor#nodeReference()}.
     * @param reference
     *         a reference from {@link NodeCursor#relationshipGroupReference()}.
     * @param cursor
     *         the cursor to use for consuming the results.
     */
    void relationshipGroups( long nodeReference, long reference, RelationshipGroupCursor cursor );

    /**
     * @param nodeReference
     *         a reference from {@link NodeCursor#nodeReference()}.
     * @param reference
     *         a reference from {@link RelationshipGroupCursor#outgoingReference()},
     *         {@link RelationshipGroupCursor#incomingReference()},
     *         or {@link RelationshipGroupCursor#loopsReference()}.
     * @param cursor
     *         the cursor to use for consuming the results.
     */
    void relationships( long nodeReference, long reference, RelationshipTraversalCursor cursor );

    /**
     * @param nodeReference
     *         the owner of the properties.
     * @param reference
     *         a reference from {@link NodeCursor#propertiesReference()}.
     * @param cursor
     *         the cursor to use for consuming the results.
     */
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

    void graphProperties( PropertyCursor cursor );

    // hints to the page cache about data we will be accessing in the future:

    void futureNodeReferenceRead( long reference );

    void futureRelationshipsReferenceRead( long reference );

    void futureNodePropertyReferenceRead( long reference );

    void futureRelationshipPropertyReferenceRead( long reference );
}
