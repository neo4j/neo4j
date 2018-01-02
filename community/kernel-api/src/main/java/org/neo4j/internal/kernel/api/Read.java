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
package org.neo4j.internal.kernel.api;

import org.neo4j.internal.kernel.api.exceptions.KernelException;

/**
 * Defines the graph read operations of the Kernel.
 */
public interface Read
{
    /**
     * @param index {@link IndexReference} referencing index to query.
     * @param cursor the cursor to use for consuming the results.
     * @param indexOrder requested {@link IndexOrder} of result. Must be among the capabilities of
     * {@link IndexReference referenced index}, or {@link IndexOrder#NONE}.
     * @param query Combination of {@link IndexQuery index queries} to run against referenced index.
     */
    void nodeIndexSeek( IndexReference index, NodeValueIndexCursor cursor, IndexOrder indexOrder, IndexQuery... query )
            throws KernelException;

    /**
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
     * @param id The id of the node to check
     * @return <tt>true</tt> if the node exists, otherwise <tt>false</tt>
     */
    boolean nodeExists( long id );

    /**
     * @param reference
     *         a reference from {@link RelationshipDataAccessor#relationshipReference()}.
     * @param cursor
     *         the cursor to use for consuming the results.
     */
    void singleRelationship( long reference, RelationshipScanCursor cursor );

    void allRelationshipsScan( RelationshipScanCursor cursor );

    Scan<RelationshipScanCursor> allRelationshipsScan();

    void relationshipLabelScan( int label, RelationshipScanCursor cursor );

    Scan<RelationshipScanCursor> relationshipLabelScan( int label );

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
