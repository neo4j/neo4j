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
package org.neo4j.internal.kernel.api;

/**
 * Defines the read operations of the Kernel API.
 */
public interface Read
{
    /**
     * TODO: this method needs a better definition.
     *
     * @param predicates
     *         predicates describing what to look for in the index.
     * @param cursor
     *         the cursor to use for consuming the results.
     */
    void nodeIndexSeek( IndexReference index, NodeValueIndexCursor cursor, IndexPredicate... predicates );

    void nodeIndexScan( IndexReference index, NodeValueIndexCursor cursor );

    void nodeLabelScan( int label, NodeLabelIndexCursor cursor );

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
     * @param reference
     *         a reference from {@link NodeCursor#propertiesReference()}.
     * @param cursor
     *         the cursor to use for consuming the results.
     */
    void nodeProperties( long reference, PropertyCursor cursor );

    /**
     * @param reference
     *         a reference from {@link RelationshipDataAccessor#propertiesReference()}.
     * @param cursor
     *         the cursor to use for consuming the results.
     */
    void relationshipProperties( long reference, PropertyCursor cursor );

    // hints to the page cache about data we will be accessing in the future:

    void futureNodeReferenceRead( long reference );

    void futureRelationshipsReferenceRead( long reference );

    void futureNodePropertyReferenceRead( long reference );

    void futureRelationshipPropertyReferenceRead( long reference );
}
