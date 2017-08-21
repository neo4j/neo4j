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
package org.neo4j.impl.kernel.api;

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
     * @param reference
     *         a reference from {@link NodeCursor#nodeReference()}, {@link EdgeDataAccessor#sourceNodeReference()},
     *         {@link EdgeDataAccessor#targetNodeReference()}, {@link NodeIndexCursor#nodeReference()},
     *         {@link EdgeIndexCursor#sourceNodeReference()}, or {@link EdgeIndexCursor#targetNodeReference()}.
     * @param cursor
     *         the cursor to use for consuming the results.
     */
    void singleNode( long reference, NodeCursor cursor );

    /**
     * @param reference
     *         a reference from {@link EdgeDataAccessor#edgeReference()}.
     * @param cursor
     *         the cursor to use for consuming the results.
     */
    void singleEdge( long reference, EdgeScanCursor cursor );

    void allEdgesScan( EdgeScanCursor cursor );

    Scan<EdgeScanCursor> allEdgesScan();

    void edgeLabelScan( int label, EdgeScanCursor cursor );

    Scan<EdgeScanCursor> edgeLabelScan( int label );

    /**
     * @param nodeReference
     *         a reference from {@link NodeCursor#nodeReference()}.
     * @param reference
     *         a reference from {@link NodeCursor#edgeGroupReference()}.
     * @param cursor
     *         the cursor to use for consuming the results.
     */
    void edgeGroups( long nodeReference, long reference, EdgeGroupCursor cursor );

    /**
     * @param nodeReference
     *         a reference from {@link NodeCursor#nodeReference()}.
     * @param reference
     *         a reference from {@link EdgeGroupCursor#outgoingReference()},
     *         {@link EdgeGroupCursor#incomingReference()},
     *         or {@link EdgeGroupCursor#loopsReference()}.
     * @param cursor
     *         the cursor to use for consuming the results.
     */
    void edges( long nodeReference, long reference, EdgeTraversalCursor cursor );

    /**
     * @param reference
     *         a reference from {@link NodeCursor#propertiesReference()}.
     * @param cursor
     *         the cursor to use for consuming the results.
     */
    void nodeProperties( long reference, PropertyCursor cursor );

    /**
     * @param reference
     *         a reference from {@link EdgeDataAccessor#propertiesReference()}.
     * @param cursor
     *         the cursor to use for consuming the results.
     */
    void edgeProperties( long reference, PropertyCursor cursor );

    // hints to the page cache about data we will be accessing in the future:

    void futureNodeReferenceRead( long reference );

    void futureEdgeReferenceRead( long reference );

    void futureNodePropertyReferenceRead( long reference );

    void futureEdgePropertyReferenceRead( long reference );
}
