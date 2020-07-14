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

import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.storageengine.api.ReadTracer;

/**
 * Tracer of kernel API reads. The Kernel will callback the tracer methods on various key events
 * of kernel reads, allowing a tracer to analyze the read patterns.
 *
 * Note: In the initial version of this tracer we only added the callbacks that were
 *       necessary to support cypher PROFILE dbHits, and some extra which makes testing
 *       easier. If we adopt this mechanism for other uses, it might be a good idea to
 *       add additional callback, extend some callbacks with more details, or
 *       differentiate callbacks (e.g. onNode) depending on the underlying read.
 */
public interface KernelReadTracer extends ReadTracer
{
    /**
     * Called just before {@link NodeCursor#next()} returns true.
     *
     * @param nodeReference the node reference that will be available.
     */
    @Override
    void onNode( long nodeReference );

    /**
     * Called on {@link Read#allNodesScan(NodeCursor)}.
     */
    @Override
    void onAllNodesScan();

    /**
     * Called on {@link Read#nodeLabelScan(int, NodeLabelIndexCursor, IndexOrder)}.
     */
    void onLabelScan( int label );

    /**
     * Called on {@link Read#relationshipTypeScan(int, RelationshipScanCursor)}
     */
    void onRelationshipTypeScan( int type );

    /**
     * Called on {@link Read#nodeIndexSeek(IndexReadSession, NodeValueIndexCursor, IndexQueryConstraints, IndexQuery...)}.
     */
    void onIndexSeek();

    /**
     * Called just before {@link RelationshipScanCursor#next()} returns true.
     *
     * @param relationshipReference the relationship reference that will be available.
     */
    @Override
    void onRelationship( long relationshipReference );

    /**
     * Called just before {@link PropertyCursor#next()} returns true.
     *
     * @param propertyKey the property key of the next property.
     */
    @Override
    void onProperty( int propertyKey );
}
