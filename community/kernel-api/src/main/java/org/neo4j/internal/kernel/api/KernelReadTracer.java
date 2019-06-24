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

import org.neo4j.util.Preconditions;

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
public interface KernelReadTracer
{
    /**
     * Called just before {@link NodeCursor#next()} returns true.
     *
     * @param nodeReference the node reference that will be available.
     */
    void onNode( long nodeReference );

    /**
     * Called on {@link Read#allNodesScan(NodeCursor)}.
     */
    void onAllNodesScan();

    /**
     * Called on {@link Read#nodeLabelScan(int, NodeLabelIndexCursor)}.
     */
    void onLabelScan( int label );

    /**
     * Called on {@link Read#nodeIndexSeek(IndexReadSession, NodeValueIndexCursor, IndexOrder, boolean, IndexQuery...)}.
     */
    void onIndexSeek();

    /**
     * Called just before {@link RelationshipScanCursor#next()} returns true.
     *
     * @param relationshipReference the relationship reference that will be available.
     */
    void onRelationship( long relationshipReference );

    /**
     * Called just before {@link RelationshipGroupCursor#next()} returns true.
     *
     * @param type the relationship type that will be available.
     */
    void onRelationshipGroup( int type );

    /**
     * Called just before {@link PropertyCursor#next()} returns true.
     *
     * @param propertyKey the property key of the next property.
     */
    void onProperty( int propertyKey );

    KernelReadTracer NONE = new KernelReadTracer()
    {
        @Override
        public void onNode( long nodeReference )
        {
        }

        @Override
        public void onAllNodesScan()
        {
        }

        @Override
        public void onLabelScan( int label )
        {
        }

        @Override
        public void onIndexSeek()
        {
        }

        @Override
        public void onRelationship( long relationshipReference )
        {
        }

        @Override
        public void onRelationshipGroup( int type )
        {
        }

        @Override
        public void onProperty( int propertyKey )
        {
        }
    };

}
