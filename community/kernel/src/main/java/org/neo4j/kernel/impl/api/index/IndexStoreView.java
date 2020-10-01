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
package org.neo4j.kernel.impl.api.index;

import java.util.function.IntPredicate;

import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.EntityTokenUpdate;
import org.neo4j.storageengine.api.EntityUpdates;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.NodePropertyAccessor;

/** The indexing services view of the universe. */
public interface IndexStoreView
{
    /**
     * Retrieve all nodes in the database which has got one or more of the given labels AND
     * one or more of the given property key ids. This scan additionally accepts a visitor
     * for label updates for a joint scan.
     *
     * @param labelIds array of label ids to generate updates for. Empty array means all.
     * @param propertyKeyIdFilter property key ids to generate updates for.
     * @param propertyUpdateVisitor visitor which will see all generated {@link EntityUpdates}.
     * @param labelUpdateVisitor visitor which will see all generated {@link EntityTokenUpdate}.
     * @param forceStoreScan overrides decision about which source to scan from. If {@code true}
     * then store scan will be used, otherwise if {@code false} then the best suited will be used.
     * @param cursorTracer underlying page cursor events tracer.
     * @return a {@link StoreScan} to start and to stop the scan.
     */
    <FAILURE extends Exception> StoreScan<FAILURE> visitNodes(
            int[] labelIds, IntPredicate propertyKeyIdFilter,
            Visitor<EntityUpdates, FAILURE> propertyUpdateVisitor,
            Visitor<EntityTokenUpdate, FAILURE> labelUpdateVisitor,
            boolean forceStoreScan, PageCursorTracer cursorTracer, MemoryTracker memoryTracker );

    /**
     * Retrieve all relationships in the database which has any of the the given relationship types AND
     * one or more of the given property key ids.
     *
     * @param relationshipTypeIds array of relationship type ids to generate updates for. Empty array means all.
     * @param propertyKeyIdFilter property key ids to generate updates for.
     * @param propertyUpdateVisitor visitor which will see all generated {@link EntityUpdates}
     * @param relationshipTypeUpdateVisitor visitor which will see all generated {@link EntityTokenUpdate}.
     * @param forceStoreScan overrides decision about which source to scan from. If {@code true}
     * then store scan will be used, otherwise if {@code false} then the best suited will be used.
     * @param cursorTracer underlying page cursor events tracer.
     * @return a {@link StoreScan} to start and to stop the scan.
     */
    <FAILURE extends Exception> StoreScan<FAILURE> visitRelationships( int[] relationshipTypeIds, IntPredicate propertyKeyIdFilter,
            Visitor<EntityUpdates,FAILURE> propertyUpdateVisitor, Visitor<EntityTokenUpdate,FAILURE> relationshipTypeUpdateVisitor,
            boolean forceStoreScan, PageCursorTracer cursorTracer, MemoryTracker memoryTracker );

    NodePropertyAccessor newPropertyAccessor( PageCursorTracer cursorTracer, MemoryTracker memoryTracker );

    @SuppressWarnings( "rawtypes" )
    StoreScan EMPTY_SCAN = new StoreScan()
    {
        @Override
        public void run()
        {
        }

        @Override
        public void stop()
        {
        }

        @Override
        public void acceptUpdate( MultipleIndexPopulator.MultipleIndexUpdater updater, IndexEntryUpdate update,
                long currentlyIndexedNodeId )
        {
        }

        @Override
        public PopulationProgress getProgress()
        {
            return PopulationProgress.DONE;
        }
    };

    IndexStoreView EMPTY = new Adaptor();

    boolean isEmpty();

    class Adaptor implements IndexStoreView
    {
        @SuppressWarnings( "unchecked" )
        @Override
        public <FAILURE extends Exception> StoreScan<FAILURE> visitNodes( int[] labelIds,
                IntPredicate propertyKeyIdFilter, Visitor<EntityUpdates,FAILURE> propertyUpdateVisitor,
                Visitor<EntityTokenUpdate,FAILURE> labelUpdateVisitor, boolean forceStoreScan, PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
        {
            return EMPTY_SCAN;
        }

        @SuppressWarnings( "unchecked" )
        @Override
        public <FAILURE extends Exception> StoreScan<FAILURE> visitRelationships( int[] relationshipTypeIds, IntPredicate propertyKeyIdFilter,
                Visitor<EntityUpdates,FAILURE> propertyUpdateVisitor, Visitor<EntityTokenUpdate,FAILURE> relationshipTypeUpdateVisitor,
                boolean forceStoreScan, PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
        {
            return EMPTY_SCAN;
        }

        @Override
        public NodePropertyAccessor newPropertyAccessor( PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
        {
            return NodePropertyAccessor.EMPTY;
        }

        @Override
        public boolean isEmpty()
        {
            return true;
        }
    }
}
