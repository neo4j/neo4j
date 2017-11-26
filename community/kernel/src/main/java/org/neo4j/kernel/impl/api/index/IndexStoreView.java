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
package org.neo4j.kernel.impl.api.index;

import java.util.function.IntPredicate;

import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.storageengine.api.schema.PopulationProgress;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

/** The indexing services view of the universe. */
public interface IndexStoreView extends PropertyAccessor, PropertyLoader
{
    /**
     * Retrieve all nodes in the database which has got one or more of the given labels AND
     * one or more of the given property key ids. This scan additionally accepts a visitor
     * for label updates for a joint scan.
     *
     * @param labelIds array of label ids to generate updates for. Empty array means all.
     * @param propertyKeyIdFilter property key ids to generate updates for.
     * @param propertyUpdateVisitor visitor which will see all generated {@link NodeUpdates}.
     * @param labelUpdateVisitor visitor which will see all generated {@link NodeLabelUpdate}.
     * @param forceStoreScan overrides decision about which source to scan from. If {@code true}
     * then store scan will be used, otherwise if {@code false} then the best suited will be used.
     * @return a {@link StoreScan} to start and to stop the scan.
     */
    <FAILURE extends Exception> StoreScan<FAILURE> visitNodes(
            int[] labelIds, IntPredicate propertyKeyIdFilter,
            Visitor<NodeUpdates, FAILURE> propertyUpdateVisitor,
            Visitor<NodeLabelUpdate, FAILURE> labelUpdateVisitor,
            boolean forceStoreScan );

    /**
     * Produces {@link NodeUpdates} objects from reading node {@code nodeId}, its labels and properties
     * and puts those updates into node updates container.
     *
     * @param nodeId id of node to load.
     * @return node updates container
     */
    NodeUpdates nodeAsUpdates( long nodeId );

    DoubleLongRegister indexUpdatesAndSize( long indexId, DoubleLongRegister output );

    DoubleLongRegister indexSample( long indexId, DoubleLongRegister output );

    void replaceIndexCounts( long indexId, long uniqueElements, long maxUniqueElements, long indexSize );

    void incrementIndexUpdates( long indexId, long updatesDelta );

    @SuppressWarnings( "rawtypes" )
    StoreScan EMPTY_SCAN = new StoreScan()
    {
        @Override
        public void run() throws Exception
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

    IndexStoreView EMPTY = new IndexStoreView()
    {
        @Override
        public void loadProperties( long nodeId, PrimitiveIntSet propertyIds, PropertyLoadSink sink )
        {
        }

        @Override
        public Value getPropertyValue( long nodeId, int propertyKeyId ) throws EntityNotFoundException
        {
            return Values.NO_VALUE;
        }

        @SuppressWarnings( "unchecked" )
        @Override
        public <FAILURE extends Exception> StoreScan<FAILURE> visitNodes( int[] labelIds,
                IntPredicate propertyKeyIdFilter, Visitor<NodeUpdates,FAILURE> propertyUpdateVisitor,
                Visitor<NodeLabelUpdate,FAILURE> labelUpdateVisitor, boolean forceStoreScan )
        {
            return EMPTY_SCAN;
        }

        @Override
        public void replaceIndexCounts( long indexId, long uniqueElements, long maxUniqueElements,
                long indexSize )
        {
        }

        @Override
        public NodeUpdates nodeAsUpdates( long nodeId )
        {
            return null;
        }

        @Override
        public DoubleLongRegister indexUpdatesAndSize( long indexId, DoubleLongRegister output )
        {
            return output;
        }

        @Override
        public DoubleLongRegister indexSample( long indexId, DoubleLongRegister output )
        {
            return output;
        }

        @Override
        public void incrementIndexUpdates( long indexId, long updatesDelta )
        {
        }
    };
}
