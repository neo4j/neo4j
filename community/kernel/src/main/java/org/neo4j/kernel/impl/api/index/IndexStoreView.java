/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.eclipse.collections.api.set.primitive.MutableIntSet;

import java.util.function.IntPredicate;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.NodePropertyAccessor;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.storageengine.api.schema.PopulationProgress;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

/** The indexing services view of the universe. */
public interface IndexStoreView extends NodePropertyAccessor, PropertyLoader
{
    /**
     * Retrieve all nodes in the database which has got one or more of the given labels AND
     * one or more of the given property key ids. This scan additionally accepts a visitor
     * for label updates for a joint scan.
     *
     * @param labelIds array of label ids to generate updates for. Empty array means all.
     * @param propertyKeyIdFilter property key ids to generate updates for.
     * @param propertyUpdateVisitor visitor which will see all generated {@link EntityUpdates}.
     * @param labelUpdateVisitor visitor which will see all generated {@link NodeLabelUpdate}.
     * @param forceStoreScan overrides decision about which source to scan from. If {@code true}
     * then store scan will be used, otherwise if {@code false} then the best suited will be used.
     * @return a {@link StoreScan} to start and to stop the scan.
     */
    <FAILURE extends Exception> StoreScan<FAILURE> visitNodes(
            int[] labelIds, IntPredicate propertyKeyIdFilter,
            Visitor<EntityUpdates, FAILURE> propertyUpdateVisitor,
            Visitor<NodeLabelUpdate, FAILURE> labelUpdateVisitor,
            boolean forceStoreScan );

    /**
     * Retrieve all relationships in the database which has any of the the given relationship types AND
     * one or more of the given property key ids.
     *
     * @param relationshipTypeIds array of relationsip type ids to generate updates for. Empty array means all.
     * @param propertyKeyIdFilter property key ids to generate updates for.
     * @param propertyUpdateVisitor visitor which will see all generated {@link EntityUpdates}
     * @return a {@link StoreScan} to start and to stop the scan.
     */
    <FAILURE extends Exception> StoreScan<FAILURE> visitRelationships( int[] relationshipTypeIds, IntPredicate propertyKeyIdFilter,
            Visitor<EntityUpdates,FAILURE> propertyUpdateVisitor );

    /**
     * Produces {@link EntityUpdates} objects from reading node {@code entityId}, its labels and properties
     * and puts those updates into node updates container.
     *
     * @param entityId id of entity to load.
     * @return node updates container
     */
    @VisibleForTesting
    EntityUpdates nodeAsUpdates( long entityId );

    DoubleLongRegister indexUpdatesAndSize( long indexId, DoubleLongRegister output );

    DoubleLongRegister indexSample( long indexId, DoubleLongRegister output );

    void replaceIndexCounts( long indexId, long uniqueElements, long maxUniqueElements, long indexSize );

    void incrementIndexUpdates( long indexId, long updatesDelta );

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

    class Adaptor implements IndexStoreView
    {
        @Override
        public void loadProperties( long nodeId, EntityType type, MutableIntSet propertyIds, PropertyLoadSink sink )
        {
        }

        @Override
        public Value getNodePropertyValue( long nodeId, int propertyKeyId )
        {
            return Values.NO_VALUE;
        }

        @SuppressWarnings( "unchecked" )
        @Override
        public <FAILURE extends Exception> StoreScan<FAILURE> visitNodes( int[] labelIds,
                IntPredicate propertyKeyIdFilter, Visitor<EntityUpdates,FAILURE> propertyUpdateVisitor,
                Visitor<NodeLabelUpdate,FAILURE> labelUpdateVisitor, boolean forceStoreScan )
        {
            return EMPTY_SCAN;
        }

        @SuppressWarnings( "unchecked" )
        @Override
        public <FAILURE extends Exception> StoreScan<FAILURE> visitRelationships( int[] relationshipTypeIds, IntPredicate propertyKeyIdFilter,
                Visitor<EntityUpdates,FAILURE> propertyUpdateVisitor )
        {
            return EMPTY_SCAN;
        }

        @Override
        public void replaceIndexCounts( long indexId, long uniqueElements, long maxUniqueElements,
                long indexSize )
        {
        }

        @Override
        public EntityUpdates nodeAsUpdates( long nodeId )
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
    }
}
