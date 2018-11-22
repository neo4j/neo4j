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
package org.neo4j.kernel.impl.transaction.state.storeview;

import java.util.function.IntPredicate;
import java.util.function.Supplier;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.index.NodePropertyAccessor;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.api.index.EntityUpdates;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageReader;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.storageengine.api.StorageReader;

/**
 * Node store view that will always visit all nodes during store scan.
 */
public class NeoStoreIndexStoreView implements IndexStoreView
{
    protected final PropertyStore propertyStore;
    protected final NodeStore nodeStore;
    protected final RelationshipStore relationshipStore;
    protected final LockService locks;
    private final CountsTracker counts;
    private final Supplier<StorageReader> storageEngine;
    private final NeoStores neoStores;

    public NeoStoreIndexStoreView( LockService locks, NeoStores neoStores, CountsTracker counts, Supplier<StorageReader> storageEngine )
    {
        this.locks = locks;
        this.neoStores = neoStores;
        this.propertyStore = neoStores.getPropertyStore();
        this.nodeStore = neoStores.getNodeStore();
        this.relationshipStore = neoStores.getRelationshipStore();
        this.counts = counts;
        this.storageEngine = storageEngine;
    }

    @Override
    public DoubleLongRegister indexUpdatesAndSize( long indexId, DoubleLongRegister output )
    {
        return counts.indexUpdatesAndSize( indexId, output );
    }

    @Override
    public void replaceIndexCounts( long indexId, long uniqueElements, long maxUniqueElements, long indexSize )
    {
        try ( CountsAccessor.IndexStatsUpdater updater = counts.updateIndexCounts() )
        {
            updater.replaceIndexSample( indexId, uniqueElements, maxUniqueElements );
            updater.replaceIndexUpdateAndSize( indexId, 0L, indexSize );
        }
    }

    @Override
    public void incrementIndexUpdates( long indexId, long updatesDelta )
    {
        try ( CountsAccessor.IndexStatsUpdater updater = counts.updateIndexCounts() )
        {
            updater.incrementIndexUpdates( indexId, updatesDelta );
        }
    }

    @Override
    public DoubleLongRegister indexSample( long indexId, DoubleLongRegister output )
    {
        return counts.indexSample( indexId, output );
    }

    @Override
    public <FAILURE extends Exception> StoreScan<FAILURE> visitNodes(
            final int[] labelIds, IntPredicate propertyKeyIdFilter,
            final Visitor<EntityUpdates, FAILURE> propertyUpdatesVisitor,
            final Visitor<NodeLabelUpdate, FAILURE> labelUpdateVisitor,
            boolean forceStoreScan )
    {
        return new StoreViewNodeStoreScan<>( new RecordStorageReader( neoStores ), locks, labelUpdateVisitor,
                propertyUpdatesVisitor, labelIds, propertyKeyIdFilter );
    }

    @Override
    public <FAILURE extends Exception> StoreScan<FAILURE> visitRelationships( final int[] relationshipTypeIds, IntPredicate propertyKeyIdFilter,
            final Visitor<EntityUpdates,FAILURE> propertyUpdatesVisitor )
    {
        return new RelationshipStoreScan<>( new RecordStorageReader( neoStores ), locks, propertyUpdatesVisitor, relationshipTypeIds, propertyKeyIdFilter );
    }

    @Override
    public NodePropertyAccessor newPropertyAccessor()
    {
        return new DefaultNodePropertyAccessor( storageEngine.get() );
    }
}
