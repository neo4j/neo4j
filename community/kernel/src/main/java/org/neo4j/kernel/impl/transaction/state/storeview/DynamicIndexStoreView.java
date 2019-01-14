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
package org.neo4j.kernel.impl.transaction.state.storeview;

import org.apache.commons.lang3.ArrayUtils;
import org.eclipse.collections.api.set.primitive.MutableIntSet;

import java.util.function.IntPredicate;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.index.EntityUpdates;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageReader;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.register.Register;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.util.FeatureToggles;
import org.neo4j.values.storable.Value;

/**
 * Store view that will try to use label scan store {@link LabelScanStore} to produce the view unless label scan
 * store is empty or explicitly told to use store in which cases it will fallback to whole store scan.
 */
public class DynamicIndexStoreView implements IndexStoreView
{
    private static boolean USE_LABEL_INDEX_FOR_SCHEMA_INDEX_POPULATION = FeatureToggles.flag(
            DynamicIndexStoreView.class, "use.label.index", true );

    private final NeoStoreIndexStoreView neoStoreIndexStoreView;
    private final LabelScanStore labelScanStore;
    protected final LockService locks;
    private final Log log;
    private final NeoStores neoStores;

    public DynamicIndexStoreView( NeoStoreIndexStoreView neoStoreIndexStoreView, LabelScanStore labelScanStore, LockService locks,
            NeoStores neoStores, LogProvider logProvider )
    {
        this.neoStores = neoStores;
        this.neoStoreIndexStoreView = neoStoreIndexStoreView;
        this.locks = locks;
        this.labelScanStore = labelScanStore;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public <FAILURE extends Exception> StoreScan<FAILURE> visitNodes( int[] labelIds,
            IntPredicate propertyKeyIdFilter, Visitor<EntityUpdates,FAILURE> propertyUpdatesVisitor,
            Visitor<NodeLabelUpdate,FAILURE> labelUpdateVisitor,
            boolean forceStoreScan )
    {
        if ( forceStoreScan || !USE_LABEL_INDEX_FOR_SCHEMA_INDEX_POPULATION || useAllNodeStoreScan( labelIds ) )
        {
            return neoStoreIndexStoreView.visitNodes( labelIds, propertyKeyIdFilter, propertyUpdatesVisitor, labelUpdateVisitor,
                    forceStoreScan );
        }
        return new LabelScanViewNodeStoreScan<>( new RecordStorageReader( neoStores ), locks, labelScanStore, labelUpdateVisitor,
                propertyUpdatesVisitor, labelIds, propertyKeyIdFilter );
    }

    @Override
    public <FAILURE extends Exception> StoreScan<FAILURE> visitRelationships( int[] relationshipTypeIds, IntPredicate propertyKeyIdFilter,
            Visitor<EntityUpdates,FAILURE> propertyUpdateVisitor )
    {
        return new RelationshipStoreScan<>( new RecordStorageReader( neoStores ), locks, propertyUpdateVisitor, relationshipTypeIds, propertyKeyIdFilter );
    }

    @Override
    public EntityUpdates nodeAsUpdates( long nodeId )
    {
        return neoStoreIndexStoreView.nodeAsUpdates( nodeId );
    }

    @Override
    public Register.DoubleLongRegister indexUpdatesAndSize( long indexId, Register.DoubleLongRegister output )
    {
        return neoStoreIndexStoreView.indexUpdatesAndSize( indexId, output );
    }

    @Override
    public Register.DoubleLongRegister indexSample( long indexId, Register.DoubleLongRegister output )
    {
        return neoStoreIndexStoreView.indexSample( indexId, output );
    }

    @Override
    public void replaceIndexCounts( long indexId, long uniqueElements, long maxUniqueElements, long indexSize )
    {
        neoStoreIndexStoreView.replaceIndexCounts( indexId, uniqueElements, maxUniqueElements, indexSize );
    }

    @Override
    public void incrementIndexUpdates( long indexId, long updatesDelta )
    {
        neoStoreIndexStoreView.incrementIndexUpdates( indexId, updatesDelta );
    }

    private boolean useAllNodeStoreScan( int[] labelIds )
    {
        try
        {
            return ArrayUtils.isEmpty( labelIds ) || isEmptyLabelScanStore();
        }
        catch ( Exception e )
        {
            log.error( "Can not determine number of labeled nodes, falling back to all nodes scan.", e );
            return true;
        }
    }

    private boolean isEmptyLabelScanStore() throws Exception
    {
        return labelScanStore.isEmpty();
    }

    @Override
    public Value getNodePropertyValue( long nodeId, int propertyKeyId ) throws EntityNotFoundException
    {
        return neoStoreIndexStoreView.getNodePropertyValue( nodeId, propertyKeyId );
    }

    @Override
    public void loadProperties( long entityId, EntityType type, MutableIntSet propertyIds, PropertyLoadSink sink )
    {
        neoStoreIndexStoreView.loadProperties( entityId, type, propertyIds, sink );
    }
}
