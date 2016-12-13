/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.state.storeview;

import org.apache.commons.lang3.ArrayUtils;

import java.util.function.IntPredicate;
import java.util.stream.IntStream;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.labelscan.AllEntriesLabelScanReader;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.index.NodePropertyUpdates;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.register.Registers;
import org.neo4j.unsafe.impl.internal.dragons.FeatureToggles;

/**
 * Store view that will try to use label scan store {@link LabelScanStore} for cases when estimated number of nodes
 * is bellow certain threshold otherwise will fallback to whole store scan
 */
public class DynamicIndexStoreView extends NeoStoreIndexStoreView
{
    private static final int VISIT_ALL_NODES_THRESHOLD_PERCENTAGE =
            FeatureToggles.getInteger( DynamicIndexStoreView.class, "all.nodes.visit.percentage.threshold", 10 );
    protected static boolean USE_LABEL_INDEX_FOR_SCHEMA_INDEX_POPULATION = FeatureToggles.flag(
            DynamicIndexStoreView.class, "use.label.index", true );

    private final LabelScanStore labelScanStore;
    private final CountsTracker counts;
    private final Log log;

    public DynamicIndexStoreView( LabelScanStore labelScanStore, LockService locks, NeoStores neoStores,
            LogProvider logProvider )
    {
        super( locks, neoStores );
        this.counts = neoStores.getCounts();
        this.labelScanStore = labelScanStore;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public <FAILURE extends Exception> StoreScan<FAILURE> visitNodes( int[] labelIds,
            IntPredicate propertyKeyIdFilter, Visitor<NodePropertyUpdates,FAILURE> propertyUpdatesVisitor,
            Visitor<NodeLabelUpdate,FAILURE> labelUpdateVisitor )
    {
        if ( !USE_LABEL_INDEX_FOR_SCHEMA_INDEX_POPULATION || useAllNodeStoreScan( labelIds ) )
        {
            return super.visitNodes( labelIds, propertyKeyIdFilter, propertyUpdatesVisitor, labelUpdateVisitor );
        }
        return new LabelScanViewNodeStoreScan<>( nodeStore, locks, propertyStore, labelScanStore, labelUpdateVisitor,
                propertyUpdatesVisitor, labelIds, propertyKeyIdFilter );
    }

    private boolean useAllNodeStoreScan( int[] labelIds )
    {
        try
        {
            return ArrayUtils.isEmpty( labelIds ) || isEmptyLabelScanStore() ||
                    isNumberOfLabeledNodesExceedThreshold( labelIds );
        }
        catch ( Exception e )
        {
            log.error( "Can not determine number of labeled nodes, falling back to all nodes scan.", e );
            return true;
        }
    }

    private boolean isEmptyLabelScanStore() throws Exception
    {
        try ( AllEntriesLabelScanReader nodeLabelRanges = labelScanStore.allNodeLabelRanges() )
        {
            return nodeLabelRanges.maxCount() == 0;
        }
    }

    private boolean isNumberOfLabeledNodesExceedThreshold( int[] labelIds )
    {
        return getNumberOfLabeledNodes( labelIds ) > getVisitAllNodesThreshold();
    }

    private long getVisitAllNodesThreshold()
    {
        return (long) ((VISIT_ALL_NODES_THRESHOLD_PERCENTAGE / 100f) * nodeStore.getHighestPossibleIdInUse());
    }

    private long getNumberOfLabeledNodes( int[] labelIds )
    {
        return IntStream.of( labelIds )
                .mapToLong( labelId -> counts.nodeCount( labelId, Registers.newDoubleLongRegister() ).readSecond() )
                .reduce( Math::addExact )
                .orElse( 0L );
    }
}
