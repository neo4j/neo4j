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

import java.util.Arrays;
import java.util.function.IntPredicate;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.index.NodePropertyUpdates;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.register.Registers;
import org.neo4j.unsafe.impl.internal.dragons.FeatureToggles;

/**
 * Store view that will try to use label scan store {@link LabelScanStore} for cases when estimated number of nodes
 * is bellow certain threshold otherwise will fallback to whole store scan
 */
public class AdaptableStoreIndexStoreView extends NeoStoreIndexStoreView
{
    private final LabelScanStore labelScanStore;
    private final CountsTracker counts;

    private static final int VISIT_ALL_NODES_THRESHOLD_PERCENTAGE =
            FeatureToggles.getInteger( AdaptableStoreIndexStoreView.class, "all.nodes.visit.percentage.threshold", 50 );

    public AdaptableStoreIndexStoreView( LabelScanStore labelScanStore, LockService locks, NeoStores neoStores )
    {
        super( locks, neoStores );
        this.counts = neoStores.getCounts();
        this.labelScanStore = labelScanStore;
    }

    @Override
    public <FAILURE extends Exception> StoreScan<FAILURE> visitNodes( int[] labelIds,
            IntPredicate propertyKeyIdFilter, Visitor<NodePropertyUpdates,FAILURE> propertyUpdatesVisitor,
            Visitor<NodeLabelUpdate,FAILURE> labelUpdateVisitor )
    {
        if ( ArrayUtils.isEmpty( labelIds ) || isNumberOfLabeledNodesExceedThreshold( labelIds ) )
        {
            return super.visitNodes( labelIds, propertyKeyIdFilter, propertyUpdatesVisitor, labelUpdateVisitor );
        }
        return new LabelScanViewNodeStoreScan<>( nodeStore, locks, propertyStore, labelScanStore, labelUpdateVisitor,
                propertyUpdatesVisitor, labelIds, propertyKeyIdFilter );
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
        return Arrays.stream( labelIds )
                .mapToLong( labelId -> counts.nodeCount( labelId, Registers.newDoubleLongRegister() ).readSecond() )
                .reduce( Math::addExact )
                .orElse( 0L );
    }
}
