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
package org.neo4j.kernel.impl.store;

import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.kvstore.DataInitializer;
import org.neo4j.kernel.impl.util.monitoring.ProgressReporter;
import org.neo4j.kernel.impl.util.monitoring.SilentProgressReporter;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.NodeCountsStage;
import org.neo4j.unsafe.impl.batchimport.RelationshipCountsStage;
import org.neo4j.unsafe.impl.batchimport.cache.NodeLabelsCache;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;

import static org.neo4j.unsafe.impl.batchimport.staging.ExecutionSupervisors.superviseDynamicExecution;

public class CountsComputer implements DataInitializer<CountsAccessor.Updater>
{
    public static void recomputeCounts( NeoStores stores, PageCache pageCache, DatabaseLayout databaseLayout )
    {
        MetaDataStore metaDataStore = stores.getMetaDataStore();
        CountsTracker counts = stores.getCounts();
        try ( CountsAccessor.Updater updater = counts.reset( metaDataStore.getLastCommittedTransactionId() ) )
        {
            new CountsComputer( stores, pageCache, databaseLayout ).initialize( updater );
        }
    }

    private final NodeStore nodes;
    private final RelationshipStore relationships;
    private final int highLabelId;
    private final int highRelationshipTypeId;
    private final long lastCommittedTransactionId;
    private final ProgressReporter progressMonitor;
    private final NumberArrayFactory numberArrayFactory;

    CountsComputer( NeoStores stores, PageCache pageCache, DatabaseLayout databaseLayout )
    {
        this( stores.getMetaDataStore().getLastCommittedTransactionId(),
                stores.getNodeStore(), stores.getRelationshipStore(),
                (int) stores.getLabelTokenStore().getHighId(),
                (int) stores.getRelationshipTypeTokenStore().getHighId(),
                NumberArrayFactory.auto( pageCache, databaseLayout.databaseDirectory(), true, NumberArrayFactory.NO_MONITOR ) );
    }

    private CountsComputer( long lastCommittedTransactionId, NodeStore nodes, RelationshipStore relationships, int highLabelId, int highRelationshipTypeId,
            NumberArrayFactory numberArrayFactory )
    {
        this( lastCommittedTransactionId, nodes, relationships, highLabelId, highRelationshipTypeId,
                numberArrayFactory, SilentProgressReporter.INSTANCE );
    }

    public CountsComputer( long lastCommittedTransactionId, NodeStore nodes, RelationshipStore relationships,
            int highLabelId, int highRelationshipTypeId, NumberArrayFactory numberArrayFactory, ProgressReporter progressMonitor )
    {
        this.lastCommittedTransactionId = lastCommittedTransactionId;
        this.nodes = nodes;
        this.relationships = relationships;
        this.highLabelId = highLabelId;
        this.highRelationshipTypeId = highRelationshipTypeId;
        this.numberArrayFactory = numberArrayFactory;
        this.progressMonitor = progressMonitor;
    }

    @Override
    public void initialize( CountsAccessor.Updater countsUpdater )
    {
        if ( hasNotEmptyNodesOrRelationshipsStores() )
        {
            progressMonitor.start( nodes.getHighestPossibleIdInUse() + relationships.getHighestPossibleIdInUse() );
            populateCountStore( countsUpdater );
        }
        progressMonitor.completed();
    }

    private boolean hasNotEmptyNodesOrRelationshipsStores()
    {
        return (nodes.getHighestPossibleIdInUse() != -1) || (relationships.getHighestPossibleIdInUse() != -1);
    }

    private void populateCountStore( CountsAccessor.Updater countsUpdater )
    {
        try ( NodeLabelsCache cache = new NodeLabelsCache( numberArrayFactory, highLabelId ) )
        {
            // Count nodes
            superviseDynamicExecution( new NodeCountsStage( Configuration.DEFAULT, cache, nodes, highLabelId, countsUpdater, progressMonitor ) );
            // Count relationships
            superviseDynamicExecution(
                    new RelationshipCountsStage( Configuration.DEFAULT, cache, relationships, highLabelId, highRelationshipTypeId, countsUpdater,
                            numberArrayFactory, progressMonitor ) );
        }
    }

    @Override
    public long initialVersion()
    {
        return lastCommittedTransactionId;
    }
}
