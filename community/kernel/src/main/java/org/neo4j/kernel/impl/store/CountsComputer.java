/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.store;

import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.transaction.state.NeoStoreProvider;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.NodeCountsProcessor;
import org.neo4j.unsafe.impl.batchimport.NodeStoreProcessorStage;
import org.neo4j.unsafe.impl.batchimport.RelationshipCountsStage;
import org.neo4j.unsafe.impl.batchimport.cache.NodeLabelsCache;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;

import static org.neo4j.unsafe.impl.batchimport.staging.ExecutionSupervisors.superviseDynamicExecution;

public class CountsComputer
{
    public static void computeCounts( GraphDatabaseAPI api )
    {
        computeCounts( api.getDependencyResolver().resolveDependency( NeoStoreProvider.class ).evaluate() );
    }

    public static void computeCounts( NeoStore stores )
    {
        computeCounts( stores.getNodeStore(), stores.getRelationshipStore(), stores.getCounts(),
                (int)stores.getLabelTokenStore().getHighId(), (int)stores.getRelationshipTypeTokenStore().getHighId() );
    }

    public static void computeCounts( NodeStore nodeStore, RelationshipStore relationshipStore,
            CountsTracker countsTracker, int highLabelId, int highRelationshipTypeId )
    {
        new CountsComputer( nodeStore, relationshipStore, countsTracker,
                highLabelId, highRelationshipTypeId ).rebuildCounts();
    }

    private final NodeStore nodes;
    private final RelationshipStore relationships;
    private final CountsTracker countsTracker;
    private final int highLabelId;
    private final int highRelationshipTypeId;

    public CountsComputer( NodeStore nodes, RelationshipStore relationships, CountsTracker countsTracker,
            int highLabelId, int highRelationshipTypeId )
    {
        this.nodes = nodes;
        this.relationships = relationships;
        this.countsTracker = countsTracker;
        this.highLabelId = highLabelId;
        this.highRelationshipTypeId = highRelationshipTypeId;
    }

    public void rebuildCounts()
    {
        NodeLabelsCache cache = new NodeLabelsCache( NumberArrayFactory.AUTO, highLabelId );
        try ( CountsAccessor.Updater countsUpdater = countsTracker.reset() )
        {
            // Count nodes
            superviseDynamicExecution( new NodeStoreProcessorStage( "COUNT NODES", Configuration.DEFAULT, nodes,
                    new NodeCountsProcessor( nodes, cache, highLabelId, countsUpdater ) ) );
            // Count relationships
            superviseDynamicExecution( new RelationshipCountsStage( Configuration.DEFAULT, cache, relationships,
                    highLabelId, highRelationshipTypeId, countsUpdater ) );
        }
        finally
        {
            cache.close();
        }
    }
}
