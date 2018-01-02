/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport;

import java.io.IOException;

import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerator;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.InputCache;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.staging.Stage;
import org.neo4j.unsafe.impl.batchimport.stats.StatsProvider;
import org.neo4j.unsafe.impl.batchimport.store.BatchingNeoStores;
import org.neo4j.unsafe.impl.batchimport.store.io.IoMonitor;

import static org.neo4j.unsafe.impl.batchimport.staging.Step.ORDER_SEND_DOWNSTREAM;

/**
 * Imports nodes and their properties, everything except
 * {@link NodeRecord#setNextRel(long) first relationship id pointer} is set for every node in this stage.
 */
public class NodeStage extends Stage
{
    public NodeStage( Configuration config, IoMonitor writeMonitor,
            InputIterable<InputNode> nodes, IdMapper idMapper, IdGenerator idGenerator,
            BatchingNeoStores neoStore, InputCache inputCache, LabelScanStore labelScanStore,
            EntityStoreUpdaterStep.Monitor storeUpdateMonitor,
            StatsProvider memoryUsage ) throws IOException
    {
        super( "Nodes", config, ORDER_SEND_DOWNSTREAM );
        add( new InputIteratorBatcherStep<>( control(), config, nodes.iterator(), InputNode.class ) );
        if ( !nodes.supportsMultiplePasses() )
        {
            add( new InputEntityCacherStep<>( control(), config, inputCache.cacheNodes() ) );
        }

        NodeStore nodeStore = neoStore.getNodeStore();
        PropertyStore propertyStore = neoStore.getPropertyStore();
        add( new PropertyEncoderStep<>( control(), config, neoStore.getPropertyKeyRepository(), propertyStore ) );
        add( new NodeEncoderStep( control(), config, idMapper, idGenerator,
                neoStore.getLabelRepository(), nodeStore, memoryUsage ) );
        add( new LabelScanStorePopulationStep( control(), config, labelScanStore ) );
        add( new EntityStoreUpdaterStep<>( control(), config, nodeStore, propertyStore,
                writeMonitor, storeUpdateMonitor ) );
    }
}
