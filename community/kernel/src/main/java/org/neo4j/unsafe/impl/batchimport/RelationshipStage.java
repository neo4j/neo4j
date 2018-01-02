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

import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.Stage;
import org.neo4j.unsafe.impl.batchimport.store.BatchingNeoStores;
import org.neo4j.unsafe.impl.batchimport.store.io.IoMonitor;

import static org.neo4j.unsafe.impl.batchimport.staging.Step.ORDER_PROCESS;
import static org.neo4j.unsafe.impl.batchimport.staging.Step.ORDER_SEND_DOWNSTREAM;

/**
 * Imports the first part of relationships, namely the relationship record itself and its properties.
 * Only the "next" pointers are set at this stage. The "prev" pointers are set in a later stage.
 */
public class RelationshipStage extends Stage
{
    public RelationshipStage( Configuration config, IoMonitor writeMonitor,
            InputIterable<InputRelationship> relationships, IdMapper idMapper,
            BatchingNeoStores neoStore, NodeRelationshipCache cache, boolean specificIds,
            EntityStoreUpdaterStep.Monitor storeUpdateMonitor )
    {
        super( "Relationships", config, ORDER_SEND_DOWNSTREAM | ORDER_PROCESS );
        add( new InputIteratorBatcherStep<>( control(), config, relationships.iterator(), InputRelationship.class ) );

        RelationshipStore relationshipStore = neoStore.getRelationshipStore();
        PropertyStore propertyStore = neoStore.getPropertyStore();
        add( new RelationshipPreparationStep( control(), config, idMapper ) );
        add( new PropertyEncoderStep<>( control(), config, neoStore.getPropertyKeyRepository(), propertyStore ) );
        add( new ParallelizeByNodeIdStep( control(), config ) );
        add( new RelationshipEncoderStep( control(), config,
                neoStore.getRelationshipTypeRepository(), cache, specificIds ) );
        add( new EntityStoreUpdaterStep<>( control(), config,
                relationshipStore, propertyStore, writeMonitor, storeUpdateMonitor ) );
    }
}
