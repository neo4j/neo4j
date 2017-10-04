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
package org.neo4j.unsafe.impl.batchimport;

import java.io.IOException;

import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputCache;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.Stage;
import org.neo4j.unsafe.impl.batchimport.store.BatchingNeoStores;
import org.neo4j.unsafe.impl.batchimport.store.io.IoMonitor;

import static org.neo4j.unsafe.impl.batchimport.input.InputCache.MAIN;
import static org.neo4j.unsafe.impl.batchimport.staging.Step.ORDER_SEND_DOWNSTREAM;

/**
 * Imports relationships and their properties w/o linking them together. Steps:
 * <ol>
 * <li>{@link InputIteratorBatcherStep} reading from {@link InputIterator} produced from
 * {@link Input#relationships()}.</li>
 * <li>{@link InputEntityCacherStep} alternatively {@link InputCache caches} this input data
 * (all the {@link InputRelationship input relationships}) if the iterator doesn't support
 * {@link InputIterable#supportsMultiplePasses() multiple passes}.</li>
 * into {@link PropertyBlock}, low level kernel encoded values.</li>
 * <li>{@link RelationshipPreparationStep} uses {@link IdMapper} to look up input id --> node id</li>
 * <li>{@link RelationshipRecordPreparationStep} creates {@link RelationshipRecord} and fills them with
 * data known at this point, which is start/end node ids and type</li>
 * <li>{@link PropertyEncoderStep} encodes properties from {@link InputRelationship input relationships}
 * <li>{@link EntityStoreUpdaterStep} forms {@link PropertyRecord property records} out of previously encoded
 * {@link PropertyBlock} and writes those as well as the {@link RelationshipRecord} to store.</li>
 * </ol>
 */
public class RelationshipStage extends Stage
{
    private RelationshipTypeCheckerStep typer;

    public RelationshipStage( Configuration config, IoMonitor writeMonitor,
            InputIterable<InputRelationship> relationships, IdMapper idMapper,
            Collector badCollector, InputCache inputCache,
            BatchingNeoStores neoStore, EntityStoreUpdaterStep.Monitor storeUpdateMonitor ) throws IOException
    {
        super( "Relationships", config, ORDER_SEND_DOWNSTREAM );
        add( new InputIteratorBatcherStep<>( control(), config, relationships.iterator(),
                InputRelationship.class, r -> true ) );
        if ( !relationships.supportsMultiplePasses() )
        {
            add( new InputEntityCacherStep<>( control(), config, inputCache.cacheRelationships( MAIN ) ) );
        }

        RelationshipStore relationshipStore = neoStore.getRelationshipStore();
        PropertyStore propertyStore = neoStore.getPropertyStore();
        add( typer = new RelationshipTypeCheckerStep( control(), config, neoStore.getRelationshipTypeRepository() ) );
        add( new AssignRelationshipIdBatchStep( control(), config, 0 ) );
        add( new RelationshipPreparationStep( control(), config, idMapper ) );
        add( new RelationshipRecordPreparationStep( control(), config,
                neoStore.getRelationshipTypeRepository(), badCollector ) );
        add( new PropertyEncoderStep<>( control(), config, neoStore.getPropertyKeyRepository(), propertyStore ) );
        add( new EntityStoreUpdaterStep<>( control(), config, relationshipStore, propertyStore,
                writeMonitor, storeUpdateMonitor ) );
    }

    public RelationshipTypeDistribution getDistribution()
    {
        return typer.getDistribution();
    }
}
