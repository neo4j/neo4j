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

import java.util.function.Predicate;

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.Stage;
import org.neo4j.unsafe.impl.batchimport.store.BatchingNeoStores;
import org.neo4j.unsafe.impl.batchimport.store.io.IoMonitor;

import static org.neo4j.unsafe.impl.batchimport.staging.Step.ORDER_SEND_DOWNSTREAM;

/**
 * Imports the initial part of relationships, namely the relationship record itself and its properties.
 * Only the "next" pointers are set at this stage. The "prev" pointers are set in a
 * {@link RelationshipLinkbackStage later stage} to avoid random store access. Steps:
 *
 * <ol>
 * <li>{@link InputIteratorBatcherStep} reading from {@link InputIterator} produced from {@link Input#relationships()}.
 * </li>
 * <li>{@link AssignRelationshipIdBatchStep} assigns record ids to batches. This to have one source of id allocation
 * since there are later steps which can be multi-threaded.</li>
 * <li>{@link RelationshipPreparationStep} looks up {@link InputRelationship#startNode() start node input id} /
 * {@link InputRelationship#endNode() end node input id} from {@link IdMapper} and attaches to the batches going
 * through because that lookup is costly and this step can be parallelized.</li>
 * <li>{@link RelationshipRecordPreparationStep} creates {@link RelationshipRecord relationship record instances}
 * and initializes them with default values suitable for import.</li>
 * <li>{@link PropertyEncoderStep} encodes properties from {@link InputNode input nodes} into {@link PropertyBlock},
 * low level kernel encoded values.</li>
 * <li>{@link RelationshipEncoderStep} sets "next" pointers in {@link RelationshipRecord} by getting id
 * from {@link NodeRelationshipCache} based on node id and {@link Direction} and at the same time updating
 * that cache entry to the id of the relationship. This forms the relationship chain linked lists.</li>
 * <li>{@link EntityStoreUpdaterStep} forms {@link PropertyRecord property records} out of previously encoded
 * {@link PropertyBlock} and writes those as well as the {@link RelationshipRecord} to store.</li>
 * </ol>
 *
 * This stage can be run multiple times, once per relationship type and new relationships are being appended
 * to the end of the store, that's why this stage accepts a relationship id to start at (firstRelationshipId).
 *
 * It is also to be said that the relationship type ids are imported descending, i.e. w/ the highest type id first
 * down to the lowest last. This simply because all records (even {@link RelationshipGroupRecord relationship groups})
 * are appended to the end, only "next" pointers can be provided in new records in the face of the sequential-only
 * I/O restriction and {@link RelationshipGroupRecord} chains must be in order of ascending type id.
 */
public class RelationshipStage extends Stage
{
    private AssignRelationshipIdBatchStep idAssigner;

    public RelationshipStage( String topic, Configuration config, IoMonitor writeMonitor,
            Predicate<InputRelationship> typeFilter,
            InputIterator<InputRelationship> relationships, IdMapper idMapper, BatchingNeoStores neoStore,
            NodeRelationshipCache cache, EntityStoreUpdaterStep.Monitor storeUpdateMonitor,
            long firstRelationshipId )
    {
        super( "Relationships" + topic, config, ORDER_SEND_DOWNSTREAM );
        add( new InputIteratorBatcherStep<>( control(), config, relationships, InputRelationship.class, typeFilter ) );

        RelationshipStore relationshipStore = neoStore.getRelationshipStore();
        PropertyStore propertyStore = neoStore.getPropertyStore();
        add( idAssigner = new AssignRelationshipIdBatchStep( control(), config, firstRelationshipId ) );
        add( new RelationshipPreparationStep( control(), config, idMapper ) );
        add( new RelationshipRecordPreparationStep( control(), config, neoStore.getRelationshipTypeRepository() ) );
        add( new PropertyEncoderStep<>( control(), config, neoStore.getPropertyKeyRepository(), propertyStore ) );
        add( new RelationshipEncoderStep( control(), config, cache ) );
        add( new EntityStoreUpdaterStep<>( control(), config, relationshipStore, propertyStore, writeMonitor,
                storeUpdateMonitor ) );
    }

    public long getNextRelationshipId()
    {
        return idAssigner.getNextRelationshipId();
    }
}
