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
package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.Stage;
import org.neo4j.unsafe.impl.batchimport.store.BatchingNeoStores;

import static org.neo4j.unsafe.impl.batchimport.staging.Step.ORDER_SEND_DOWNSTREAM;

/**
 * Inserts relationships one by one, {@link BatchInserter} style which may incur random I/O. This stage
 * should only be used on relationship types which are very small (<100). Steps:
 *
 * <ol>
 * <li>{@link InputIteratorBatcherStep} reading from {@link InputIterator} produced from {@link Input#nodes()}.</li>
 * <li>{@link RelationshipPreparationStep} looks up {@link InputRelationship#startNode() start node input id} /
 * {@link InputRelationship#endNode() end node input id} from {@link IdMapper} and attaches to the batches going
 * through because that lookup is costly and this step can be parallelized.</li>
 * <li>{@link PropertyEncoderStep} encodes properties from {@link InputRelationship input relationships} into
 * {@link PropertyBlock}, low level kernel encoded values.</li>
 * <li>{@link BatchInsertRelationshipsStep} inserts relationships one by one by reading from and updating store
 * as it sees required.</li>
 * </ol>
 */
public class BatchInsertRelationshipsStage extends Stage
{
    public BatchInsertRelationshipsStage( Configuration config, IdMapper idMapper,
            InputIterator<InputRelationship> relationships, BatchingNeoStores store, long nextRelationshipId )
    {
        super( "Minority relationships", config, ORDER_SEND_DOWNSTREAM );
        add( new InputIteratorBatcherStep<>( control(), config, relationships, InputRelationship.class ) );
        add( new RelationshipPreparationStep( control(), config, idMapper ) );
        add( new PropertyEncoderStep<>( control(), config, store.getPropertyKeyRepository(),
                store.getPropertyStore() ) );
        add( new BatchInsertRelationshipsStep( control(), config, store,
                store.getRelationshipTypeRepository(), nextRelationshipId ) );
    }
}
