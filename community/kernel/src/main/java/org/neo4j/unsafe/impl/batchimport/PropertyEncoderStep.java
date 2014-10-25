/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.util.Iterator;

import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.transaction.state.PropertyCreator;
import org.neo4j.unsafe.impl.batchimport.input.InputEntity;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutorServiceStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;
import org.neo4j.unsafe.impl.batchimport.store.BatchingPropertyRecordAccess;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingPropertyKeyTokenRepository;

/**
 * Accepts {@link RecordBatch batches} of {@link NodeRecord nodes} and fills those with encoded properties.
 */
public class PropertyEncoderStep<ENTITY extends PrimitiveRecord,INPUT extends InputEntity>
        extends ExecutorServiceStep<RecordBatch<ENTITY,INPUT>>
{
    private final BatchingPropertyKeyTokenRepository propertyKeyHolder;
    private final PropertyCreator propertyCreator;

    protected PropertyEncoderStep( StageControl control, int workAheadSize, int numberOfExecutors,
            BatchingPropertyKeyTokenRepository propertyKeyHolder,
            PropertyStore propertyStore )
    {
        super( control, "NODE PROPERTY", workAheadSize, numberOfExecutors );
        this.propertyKeyHolder = propertyKeyHolder;
        this.propertyCreator = new PropertyCreator( propertyStore, null );
    }

    @Override
    protected Object process( long ticket, RecordBatch<ENTITY,INPUT> batch )
    {
        BatchingPropertyRecordAccess propertyRecords = new BatchingPropertyRecordAccess();
        Iterator<ENTITY> records = batch.getEntityRecords().iterator();
        Iterator<INPUT> inputs = batch.getInputEntities().iterator();
        while ( inputs.hasNext() )
        {
            INPUT input = inputs.next();
            ENTITY record = records.next();

            // Properties
            long nextProp;
            if ( input.hasFirstPropertyId() )
            {
                nextProp = input.firstPropertyId();
            }
            else
            {
                nextProp = propertyCreator.createPropertyChain( record, propertyKeyHolder.propertyKeysAndValues(
                        input.properties(), propertyCreator ), propertyRecords );
            }
            record.setNextProp( nextProp );
        }

        batch.setPropertyRecords( propertyRecords.records() );
        return batch;
    }
}
