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

import java.util.List;

import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.transaction.state.PropertyCreator;
import org.neo4j.unsafe.impl.batchimport.input.InputEntity;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutorServiceStep;
import org.neo4j.unsafe.impl.batchimport.staging.Stage;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingPropertyKeyTokenRepository;

/**
 * Encodes property data into {@link PropertyBlock property blocks}, attaching them to each
 * {@link BatchEntity}. This step is designed to handle multiple threads doing the property encoding,
 * since property encoding is potentially the most costly step in this {@link Stage}.
 */
public class PropertyEncoderStep<ENTITY extends PrimitiveRecord,INPUT extends InputEntity>
        extends ExecutorServiceStep<List<BatchEntity<ENTITY,INPUT>>>
{
    private final BatchingPropertyKeyTokenRepository propertyKeyHolder;
    private final int arrayDataSize;
    private final int stringDataSize;

    protected PropertyEncoderStep( StageControl control, int workAheadSize, int numberOfExecutors,
            BatchingPropertyKeyTokenRepository propertyKeyHolder,
            PropertyStore propertyStore )
    {
        super( control, "PROPERTIES", workAheadSize, numberOfExecutors );
        this.propertyKeyHolder = propertyKeyHolder;
        this.arrayDataSize = propertyStore.getArrayStore().dataSize();
        this.stringDataSize = propertyStore.getStringStore().dataSize();
    }

    @Override
    protected Object process( long ticket, List<BatchEntity<ENTITY,INPUT>> batch )
    {
        RelativeIdRecordAllocator stringAllocator = new RelativeIdRecordAllocator( stringDataSize );
        RelativeIdRecordAllocator arrayAllocator = new RelativeIdRecordAllocator( arrayDataSize );
        PropertyCreator propertyCreator = new PropertyCreator( stringAllocator, arrayAllocator, null, null );
        for ( BatchEntity<ENTITY,INPUT> entity : batch )
        {
            INPUT input = entity.input();
            stringAllocator.initialize();
            arrayAllocator.initialize();

            if ( input.hasFirstPropertyId() )
            {   // Here we have the properties already "encoded", so just set the correct id in the entity record.
                long nextProp = input.firstPropertyId();
                entity.record().setNextProp( nextProp );
            }
            else
            {   // Encode the properties and attach the blocks to the BatchEntity.
                // Dynamic records for each entity will start from 0, they will be reassigned later anyway
                PropertyBlock[] blocks = propertyKeyHolder.propertyKeysAndValues(
                        entity.input().properties(), propertyCreator );
                entity.setPropertyBlocks( blocks );
            }
        }
        return batch;
    }
}
