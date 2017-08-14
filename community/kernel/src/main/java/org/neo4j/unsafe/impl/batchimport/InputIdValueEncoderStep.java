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

import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.staging.BatchSender;
import org.neo4j.unsafe.impl.batchimport.staging.ProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

public class InputIdValueEncoderStep extends ProcessorStep<Batch<InputNode,NodeRecord>>
{
    static final int INPUT_ID_KEY_ID = 0;
    private final PropertyStore inputIdValueStore;

    public InputIdValueEncoderStep( StageControl control, Configuration config, PropertyStore inputIdValueStore )
    {
        super( control, ":ID", config, 0 );
        this.inputIdValueStore = inputIdValueStore;
    }

    @Override
    protected void process( Batch<InputNode,NodeRecord> batch, BatchSender sender ) throws Throwable
    {
        batch.inputIdPropertyRecords = new PropertyRecord[batch.input.length];
        for ( int i = 0; i < batch.input.length; i++ )
        {
            Object id = batch.input[i].id();
            if ( id != null )
            {
                PropertyBlock block = new PropertyBlock();
                PropertyStore.encodeValue( block, INPUT_ID_KEY_ID, id, inputIdValueStore.getStringStore(),
                        inputIdValueStore.getArrayStore() );

                // An optimization where each property record will represent one input :ID and the id of
                // the property record will be the node record id, for efficient assignment and latter,
                // efficient lookup.
                PropertyRecord record = new PropertyRecord( batch.records[i].getId() );
                record.setInUse( true );
                record.addPropertyBlock( block );
                batch.inputIdPropertyRecords[i] = record;
            }
        }
        sender.send( batch );
    }
}
