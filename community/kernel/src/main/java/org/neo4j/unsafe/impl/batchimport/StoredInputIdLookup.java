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

import java.util.function.LongFunction;

import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;

import static org.neo4j.unsafe.impl.batchimport.InputIdValueEncoderStep.INPUT_ID_KEY_ID;

class StoredInputIdLookup implements LongFunction<Object>
{
    private final PropertyStore inputIdValueStore;

    StoredInputIdLookup( PropertyStore inputIdValueStore )
    {
        this.inputIdValueStore = inputIdValueStore;
    }

    @Override
    public Object apply( long id )
    {
        PropertyRecord record = inputIdValueStore.newRecord();
        inputIdValueStore.getRecord( id, record, RecordLoad.NORMAL );
        PropertyBlock block = record.getPropertyBlock( INPUT_ID_KEY_ID );
        assert block != null;
        return inputIdValueStore.getValue( block );
    }
}
