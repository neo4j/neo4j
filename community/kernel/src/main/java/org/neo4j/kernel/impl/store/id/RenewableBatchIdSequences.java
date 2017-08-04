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
package org.neo4j.kernel.impl.store.id;

import java.util.function.LongConsumer;
import java.util.function.Supplier;

import org.neo4j.graphdb.Resource;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

public class RenewableBatchIdSequences implements Resource
{
    private final RenewableBatchIdSequence[] types = new RenewableBatchIdSequence[StoreType.values().length];

    public RenewableBatchIdSequences( NeoStores stores, int batchSize )
    {
        for ( StoreType type : StoreType.values() )
        {
            if ( type.isRecordStore() )
            {
                RecordStore<AbstractBaseRecord> store = stores.getRecordStore( type );
                Supplier<IdRangeIterator> source = () -> new IdRangeIterator( store.nextIdBatch( batchSize ) );
                LongConsumer idConsumer = id -> store.freeId( id );
                types[type.ordinal()] = new RenewableBatchIdSequence( source, idConsumer );
            }
        }
    }

    public long nextId( StoreType type )
    {
        return idGenerator( type ).nextId();
    }

    public RenewableBatchIdSequence idGenerator( StoreType type )
    {
        return types[type.ordinal()];
    }

    @Override
    public void close()
    {
        for ( StoreType type : StoreType.values() )
        {
            if ( type.isRecordStore() )
            {
                idGenerator( type ).close();
            }
        }
    }
}
