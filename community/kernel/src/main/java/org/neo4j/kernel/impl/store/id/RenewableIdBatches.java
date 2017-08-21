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

import java.util.function.Supplier;

import org.neo4j.graphdb.Resource;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

public class RenewableIdBatches implements Resource
{
    private final RenewableIdBatch[] types = new RenewableIdBatch[StoreType.values().length];

    public RenewableIdBatches( NeoStores stores, int batchSize )
    {
        for ( StoreType type : StoreType.values() )
        {
            RecordStore<AbstractBaseRecord> store = stores.getRecordStore( type );
            Supplier<IdRangeIterator> source = () -> new IdRangeIterator( store.nextIdBatch( batchSize ) );
            types[type.ordinal()] = new RenewableIdBatch( source );
        }
    }

    public long nextId( StoreType type )
    {
        return types[type.ordinal()].next();
    }

    @Override
    public void close()
    {
        for ( RenewableIdBatch renewableIdBatch : types )
        {
            renewableIdBatch.close();
        }
    }
}
