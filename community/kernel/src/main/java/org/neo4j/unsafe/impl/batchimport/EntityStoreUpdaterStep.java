/**
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

import java.util.Collection;

import org.neo4j.kernel.impl.nioneo.store.AbstractRecordStore;
import org.neo4j.kernel.impl.nioneo.store.PrimitiveRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutorServiceStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;
import org.neo4j.unsafe.impl.batchimport.stats.StatsProvider;
import org.neo4j.unsafe.impl.batchimport.store.io.IoMonitor;

/**
 * Writes {@link RecordBatch entity batches} to the underlying stores.
 *
 * @param <T> type of entities.
 */
public class EntityStoreUpdaterStep<T extends PrimitiveRecord> extends ExecutorServiceStep<RecordBatch<T>>
{
    private final AbstractRecordStore<T> entityStore;
    private final PropertyStore propertyStore;
    private final IoMonitor monitor;

    EntityStoreUpdaterStep( StageControl control, String name, AbstractRecordStore<T> entityStore,
            PropertyStore propertyStore, IoMonitor monitor )
    {
        super( control, name, 1, 1 ); // work-ahead doesn't matter, we're the last one
        this.entityStore = entityStore;
        this.propertyStore = propertyStore;
        this.monitor = monitor;
        this.monitor.reset();
    }

    @Override
    protected Object process( long ticket, RecordBatch<T> batch )
    {
        for ( T entityRecord : batch.getEntityRecords() )
        {
            // +1 since "high id" is the next id to return, i.e. "high id" is "highest id in use"+1
            entityStore.setHighId( entityRecord.getId()+1 );
            entityStore.updateRecord( entityRecord );
        }
        for ( PropertyRecord propertyRecord : batch.getPropertyRecords() )
        {
            propertyStore.updateRecord( propertyRecord );
        }
        return null; // end of the line
    }

    @Override
    protected void addStatsProviders( Collection<StatsProvider> providers )
    {
        super.addStatsProviders( providers );
        providers.add( monitor );
    }
}
