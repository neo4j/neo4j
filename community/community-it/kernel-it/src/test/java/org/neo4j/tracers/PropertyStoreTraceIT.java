/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.tracers;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.collection.Dependencies;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.kernel.impl.scheduler.CentralJobScheduler;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobMonitoringParams;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.time.Clocks;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.values.storable.Values.stringValue;

@DbmsExtension( configurationCallback = "configure" )
class PropertyStoreTraceIT
{
    @Inject
    private GraphDatabaseAPI database;
    @Inject
    private RecordStorageEngine storageEngine;

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        var dependencies = new Dependencies();
        // disabling periodic id buffers maintenance jobs
        dependencies.satisfyDependency( new CentralJobScheduler( Clocks.nanoClock() )
        {
            @Override
            public JobHandle<?> scheduleRecurring( Group group, JobMonitoringParams monitoredJobParams, Runnable runnable, long period, TimeUnit timeUnit )
            {
                return JobHandle.EMPTY;
            }

            @Override
            public JobHandle<?> scheduleRecurring( Group group, JobMonitoringParams monitoredJobParams, Runnable runnable, long initialDelay, long period,
                    TimeUnit unit )
            {
                return JobHandle.EMPTY;
            }
        } );
        builder.setExternalDependencies(dependencies);
    }

    @Test
    void tracePageCacheAccessOnPropertyBlockIdGeneration()
    {
        var propertyStore = storageEngine.testAccessNeoStores().getPropertyStore();
        prepareIdGenerator( propertyStore.getStringStore().getIdGenerator() );
        var pageCacheTracer = new DefaultPageCacheTracer();
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( "tracePageCacheAccessOnPropertyBlockIdGeneration" ) )
        {
            var propertyBlock = new PropertyBlock();
            var dynamicRecord = new DynamicRecord( 2 );
            dynamicRecord.setData( new byte[]{0, 1, 2, 3, 4, 5, 6, 7} );
            propertyBlock.addValueRecord( dynamicRecord );
            propertyStore.encodeValue( propertyBlock, 1, stringValue( randomAlphabetic( (int) kibiBytes( 4 ) ) ), cursorTracer, INSTANCE );

            assertThat( cursorTracer.pins() ).isOne();
            assertThat( cursorTracer.unpins() ).isOne();
            assertThat( cursorTracer.hits() ).isOne();
        }
    }

    private void prepareIdGenerator( IdGenerator idGenerator )
    {
        try ( var marker = idGenerator.marker( NULL ) )
        {
            marker.markFree( 1L );
        }
        idGenerator.clearCache( NULL );
    }
}
