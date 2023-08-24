/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.tracers;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.collection.Dependencies.dependenciesOf;
import static org.neo4j.configuration.GraphDatabaseSettings.db_format;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.impl.store.StoreType.PROPERTY_ARRAY;
import static org.neo4j.kernel.impl.store.StoreType.PROPERTY_STRING;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.values.storable.Values.stringValue;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.scheduler.CentralJobScheduler;
import org.neo4j.kernel.impl.store.DynamicAllocatorProvider;
import org.neo4j.kernel.impl.store.DynamicAllocatorProviders;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobMonitoringParams;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.time.Clocks;

@DbmsExtension(configurationCallback = "configure")
class PropertyStoreTraceIT {
    @Inject
    private GraphDatabaseAPI database;

    @Inject
    private RecordStorageEngine storageEngine;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(db_format, RecordFormatSelector.defaultFormat().name());
        var dependencies = dependenciesOf(new CentralJobScheduler(Clocks.nanoClock(), NullLogProvider.getInstance()) {
            @Override
            public JobHandle<?> scheduleRecurring(
                    Group group,
                    JobMonitoringParams monitoredJobParams,
                    Runnable runnable,
                    long period,
                    TimeUnit timeUnit) {
                return JobHandle.EMPTY;
            }

            @Override
            public JobHandle<?> scheduleRecurring(
                    Group group,
                    JobMonitoringParams monitoredJobParams,
                    Runnable runnable,
                    long initialDelay,
                    long period,
                    TimeUnit unit) {
                return JobHandle.EMPTY;
            }
        });
        builder.setExternalDependencies(dependencies);
    }

    @Test
    void tracePageCacheAccessOnPropertyBlockIdGeneration() {
        NeoStores neoStores = storageEngine.testAccessNeoStores();
        var propertyStore = neoStores.getPropertyStore();
        prepareIdGenerator(propertyStore.getStringStore().getIdGenerator());
        var pageCacheTracer = new DefaultPageCacheTracer();
        var contextFactory = new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER);
        try (var cursorContext = contextFactory.create("tracePageCacheAccessOnPropertyBlockIdGeneration")) {
            var propertyBlock = new PropertyBlock();
            var dynamicRecord = new DynamicRecord(2);
            dynamicRecord.setData(new byte[] {0, 1, 2, 3, 4, 5, 6, 7});
            propertyBlock.addValueRecord(dynamicRecord);
            DynamicAllocatorProvider allocatorProvider = DynamicAllocatorProviders.nonTransactionalAllocator(neoStores);

            PropertyStore.encodeValue(
                    propertyBlock,
                    1,
                    stringValue(randomAlphabetic((int) kibiBytes(4))),
                    allocatorProvider.allocator(PROPERTY_STRING),
                    allocatorProvider.allocator(PROPERTY_ARRAY),
                    cursorContext,
                    INSTANCE);

            PageCursorTracer cursorTracer = cursorContext.getCursorTracer();
            assertThat(cursorTracer.pins()).isOne();
            assertThat(cursorTracer.unpins()).isOne();
            assertThat(cursorTracer.hits()).isOne();
        }
    }

    private static void prepareIdGenerator(IdGenerator idGenerator) {
        try (var marker = idGenerator.contextualMarker(NULL_CONTEXT)) {
            marker.markFree(1L);
        }
        idGenerator.clearCache(NULL_CONTEXT);
    }
}
