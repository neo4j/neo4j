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
package org.neo4j.internal.batchimport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.batchimport.api.Configuration.DEFAULT;
import static org.neo4j.configuration.Config.defaults;
import static org.neo4j.internal.batchimport.DefaultAdditionalIds.EMPTY;
import static org.neo4j.internal.batchimport.store.BatchingNeoStores.batchingNeoStoresWithExternalPageCache;
import static org.neo4j.io.pagecache.context.CursorContextFactory.NULL_CONTEXT_FACTORY;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata.EMPTY_LOG_TAIL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.batchimport.cache.idmapping.IdMappers;
import org.neo4j.internal.batchimport.input.Collectors;
import org.neo4j.internal.batchimport.store.BatchingNeoStores;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.utils.TestDirectory;

@EphemeralPageCacheExtension
class RelationshipImporterTest {
    @Inject
    private PageCache pageCache;

    @Inject
    private TestDirectory directory;

    private BatchingNeoStores stores;

    @BeforeEach
    void start() throws IOException {
        stores = batchingNeoStoresWithExternalPageCache(
                directory.getFileSystem(),
                pageCache,
                NULL,
                NULL_CONTEXT_FACTORY,
                RecordDatabaseLayout.ofFlat(directory.homePath()),
                DEFAULT,
                NullLogService.getInstance(),
                EMPTY,
                EMPTY_LOG_TAIL,
                defaults(),
                INSTANCE);
        stores.createNew();
    }

    @AfterEach
    void stop() throws IOException {
        stores.close();
    }

    @Test
    void shouldReportMissingNodeForActualIds() {
        // given
        var idMapper = IdMappers.actual();
        var monitor = new DataImporter.Monitor();
        var output = new ByteArrayOutputStream();
        try (var collector = Collectors.badCollector(output, 1);
                var importer = new RelationshipImporter(
                        stores,
                        idMapper,
                        new DataStatistics(monitor, new DataStatistics.RelationshipTypeCount[0]),
                        monitor,
                        collector,
                        false,
                        false,
                        NULL_CONTEXT_FACTORY,
                        INSTANCE,
                        SchemaMonitor.NO_MONITOR); ) {

            // when
            importer.startId(1);
            importer.type(2);
            importer.endOfEntity();
        }

        // then
        assertThat(output.toString()).contains("1 (null)-[2]->null (null) is missing data");
    }
}
