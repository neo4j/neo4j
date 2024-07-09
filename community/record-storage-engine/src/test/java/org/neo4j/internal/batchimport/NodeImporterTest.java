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

import static org.apache.commons.lang3.RandomStringUtils.randomAscii;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.internal.batchimport.SchemaMonitor.NO_MONITOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.NODE_CURSOR;
import static org.neo4j.io.pagecache.context.CursorContextFactory.NULL_CONTEXT_FACTORY;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.batchimport.cache.idmapping.IdMapper;
import org.neo4j.internal.batchimport.cache.idmapping.IdMappers;
import org.neo4j.internal.batchimport.store.BatchingNeoStores;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.token.api.TokenHolder;

@PageCacheExtension
@Neo4jLayoutExtension
class NodeImporterTest {
    @Inject
    private PageCache pageCache;

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private RecordDatabaseLayout layout;

    private ThreadPoolJobScheduler jobScheduler;
    private BatchingNeoStores stores;
    private CachedStoreCursors storeCursors;

    @BeforeEach
    void start() throws IOException {
        jobScheduler = new ThreadPoolJobScheduler();
        stores = BatchingNeoStores.batchingNeoStoresWithExternalPageCache(
                fs,
                pageCache,
                NULL,
                NULL_CONTEXT_FACTORY,
                layout,
                Configuration.DEFAULT,
                NullLogService.getInstance(),
                DefaultAdditionalIds.EMPTY,
                LogTailLogVersionsMetadata.EMPTY_LOG_TAIL,
                Config.defaults(),
                INSTANCE);
        stores.createNew();
        storeCursors = new CachedStoreCursors(stores.getNeoStores(), CursorContext.NULL_CONTEXT);
    }

    @AfterEach
    void stop() throws IOException {
        stores.close();
        jobScheduler.close();
    }

    @Test
    void shouldHandleLargeAmountsOfLabels() {
        // given
        IdMapper idMapper = mock(IdMapper.class);
        int numberOfLabels = 50;
        long nodeId = 0;

        // when
        try (NodeImporter importer = new NodeImporter(
                stores,
                idMapper,
                new DataImporter.Monitor(),
                Collector.EMPTY,
                NULL_CONTEXT_FACTORY,
                INSTANCE,
                NO_MONITOR)) {
            importer.id(nodeId);
            String[] labels = new String[numberOfLabels];
            for (int i = 0; i < labels.length; i++) {
                labels[i] = "Label" + i;
            }
            importer.labels(labels);
            importer.endOfEntity();
        }

        // then
        NodeStore nodeStore = stores.getNodeStore();
        PageCursor nodeCursor = storeCursors.readCursor(NODE_CURSOR);
        NodeRecord record = nodeStore.getRecordByCursor(nodeId, nodeStore.newRecord(), RecordLoad.NORMAL, nodeCursor);
        int[] labels = NodeLabelsField.parseLabelsField(record).get(nodeStore, storeCursors);
        assertEquals(numberOfLabels, labels.length);
    }

    @Test
    void tracePageCacheAccessOnNodeImport() {
        // given
        int numberOfLabels = 50;
        long nodeId = 0;
        var cacheTracer = new DefaultPageCacheTracer();
        var contextFactory = new CursorContextFactory(cacheTracer, EMPTY_CONTEXT_SUPPLIER);

        // when
        try (NodeImporter importer = new NodeImporter(
                stores,
                IdMappers.actual(),
                new DataImporter.Monitor(),
                Collector.EMPTY,
                contextFactory,
                INSTANCE,
                NO_MONITOR)) {
            importer.id(nodeId);
            String[] labels = new String[numberOfLabels];
            for (int i = 0; i < labels.length; i++) {
                labels[i] = "Label" + i;
            }
            importer.labels(labels);
            importer.property("a", randomAscii(10));
            importer.property("b", randomAscii(100));
            importer.property("c", randomAscii(1000));
            importer.endOfEntity();
        }

        // then
        NodeStore nodeStore = stores.getNodeStore();
        PageCursor nodeCursor = storeCursors.readCursor(NODE_CURSOR);
        NodeRecord record = nodeStore.getRecordByCursor(nodeId, nodeStore.newRecord(), RecordLoad.NORMAL, nodeCursor);
        int[] labels = NodeLabelsField.parseLabelsField(record).get(nodeStore, storeCursors);
        assertEquals(numberOfLabels, labels.length);
        assertThat(cacheTracer.faults()).isEqualTo(2);
        assertThat(cacheTracer.pins()).isEqualTo(5);
        assertThat(cacheTracer.unpins()).isEqualTo(5);
        assertThat(cacheTracer.hits()).isEqualTo(3);
    }

    @Test
    void shouldTrackAffectedSchema() throws KernelException {
        // given
        var schemaMonitor = mock(SchemaMonitor.class);

        // when
        try (var importer = new NodeImporter(
                stores,
                mock(IdMapper.class),
                new DataImporter.Monitor(),
                Collector.EMPTY,
                NULL_CONTEXT_FACTORY,
                INSTANCE,
                schemaMonitor)) {
            importNode(importer, 1, Map.of("key2", "value2", "key3", "value3"), "label1", "label2");
        }

        // then
        verify(schemaMonitor).property(keyIds("key2")[0], "value2");
        verify(schemaMonitor).property(keyIds("key3")[0], "value3");
        verify(schemaMonitor).entityTokens(labelIds("label1", "label2"));
        verify(schemaMonitor).endOfEntity(anyLong(), any());
    }

    private int[] labelIds(String... labels) throws KernelException {
        return tokenIds(stores.getTokenHolders().labelTokens(), labels);
    }

    private int[] keyIds(String... keys) throws KernelException {
        return tokenIds(stores.getTokenHolders().propertyKeyTokens(), keys);
    }

    private int[] tokenIds(TokenHolder tokenHolder, String... names) throws KernelException {
        var ids = new int[names.length];
        tokenHolder.getOrCreateIds(names, ids);
        Arrays.sort(ids);
        return ids;
    }

    private static void importNode(NodeImporter importer, long id, Map<String, String> properties, String... labels) {
        importer.id(id);
        properties.forEach(importer::property);
        importer.labels(labels);
        importer.endOfEntity();
    }
}
