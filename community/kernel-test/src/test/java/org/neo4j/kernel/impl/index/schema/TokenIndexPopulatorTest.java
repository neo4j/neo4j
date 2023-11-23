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
package org.neo4j.kernel.impl.index.schema;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.CursorContextFactory.NULL_CONTEXT_FACTORY;
import static org.neo4j.kernel.impl.api.index.PhaseTracker.nullInstance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;
import org.junit.jupiter.api.Test;
import org.neo4j.index.internal.gbptree.MultiRootGBPTree;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.TokenIndexEntryUpdate;

class TokenIndexPopulatorTest extends IndexPopulatorTests<TokenScanKey, TokenScanValue, TokenScanLayout> {

    @Override
    IndexDescriptor indexDescriptor() {
        return forSchema(SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR, TokenIndexProvider.DESCRIPTOR)
                .withName("index")
                .materialise(0);
    }

    @Override
    TokenScanLayout layout() {
        return new TokenScanLayout();
    }

    @Override
    byte failureByte() {
        return TokenIndex.FAILED;
    }

    @Override
    byte populatingByte() {
        return TokenIndex.POPULATING;
    }

    @Override
    byte onlineByte() {
        return TokenIndex.ONLINE;
    }

    @Override
    TokenIndexPopulator createPopulator(PageCache pageCache) {
        return createPopulator(pageCache, new Monitors(), "");
    }

    private TokenIndexPopulator createPopulator(PageCache pageCache, Monitors monitors, String monitorTag) {
        DatabaseIndexContext context = DatabaseIndexContext.builder(
                        pageCache, fs, NULL_CONTEXT_FACTORY, PageCacheTracer.NULL, DEFAULT_DATABASE_NAME)
                .withMonitors(monitors)
                .withTag(monitorTag)
                .withReadOnlyChecker(writable())
                .build();
        return new TokenIndexPopulator(
                context, indexFiles, indexDescriptor, Sets.immutable.empty(), StorageEngineIndexingBehaviour.EMPTY);
    }

    @Test
    void addShouldApplyAllUpdatesOnce() throws Exception {
        // Give
        MutableLongObjectMap<int[]> entityTokens = LongObjectMaps.mutable.empty();

        populator.create();

        List<TokenIndexEntryUpdate<?>> updates = TokenIndexUtility.generateSomeRandomUpdates(entityTokens, random);
        // Add updates to populator
        populator.add(updates, NULL_CONTEXT);

        populator.scanCompleted(nullInstance, populationWorkScheduler, NULL_CONTEXT);
        populator.close(true, NULL_CONTEXT);

        TokenIndexUtility.verifyUpdates(entityTokens, layout, this::getTree, new DefaultTokenIndexIdLayout());
    }

    @Test
    void updaterShouldApplyUpdates() throws Exception {
        // Give
        MutableLongObjectMap<int[]> entityTokens = LongObjectMaps.mutable.empty();

        populator.create();

        List<TokenIndexEntryUpdate<?>> updates = TokenIndexUtility.generateSomeRandomUpdates(entityTokens, random);

        try (IndexUpdater updater = populator.newPopulatingUpdater(NULL_CONTEXT)) {
            for (TokenIndexEntryUpdate<?> update : updates) {
                updater.process(update);
            }
        }

        // then
        populator.scanCompleted(nullInstance, populationWorkScheduler, NULL_CONTEXT);
        populator.close(true, NULL_CONTEXT);
        TokenIndexUtility.verifyUpdates(entityTokens, layout, this::getTree, new DefaultTokenIndexIdLayout());
    }

    @Test
    void updaterMustThrowIfProcessAfterClose() throws Exception {
        // given
        populator.create();
        IndexUpdater updater = populator.newPopulatingUpdater(NULL_CONTEXT);

        // when
        updater.close();

        IllegalStateException e = assertThrows(
                IllegalStateException.class,
                () -> updater.process(IndexEntryUpdate.change(
                        random.nextInt(), null, EMPTY_INT_ARRAY, TokenIndexUtility.generateRandomTokens(random))));
        assertThat(e).hasMessageContaining("Updater has been closed");
        populator.close(true, NULL_CONTEXT);
    }

    @Test
    void shouldHandleInterleavedRandomizedUpdates() throws IndexEntryConflictException, IOException {
        // Give
        int numberOfEntities = 1_000;
        long currentScanId = 0;
        MutableLongObjectMap<int[]> entityTokens = LongObjectMaps.mutable.empty();

        populator.create();

        while (currentScanId < numberOfEntities) {
            // Collect a batch of max 100 updates from scan
            List<TokenIndexEntryUpdate<?>> updates = new ArrayList<>();
            for (int i = 0; i < 100 && currentScanId < numberOfEntities; i++) {
                TokenIndexUtility.generateRandomUpdate(currentScanId, entityTokens, updates, random);

                // Advance scan
                currentScanId++;
            }
            // Add updates to populator
            populator.add(updates, NULL_CONTEXT);

            // Interleave external updates in id range lower than currentScanId
            try (IndexUpdater updater = populator.newPopulatingUpdater(NULL_CONTEXT)) {
                for (int i = 0; i < 100; i++) {
                    long entityId = random.nextLong(currentScanId);
                    // Current tokens for the entity in the tree
                    int[] beforeTokens = entityTokens.get(entityId);
                    if (beforeTokens == null) {
                        beforeTokens = EMPTY_INT_ARRAY;
                    }
                    int[] afterTokens = TokenIndexUtility.generateRandomTokens(random);
                    entityTokens.put(entityId, Arrays.copyOf(afterTokens, afterTokens.length));
                    updater.process(IndexEntryUpdate.change(entityId, null, beforeTokens, afterTokens));
                }
            }
        }

        populator.scanCompleted(nullInstance, populationWorkScheduler, NULL_CONTEXT);
        populator.close(true, NULL_CONTEXT);

        TokenIndexUtility.verifyUpdates(entityTokens, layout, this::getTree, new DefaultTokenIndexIdLayout());
    }

    @Test
    void shouldRelayMonitorCallsToRegisteredGBPTreeMonitorWithoutTag() throws IOException {
        // Given
        AtomicBoolean checkpointCompletedCall = new AtomicBoolean();
        Monitors monitors = new Monitors();
        monitors.addMonitorListener(getCheckpointCompletedListener(checkpointCompletedCall));
        populator = createPopulator(pageCache, monitors, "tag");

        // When
        populator.create();
        populator.close(true, NULL_CONTEXT);

        // Then
        assertTrue(checkpointCompletedCall.get());
    }

    @Test
    void shouldNotRelayMonitorCallsToRegisteredGBPTreeMonitorWithDifferentTag() throws IOException {
        // Given
        AtomicBoolean checkpointCompletedCall = new AtomicBoolean();
        Monitors monitors = new Monitors();
        monitors.addMonitorListener(getCheckpointCompletedListener(checkpointCompletedCall), "differentTag");
        populator = createPopulator(pageCache, monitors, "tag");

        // When
        populator.create();
        populator.close(true, NULL_CONTEXT);

        // Then
        assertFalse(checkpointCompletedCall.get());
    }

    @Test
    void shouldRelayMonitorCallsToRegisteredGBPTreeMonitorWithTag() throws IOException {
        // Given
        AtomicBoolean checkpointCompletedCall = new AtomicBoolean();
        Monitors monitors = new Monitors();
        monitors.addMonitorListener(getCheckpointCompletedListener(checkpointCompletedCall), "tag");
        populator = createPopulator(pageCache, monitors, "tag");

        // When
        populator.create();
        populator.close(true, NULL_CONTEXT);

        // Then
        assertTrue(checkpointCompletedCall.get());
    }

    private static MultiRootGBPTree.Monitor.Adaptor getCheckpointCompletedListener(
            AtomicBoolean checkpointCompletedCall) {
        return new MultiRootGBPTree.Monitor.Adaptor() {
            @Override
            public void checkpointCompleted() {
                checkpointCompletedCall.set(true);
            }
        };
    }
}
