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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;
import static org.neo4j.internal.helpers.collection.Iterators.single;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.impl.api.index.IndexUpdateMode.ONLINE;
import static org.neo4j.kernel.impl.index.schema.IndexUsageTracker.NO_USAGE_TRACKER;
import static org.neo4j.kernel.impl.index.schema.TokenIndexUtility.TOKENS;
import static org.neo4j.kernel.impl.index.schema.TokenIndexUtility.generateRandomTokens;
import static org.neo4j.kernel.impl.index.schema.TokenIndexUtility.generateSomeRandomUpdates;
import static org.neo4j.kernel.impl.index.schema.TokenIndexUtility.verifyUpdates;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.list.primitive.LongList;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.TokenIndexReader;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.TokenIndexEntryUpdate;

public class TokenIndexAccessorTest extends IndexAccessorTests<TokenScanKey, TokenScanValue, TokenScanLayout> {
    @Override
    IndexAccessor createAccessor(PageCache pageCache) {
        RecoveryCleanupWorkCollector cleanup = RecoveryCleanupWorkCollector.immediate();
        DatabaseIndexContext context = DatabaseIndexContext.builder(
                        pageCache, fs, contextFactory, pageCacheTracer, DEFAULT_DATABASE_NAME)
                .withReadOnlyChecker(writable())
                .build();
        return new TokenIndexAccessor(
                context,
                indexFiles,
                indexDescriptor,
                cleanup,
                Sets.immutable.empty(),
                false,
                StorageEngineIndexingBehaviour.EMPTY);
    }

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

    @Test
    void processMustThrowAfterClose() throws Exception {
        // given
        IndexUpdater updater = accessor.newUpdater(ONLINE, NULL_CONTEXT, false);
        updater.close();

        assertThrows(IllegalStateException.class, () -> updater.process(simpleUpdate()));
    }

    @Test
    void shouldAddWithUpdater() throws IndexEntryConflictException, IOException {
        // Give
        MutableLongObjectMap<int[]> entityTokens = LongObjectMaps.mutable.empty();
        List<TokenIndexEntryUpdate<?>> updates = generateSomeRandomUpdates(entityTokens, random);

        // When
        try (IndexUpdater updater = accessor.newUpdater(ONLINE, NULL_CONTEXT, false)) {
            for (TokenIndexEntryUpdate<?> update : updates) {
                updater.process(update);
            }
        }

        // Then
        forceAndCloseAccessor();
        verifyUpdates(entityTokens, layout, this::getTree, new DefaultTokenIndexIdLayout());
    }

    @Test
    void updaterShouldHandleRandomizedUpdates() throws Throwable {
        Executable additionalOperation = () -> {};
        updaterShouldHandleRandomizedUpdates(additionalOperation);
    }

    @Test
    void updaterShouldHandleRandomizedUpdatesWithRestart() throws Throwable {
        Executable additionalOperation = this::maybeRestartAccessor;
        updaterShouldHandleRandomizedUpdates(additionalOperation);
    }

    @Test
    void updaterShouldHandleRandomizedUpdatesWithCheckpoint() throws Throwable {
        Executable additionalOperation = this::maybeCheckpoint;
        updaterShouldHandleRandomizedUpdates(additionalOperation);
    }

    private void updaterShouldHandleRandomizedUpdates(Executable additionalOperation) throws Throwable {
        MutableLongObjectMap<int[]> entityTokens = LongObjectMaps.mutable.empty();
        doRandomizedUpdatesWithAdditionalOperation(additionalOperation, entityTokens);
        forceAndCloseAccessor();
        verifyUpdates(entityTokens, layout, this::getTree, new DefaultTokenIndexIdLayout());
    }

    @Test
    void newValueReaderShouldThrow() {
        assertThatThrownBy(() -> accessor.newValueReader(NO_USAGE_TRACKER))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void readerShouldFindNothingOnEmptyIndex() throws Exception {
        assertReaderFindsExpected(1, LongLists.immutable.empty());
    }

    @Test
    void readerShouldFindNothingBecauseNoMatchingToken() throws Exception {
        // Given
        int tokenId = 1;
        int otherTokenId = 2;
        addToIndex(otherTokenId, 1);

        // When
        assertReaderFindsExpected(tokenId, LongLists.immutable.empty());
    }

    @Test
    void readerShouldFindSingleWithNoOtherTokens() throws Exception {
        // Given
        int tokenId = 1;
        long entityId = 1;
        addToIndex(tokenId, entityId);
        LongList expectedIds = LongLists.immutable.of(entityId);

        // When
        assertReaderFindsExpected(tokenId, expectedIds);
    }

    @Test
    void readerShouldFindSingleWithOtherTokens() throws Exception {
        // Given
        int tokenId = 1;
        long entityId = 1;
        addToIndex(tokenId, entityId);
        addToIndex(2, 2);
        LongList expectedIds = LongLists.immutable.of(entityId);

        // When
        assertReaderFindsExpected(tokenId, expectedIds);
    }

    @ParameterizedTest
    @EnumSource(IndexOrder.class)
    void readerShouldFindManyWithNoOtherTokens(IndexOrder indexOrder) throws Exception {
        // Given
        int tokenId = 1;
        long[] entityIds = new long[] {1, 2, 3, 64, 65, 1000, 2000};
        addToIndex(tokenId, entityIds);
        LongList expectedIds = LongLists.immutable.of(entityIds);

        // When
        assertReaderFindsExpected(indexOrder, tokenId, expectedIds);
    }

    @ParameterizedTest
    @EnumSource(IndexOrder.class)
    void readerShouldFindManyWithOtherTokens(IndexOrder indexOrder) throws Exception {
        // Given
        int tokenId = 1;
        long[] entityIds = new long[] {1, 2, 3, 64, 65, 1000, 2001};
        long[] otherEntityIds = new long[] {1, 2, 4, 64, 66, 1000, 2000};
        addToIndex(tokenId, entityIds);
        addToIndex(2, otherEntityIds);
        LongList expectedIds = LongLists.immutable.of(entityIds);

        // When
        assertReaderFindsExpected(indexOrder, tokenId, expectedIds);
    }

    @ParameterizedTest
    @MethodSource("orderCombinations")
    void readerShouldHandleNestedQueries(IndexOrder outerOrder, IndexOrder innerOrder) throws Exception {
        int outerTokenId = 1;
        int innerTokenId = 2;
        long[] outerIds = new long[] {1, 2, 3, 64, 65, 1000, 2001};
        long[] innerIds = new long[] {1, 2, 4, 64, 66, 1000, 2000};
        LongList outerExpectedIds = LongLists.immutable.of(outerIds);
        LongList innerExpectedIds = LongLists.immutable.of(innerIds);
        addToIndex(outerTokenId, outerIds);
        addToIndex(innerTokenId, innerIds);

        try (var reader = accessor.newTokenReader(NO_USAGE_TRACKER)) {
            assertReaderFindsExpected(
                    reader,
                    outerOrder,
                    outerTokenId,
                    outerExpectedIds,
                    indexReader -> assertReaderFindsExpected(
                            indexReader, innerOrder, innerTokenId, innerExpectedIds, ThrowingConsumer.noop()));
        }
    }

    @Test
    void readerShouldFindRandomizedUpdates() throws Throwable {
        Executable additionalOperation = () -> {};
        readerShouldFindRandomizedUpdates(additionalOperation);
    }

    @Test
    void readerShouldFindRandomizedUpdatesWithCheckpoint() throws Throwable {
        Executable additionalOperation = this::maybeCheckpoint;
        readerShouldFindRandomizedUpdates(additionalOperation);
    }

    @Test
    void readerShouldFindRandomizedUpdatesWithRestart() throws Throwable {
        Executable additionalOperation = this::maybeRestartAccessor;
        readerShouldFindRandomizedUpdates(additionalOperation);
    }

    @Test
    void readingAfterDropShouldThrow() {
        // given
        accessor.drop();

        assertThrows(IllegalStateException.class, () -> accessor.newTokenReader(NO_USAGE_TRACKER));
    }

    @Test
    void readingAfterCloseShouldThrow() {
        // given
        accessor.close();

        assertThrows(IllegalStateException.class, () -> accessor.newTokenReader(NO_USAGE_TRACKER));
    }

    @Test
    void shouldScanSingleRange() throws IndexEntryConflictException {
        // GIVEN
        int labelId1 = 1;
        int labelId2 = 2;
        long nodeId1 = 10;
        long nodeId2 = 11;
        try (IndexUpdater indexUpdater = accessor.newUpdater(ONLINE, NULL_CONTEXT, false)) {
            indexUpdater.process(
                    IndexEntryUpdate.change(nodeId1, indexDescriptor, EMPTY_INT_ARRAY, new int[] {labelId1}));
            indexUpdater.process(
                    IndexEntryUpdate.change(nodeId2, indexDescriptor, EMPTY_INT_ARRAY, new int[] {labelId1, labelId2}));
        }

        // WHEN
        BoundedIterable<EntityTokenRange> reader =
                accessor.newAllEntriesTokenReader(Long.MIN_VALUE, Long.MAX_VALUE, NULL_CONTEXT);
        EntityTokenRange range = single(reader.iterator());

        // THEN
        assertThat(new long[] {nodeId1, nodeId2}).isEqualTo(reducedNodes(range));
        assertThat(new int[] {labelId1}).isEqualTo(sorted(range.tokens(nodeId1)));
        assertThat(new int[] {labelId1, labelId2}).isEqualTo(sorted(range.tokens(nodeId2)));
    }

    @Test
    void shouldScanMultipleRanges() throws IndexEntryConflictException {
        // GIVEN
        int labelId1 = 1;
        int labelId2 = 2;
        long nodeId1 = 10;
        long nodeId2 = 1280;
        try (IndexUpdater indexUpdater = accessor.newUpdater(ONLINE, NULL_CONTEXT, false)) {
            indexUpdater.process(
                    IndexEntryUpdate.change(nodeId1, indexDescriptor, EMPTY_INT_ARRAY, new int[] {labelId1}));
            indexUpdater.process(
                    IndexEntryUpdate.change(nodeId2, indexDescriptor, EMPTY_INT_ARRAY, new int[] {labelId1, labelId2}));
        }

        // WHEN
        BoundedIterable<EntityTokenRange> reader =
                accessor.newAllEntriesTokenReader(Long.MIN_VALUE, Long.MAX_VALUE, NULL_CONTEXT);
        Iterator<EntityTokenRange> iterator = reader.iterator();
        EntityTokenRange range1 = iterator.next();
        EntityTokenRange range2 = iterator.next();
        assertFalse(iterator.hasNext());

        // THEN
        assertThat(new long[] {nodeId1}).isEqualTo(reducedNodes(range1));
        assertThat(new long[] {nodeId2}).isEqualTo(reducedNodes(range2));

        assertThat(new int[] {labelId1}).isEqualTo(sorted(range1.tokens(nodeId1)));
        assertThat(new int[] {labelId1, labelId2}).isEqualTo(sorted(range2.tokens(nodeId2)));
    }

    private static int[] sorted(int[] input) {
        Arrays.sort(input);
        return input;
    }

    private static long[] reducedNodes(EntityTokenRange range) {
        long[] nodes = range.entities();
        long[] result = new long[nodes.length];
        int cursor = 0;
        for (long node : nodes) {
            if (range.tokens(node).length > 0) {
                result[cursor++] = node;
            }
        }
        return Arrays.copyOf(result, cursor);
    }

    private void readerShouldFindRandomizedUpdates(Executable additionalOperation) throws Throwable {
        MutableLongObjectMap<int[]> entityTokens = LongObjectMaps.mutable.empty();
        doRandomizedUpdatesWithAdditionalOperation(additionalOperation, entityTokens);
        verifyReaderSeesAllUpdates(convertToExpectedEntitiesPerToken(entityTokens));
    }

    private void doRandomizedUpdatesWithAdditionalOperation(
            Executable additionalOperation, MutableLongObjectMap<int[]> trackingStructure) throws Throwable {
        int numberOfEntities = 1_000;
        long currentMaxEntityId = 0;

        while (currentMaxEntityId < numberOfEntities) {
            try (IndexUpdater updater = accessor.newUpdater(ONLINE, NULL_CONTEXT, false)) {
                // Simply add random token ids to a new batch of 100 entities
                for (int i = 0; i < 100; i++) {
                    int[] afterTokens = generateRandomTokens(random);
                    if (afterTokens.length != 0) {
                        trackingStructure.put(currentMaxEntityId, Arrays.copyOf(afterTokens, afterTokens.length));
                        updater.process(
                                IndexEntryUpdate.change(currentMaxEntityId, null, EMPTY_INT_ARRAY, afterTokens));
                    }
                    currentMaxEntityId++;
                }
            }
            additionalOperation.execute();
            // Interleave updates in id range lower than currentMaxEntityId
            try (IndexUpdater updater = accessor.newUpdater(ONLINE, NULL_CONTEXT, false)) {
                for (int i = 0; i < 100; i++) {
                    long entityId = random.nextLong(currentMaxEntityId);
                    // Current tokens for the entity in the tree
                    int[] beforeTokens = trackingStructure.get(entityId);
                    if (beforeTokens == null) {
                        beforeTokens = EMPTY_INT_ARRAY;
                    }
                    int[] afterTokens = generateRandomTokens(random);
                    trackingStructure.put(entityId, Arrays.copyOf(afterTokens, afterTokens.length));
                    updater.process(IndexEntryUpdate.change(entityId, null, beforeTokens, afterTokens));
                }
            }
            additionalOperation.execute();
        }
    }

    private static MutableLongObjectMap<MutableLongList> convertToExpectedEntitiesPerToken(
            MutableLongObjectMap<int[]> trackingStructure) {
        MutableLongObjectMap<MutableLongList> entitiesPerToken = LongObjectMaps.mutable.empty();
        trackingStructure.forEachKeyValue((entityId, tokens) -> {
            for (long token : tokens) {
                MutableLongList entities = entitiesPerToken.getIfAbsentPut(token, LongLists.mutable.empty());
                entities.add(entityId);
            }
        });
        return entitiesPerToken;
    }

    private void addToIndex(int tokenId, long... entityIds) throws IndexEntryConflictException {
        try (IndexUpdater updater = accessor.newUpdater(ONLINE, NULL_CONTEXT, false)) {
            for (long entityId : entityIds) {
                updater.process(
                        IndexEntryUpdate.change(entityId, indexDescriptor, EMPTY_INT_ARRAY, new int[] {tokenId}));
            }
        }
    }

    private void verifyReaderSeesAllUpdates(MutableLongObjectMap<MutableLongList> entitiesPerToken) throws Exception {
        for (long token : TOKENS) {
            MutableLongList expectedEntities = entitiesPerToken.getIfAbsent(token, LongLists.mutable::empty);
            assertReaderFindsExpected(token, expectedEntities);
        }
    }

    private void assertReaderFindsExpected(long tokenId, LongList expectedIds) throws Exception {
        assertReaderFindsExpected(IndexOrder.NONE, tokenId, expectedIds);
    }

    private void assertReaderFindsExpected(IndexOrder indexOrder, long tokenId, LongList expectedIds) throws Exception {
        try (var indexReader = accessor.newTokenReader(NO_USAGE_TRACKER)) {
            assertReaderFindsExpected(indexReader, indexOrder, tokenId, expectedIds, ThrowingConsumer.noop());
        }
    }

    private static void assertReaderFindsExpected(
            TokenIndexReader reader,
            IndexOrder indexOrder,
            long tokenId,
            LongList expectedIds,
            ThrowingConsumer<TokenIndexReader, Exception> innerCalling)
            throws Exception {
        if (indexOrder.equals(IndexOrder.DESCENDING)) {
            expectedIds = expectedIds.toReversed();
        }
        try (CollectingEntityTokenClient collectingEntityTokenClient = new CollectingEntityTokenClient(tokenId)) {
            IndexQueryConstraints constraint = IndexQueryConstraints.constrained(indexOrder, false);
            TokenPredicate query = new TokenPredicate((int) tokenId);
            reader.query(collectingEntityTokenClient, constraint, query, NULL_CONTEXT);

            // Then
            int count = 0;
            while (collectingEntityTokenClient.next()) {
                innerCalling.accept(reader);
                count++;
            }
            assertThat(count).isEqualTo(expectedIds.size());
            assertThat(collectingEntityTokenClient.actualIds).isEqualTo(expectedIds);
        }
    }

    private void maybeRestartAccessor() throws IOException {
        if (random.nextDouble() < 0.1) {
            forceAndCloseAccessor();
            setupAccessor();
        }
    }

    private void maybeCheckpoint() {
        if (random.nextBoolean()) {
            accessor.force(FileFlushEvent.NULL, NULL_CONTEXT);
        }
    }

    private void forceAndCloseAccessor() {
        accessor.force(FileFlushEvent.NULL, NULL_CONTEXT);
        accessor.close();
    }

    private TokenIndexEntryUpdate<IndexDescriptor> simpleUpdate() {
        return TokenIndexEntryUpdate.change(0, indexDescriptor, EMPTY_INT_ARRAY, new int[] {0});
    }

    private static Stream<Arguments> orderCombinations() {
        List<Arguments> arguments = new ArrayList<>();
        for (IndexOrder outer : IndexOrder.values()) {
            for (IndexOrder inner : IndexOrder.values()) {
                arguments.add(Arguments.of(outer, inner));
            }
        }
        return arguments.stream();
    }

    private static class CollectingEntityTokenClient implements IndexProgressor.EntityTokenClient, Closeable {
        private final long expectedToken;
        private final MutableLongList actualIds = LongLists.mutable.empty();
        private IndexProgressor progressor;

        private CollectingEntityTokenClient(long expectedToken) {
            this.expectedToken = expectedToken;
        }

        @Override
        public void initialize(IndexProgressor progressor, int token, IndexOrder order) {
            assertThat(token).isEqualTo(expectedToken);
            this.progressor = progressor;
        }

        @Override
        public void initialize(IndexProgressor progressor, int token, LongIterator added, LongSet removed) {
            throw new UnsupportedOperationException("Did not expect to use this method");
        }

        @Override
        public boolean acceptEntity(long reference, int tokenId) {
            actualIds.add(reference);
            return true;
        }

        boolean next() {
            return progressor.next();
        }

        @Override
        public void close() {
            if (progressor != null) {
                progressor.close();
            }
        }
    }
}
