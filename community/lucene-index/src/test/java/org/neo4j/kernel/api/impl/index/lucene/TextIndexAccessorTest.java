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
package org.neo4j.kernel.api.impl.index.lucene;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.neo4j.collection.PrimitiveLongCollections.toSet;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.allEntries;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.exact;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.exists;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.range;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.io.IOUtils.closeAll;
import static org.neo4j.kernel.api.impl.schema.AbstractTextIndexProvider.UPDATE_IGNORE_STRATEGY;
import static org.neo4j.kernel.api.impl.schema.LuceneTestTokenNameLookup.SIMPLE_TOKEN_LOOKUP;
import static org.neo4j.kernel.impl.index.schema.IndexUsageTracking.NO_USAGE_TRACKING;
import static org.neo4j.test.extension.Threading.waitingWhileIn;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.configuration.Config;
import org.neo4j.function.IOFunction;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.ExistsPredicate;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.RangePredicate;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.AllIndexProviderDescriptors;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.schema.TaskCoordinator;
import org.neo4j.kernel.api.impl.schema.text.TextIndexAccessor;
import org.neo4j.kernel.api.impl.schema.text.TextIndexBuilder;
import org.neo4j.kernel.api.impl.schema.text.TextIndexProvider;
import org.neo4j.kernel.api.index.IndexQueryHelper;
import org.neo4j.kernel.api.index.IndexSampler;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.index.schema.NodeValueIterator;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Threading;
import org.neo4j.test.extension.ThreadingExtension;
import org.neo4j.util.concurrent.BinaryLatch;
import org.neo4j.values.ElementIdMapper;

@ExtendWith(ThreadingExtension.class)
public class TextIndexAccessorTest {
    private static final int PROP_ID = 1;
    private static final long NODE_ID = 1;
    private static final long NODE_ID_2 = 2;
    private static final Object VALUE = "value";
    private static final Object VALUE_2 = "40";
    private static final IndexDescriptor GENERAL_INDEX = IndexPrototype.forSchema(forLabel(0, PROP_ID))
            .withName("a")
            .withIndexType(IndexType.TEXT)
            .withIndexProvider(AllIndexProviderDescriptors.TEXT_V1_DESCRIPTOR)
            .materialise(0)
            .withIndexCapability(TextIndexProvider.CAPABILITY);
    private static final Config CONFIG = Config.defaults();

    private static final EphemeralFileSystemAbstraction FILE_SYSTEM = new EphemeralFileSystemAbstraction();

    @Inject
    private Threading threading;

    private DirectoryFactory dirFactory;
    private TextIndexAccessor accessor;

    public static Stream<Arguments> implementations() {
        Path dir = Path.of("dir");
        return Stream.of(Arguments.of(GENERAL_INDEX, (IOFunction<DirectoryFactory, TextIndexAccessor>) dirFactory1 -> {
            DatabaseIndex<ValueIndexReader> index = TextIndexBuilder.create(
                            GENERAL_INDEX, writable(), CONFIG, NullLogProvider.getInstance())
                    .withFileSystem(FILE_SYSTEM)
                    .withDirectoryFactory(dirFactory1)
                    .withIndexRootFolder(dir.resolve("1"))
                    .build();

            index.create();
            index.open();
            return new TextIndexAccessor(
                    index, GENERAL_INDEX, SIMPLE_TOKEN_LOOKUP, ElementIdMapper.PLACEHOLDER, UPDATE_IGNORE_STRATEGY);
        }));
    }

    @AfterAll
    static void afterAll() throws IOException {
        FILE_SYSTEM.close();
    }

    void init(LuceneContext luceneContext) throws IOException {
        dirFactory = DirectoryFactory.inMemory(luceneContext);
        Path dir = Path.of("dir");
        DatabaseIndex<ValueIndexReader> databaseIndex = TextIndexBuilder.create(
                        GENERAL_INDEX, writable(), CONFIG, NullLogProvider.getInstance())
                .withFileSystem(FILE_SYSTEM)
                .withDirectoryFactory(dirFactory)
                .withLuceneContext(luceneContext)
                .withIndexRootFolder(dir.resolve("1"))
                .build();

        databaseIndex.create();
        databaseIndex.open();
        accessor = new TextIndexAccessor(
                databaseIndex, GENERAL_INDEX, SIMPLE_TOKEN_LOOKUP, ElementIdMapper.PLACEHOLDER, UPDATE_IGNORE_STRATEGY);
    }

    @AfterEach
    void after() throws IOException {
        closeAll(accessor, dirFactory);
    }

    @ParameterizedTest
    @EnumSource
    void indexReaderShouldSupportScan(LuceneContext luceneContext) throws Exception {
        init(luceneContext);
        // GIVEN
        updateAndCommit(asList(add(NODE_ID, VALUE), add(NODE_ID_2, VALUE_2)));
        ValueIndexReader reader = accessor.newValueReader(NO_USAGE_TRACKING);

        // WHEN
        Set<Long> results = resultSet(reader, allEntries());

        // THEN
        assertEquals(asSet(NODE_ID, NODE_ID_2), results);
        reader.close();
    }

    @ParameterizedTest
    @EnumSource
    void indexReaderExistsQuery(LuceneContext luceneContext) throws Exception {
        init(luceneContext);
        // GIVEN
        try (ValueIndexReader reader = accessor.newValueReader(NO_USAGE_TRACKING)) {
            // WHEN
            ExistsPredicate query = exists(PROP_ID);
            assertThatThrownBy(() -> resultsArray(reader, query))
                    .isInstanceOf(IndexNotApplicableKernelException.class)
                    .hasMessageContainingAll(
                            "Index query not supported for", IndexType.TEXT.name(), "index", "Query", query.toString());
        }
    }

    @ParameterizedTest
    @EnumSource
    void indexReaderExactQuery(LuceneContext luceneContext) throws Exception {
        init(luceneContext);
        // GIVEN
        updateAndCommit(asList(add(NODE_ID, VALUE), add(NODE_ID_2, VALUE_2)));
        ValueIndexReader reader = accessor.newValueReader(NO_USAGE_TRACKING);

        // WHEN
        Set<Long> results = resultSet(reader, exact(PROP_ID, VALUE));

        // THEN
        assertEquals(asSet(NODE_ID), results);
        reader.close();
    }

    @ParameterizedTest
    @EnumSource
    void indexStringRangeQuery(LuceneContext luceneContext) throws Exception {
        init(luceneContext);

        // GIVEN
        try (ValueIndexReader reader = accessor.newValueReader(NO_USAGE_TRACKING)) {
            // WHEN
            ExistsPredicate query = exists(PROP_ID);
            assertThatThrownBy(() -> resultsArray(reader, query))
                    .isInstanceOf(IndexNotApplicableKernelException.class)
                    .hasMessageContainingAll(
                            "Index query not supported for", IndexType.TEXT.name(), "index", "Query", query.toString());
        }
    }

    @ParameterizedTest
    @EnumSource
    void indexNumberRangeQueryMustThrow(LuceneContext luceneContext) throws Exception {
        init(luceneContext);

        updateAndCommit(asList(add(1, "1"), add(2, "2"), add(3, "3"), add(4, "4"), add(5, "Double.NaN")));

        ValueIndexReader reader = accessor.newValueReader(NO_USAGE_TRACKING);
        RangePredicate<?> query = range(PROP_ID, 2, true, 3, true);
        assertThatThrownBy(() -> resultsArray(reader, query))
                .isInstanceOf(IndexNotApplicableKernelException.class)
                .hasMessageContainingAll(
                        "Index query not supported for", IndexType.TEXT.name(), "index", "Query", query.toString());
    }

    @ParameterizedTest
    @EnumSource
    void indexReaderShouldHonorRepeatableReads(LuceneContext luceneContext) throws Exception {
        init(luceneContext);

        // GIVEN
        updateAndCommit(singletonList(add(NODE_ID, VALUE)));
        ValueIndexReader reader = accessor.newValueReader(NO_USAGE_TRACKING);

        // WHEN
        updateAndCommit(singletonList(remove(NODE_ID, VALUE)));

        // THEN
        assertEquals(asSet(NODE_ID), resultSet(reader, exact(PROP_ID, VALUE)));
        reader.close();
    }

    @ParameterizedTest
    @EnumSource
    void multipleIndexReadersFromDifferentPointsInTimeCanSeeDifferentResults(LuceneContext luceneContext)
            throws Exception {
        init(luceneContext);

        // WHEN
        updateAndCommit(singletonList(add(NODE_ID, VALUE)));
        ValueIndexReader firstReader = accessor.newValueReader(NO_USAGE_TRACKING);
        updateAndCommit(singletonList(add(NODE_ID_2, VALUE_2)));
        ValueIndexReader secondReader = accessor.newValueReader(NO_USAGE_TRACKING);

        // THEN
        assertEquals(asSet(NODE_ID), resultSet(firstReader, exact(PROP_ID, VALUE)));
        assertEquals(asSet(), resultSet(firstReader, exact(PROP_ID, VALUE_2)));
        assertEquals(asSet(NODE_ID), resultSet(secondReader, exact(PROP_ID, VALUE)));
        assertEquals(asSet(NODE_ID_2), resultSet(secondReader, exact(PROP_ID, VALUE_2)));
        firstReader.close();
        secondReader.close();
    }

    @ParameterizedTest
    @EnumSource
    void canAddNewData(LuceneContext luceneContext) throws Exception {
        init(luceneContext);

        // WHEN
        updateAndCommit(asList(add(NODE_ID, VALUE), add(NODE_ID_2, VALUE_2)));
        ValueIndexReader reader = accessor.newValueReader(NO_USAGE_TRACKING);

        // THEN
        assertEquals(asSet(NODE_ID), resultSet(reader, exact(PROP_ID, VALUE)));
        reader.close();
    }

    @ParameterizedTest
    @EnumSource
    void canChangeExistingData(LuceneContext luceneContext) throws Exception {
        init(luceneContext);

        // GIVEN
        updateAndCommit(singletonList(add(NODE_ID, VALUE)));

        // WHEN
        updateAndCommit(singletonList(change(NODE_ID, VALUE, VALUE_2)));
        ValueIndexReader reader = accessor.newValueReader(NO_USAGE_TRACKING);

        // THEN
        assertEquals(asSet(NODE_ID), resultSet(reader, exact(PROP_ID, VALUE_2)));
        assertEquals(emptySet(), resultSet(reader, exact(PROP_ID, VALUE)));
        reader.close();
    }

    @ParameterizedTest
    @EnumSource
    void canRemoveExistingData(LuceneContext luceneContext) throws Exception {
        init(luceneContext);

        // GIVEN
        updateAndCommit(asList(add(NODE_ID, VALUE), add(NODE_ID_2, VALUE_2)));

        // WHEN
        updateAndCommit(singletonList(remove(NODE_ID, VALUE)));
        ValueIndexReader reader = accessor.newValueReader(NO_USAGE_TRACKING);

        // THEN
        assertEquals(asSet(NODE_ID_2), resultSet(reader, exact(PROP_ID, VALUE_2)));
        assertEquals(asSet(), resultSet(reader, exact(PROP_ID, VALUE)));
        reader.close();
    }

    @ParameterizedTest
    @EnumSource
    void shouldStopSamplingWhenIndexIsDropped(LuceneContext luceneContext) throws Exception {
        init(luceneContext);

        // given
        updateAndCommit(asList(add(NODE_ID, VALUE), add(NODE_ID_2, VALUE_2)));

        // when
        ValueIndexReader indexReader = accessor.newValueReader(NO_USAGE_TRACKING);
        BinaryLatch dropLatch = new BinaryLatch();
        BinaryLatch sampleLatch = new BinaryLatch();

        LuceneIndexSampler indexSampler = spy((LuceneIndexSampler) indexReader.createSampler());
        doAnswer(inv -> {
                    Object obj = inv.callRealMethod();
                    dropLatch.release(); // We have now started the sampling, let the index try to drop
                    sampleLatch.await(); // Wait for the drop to be blocked
                    return obj;
                })
                .when(indexSampler)
                .newTask();

        List<Future<?>> futures = new ArrayList<>();
        try (ValueIndexReader reader = indexReader /* do not inline! */;
                IndexSampler sampler = indexSampler /* do not inline! */) {
            futures.add(threading.execute(
                    nothing -> {
                        try {
                            assertThatThrownBy(() ->
                                            indexSampler.sampleIndex(CursorContext.NULL_CONTEXT, new AtomicBoolean()))
                                    .isInstanceOf(IndexNotFoundKernelException.class)
                                    .hasMessage("Index dropped while sampling.");
                        } finally {
                            dropLatch.release(); // if something goes wrong we do not want to block the drop
                        }
                        return nothing;
                    },
                    null));

            futures.add(threading.executeAndAwait(
                    nothing -> {
                        dropLatch.await(); // need to wait for the sampling to start before we drop
                        accessor.drop();
                        return nothing;
                    },
                    null,
                    waitingWhileIn(TaskCoordinator.class, "awaitCompletion"),
                    10,
                    MINUTES));

        } finally {
            sampleLatch.release(); // drop is blocked, okay to finish sampling (will fail since index is dropped)
            for (Future<?> future : futures) {
                future.get();
            }
        }
    }

    private static Set<Long> resultSet(ValueIndexReader reader, PropertyIndexQuery... queries)
            throws IndexNotApplicableKernelException {
        return toSet(results(reader, queries));
    }

    private static NodeValueIterator results(ValueIndexReader reader, PropertyIndexQuery... queries)
            throws IndexNotApplicableKernelException {
        NodeValueIterator results = new NodeValueIterator();
        reader.query(results, QueryContext.NULL_CONTEXT, CursorContext.NULL_CONTEXT, unconstrained(), queries);
        return results;
    }

    private static long[] resultsArray(ValueIndexReader reader, PropertyIndexQuery... queries)
            throws IndexNotApplicableKernelException {
        try (NodeValueIterator iterator = results(reader, queries)) {
            return PrimitiveLongCollections.asArray(iterator);
        }
    }

    private static IndexEntryUpdate add(long nodeId, Object value) {
        return IndexQueryHelper.add(nodeId, GENERAL_INDEX, value);
    }

    private static IndexEntryUpdate remove(long nodeId, Object value) {
        return IndexQueryHelper.remove(nodeId, GENERAL_INDEX, value);
    }

    private static IndexEntryUpdate change(long nodeId, Object valueBefore, Object valueAfter) {
        return IndexQueryHelper.change(nodeId, GENERAL_INDEX, valueBefore, valueAfter);
    }

    private void updateAndCommit(List<IndexEntryUpdate> nodePropertyUpdates) throws IndexEntryConflictException {
        try (IndexUpdater updater = accessor.newUpdater(IndexUpdateMode.ONLINE, CursorContext.NULL_CONTEXT, false)) {
            for (IndexEntryUpdate update : nodePropertyUpdates) {
                updater.process(update);
            }
        }
    }
}
