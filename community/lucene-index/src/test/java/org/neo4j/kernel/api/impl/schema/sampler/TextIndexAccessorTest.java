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
package org.neo4j.kernel.api.impl.schema.sampler;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.configuration.Config;
import org.neo4j.function.IOFunction;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.schema.TaskCoordinator;
import org.neo4j.kernel.api.impl.schema.TextIndexAccessor;
import org.neo4j.kernel.api.impl.schema.TextIndexBuilder;
import org.neo4j.kernel.api.impl.schema.TextIndexProvider;
import org.neo4j.kernel.api.index.IndexQueryHelper;
import org.neo4j.kernel.api.index.IndexSampler;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.index.schema.NodeValueIterator;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Threading;
import org.neo4j.test.extension.ThreadingExtension;
import org.neo4j.util.concurrent.BinaryLatch;

@ExtendWith(ThreadingExtension.class)
public class TextIndexAccessorTest {
    private static final int PROP_ID = 1;

    @Inject
    private Threading threading;

    private static EphemeralFileSystemAbstraction fileSystem;

    private IndexDescriptor index;
    private TextIndexAccessor accessor;
    private final long nodeId = 1;
    private final long nodeId2 = 2;
    private final Object value = "value";
    private final Object value2 = "40";
    private DirectoryFactory.InMemoryDirectoryFactory dirFactory;
    private static final IndexDescriptor GENERAL_INDEX = IndexPrototype.forSchema(forLabel(0, PROP_ID))
            .withName("a")
            .withIndexType(IndexType.TEXT)
            .withIndexProvider(TextIndexProvider.DESCRIPTOR)
            .materialise(0)
            .withIndexCapability(TextIndexProvider.CAPABILITY);
    private static final Config CONFIG = Config.defaults();

    public static Stream<Arguments> implementations() {
        final Path dir = Path.of("dir");
        return Stream.of(Arguments.of(GENERAL_INDEX, (IOFunction<DirectoryFactory, TextIndexAccessor>) dirFactory1 -> {
            var index = TextIndexBuilder.create(GENERAL_INDEX, writable(), CONFIG)
                    .withFileSystem(fileSystem)
                    .withDirectoryFactory(dirFactory1)
                    .withIndexRootFolder(dir.resolve("1"))
                    .build();

            index.create();
            index.open();
            return new TextIndexAccessor(index, GENERAL_INDEX, SIMPLE_TOKEN_LOOKUP, UPDATE_IGNORE_STRATEGY);
        }));
    }

    @BeforeAll
    static void beforeAll() {
        fileSystem = new EphemeralFileSystemAbstraction();
    }

    @AfterAll
    static void afterAll() throws IOException {
        fileSystem.close();
    }

    void init(IndexDescriptor index, IOFunction<DirectoryFactory, TextIndexAccessor> accessorFactory)
            throws IOException {
        this.index = index;
        dirFactory = new DirectoryFactory.InMemoryDirectoryFactory();
        accessor = accessorFactory.apply(dirFactory);
    }

    @AfterEach
    void after() throws IOException {
        closeAll(accessor, dirFactory);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("implementations")
    void indexReaderShouldSupportScan(
            IndexDescriptor index, IOFunction<DirectoryFactory, TextIndexAccessor> accessorFactory) throws Exception {
        init(index, accessorFactory);
        // GIVEN
        updateAndCommit(asList(add(nodeId, value), add(nodeId2, value2)));
        var reader = accessor.newValueReader(NO_USAGE_TRACKING);

        // WHEN
        Set<Long> results = resultSet(reader, allEntries());

        // THEN
        assertEquals(asSet(nodeId, nodeId2), results);
        reader.close();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("implementations")
    void indexReaderExistsQuery(IndexDescriptor index, IOFunction<DirectoryFactory, TextIndexAccessor> accessorFactory)
            throws Exception {
        init(index, accessorFactory);
        // GIVEN
        try (var reader = accessor.newValueReader(NO_USAGE_TRACKING)) {
            // WHEN
            var query = exists(PROP_ID);
            assertThatThrownBy(() -> resultsArray(reader, query))
                    .isInstanceOf(IndexNotApplicableKernelException.class)
                    .hasMessageContainingAll(
                            "Index query not supported for", IndexType.TEXT.name(), "index", "Query", query.toString());
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("implementations")
    void indexReaderExactQuery(IndexDescriptor index, IOFunction<DirectoryFactory, TextIndexAccessor> accessorFactory)
            throws Exception {
        init(index, accessorFactory);
        // GIVEN
        updateAndCommit(asList(add(nodeId, value), add(nodeId2, value2)));
        var reader = accessor.newValueReader(NO_USAGE_TRACKING);

        // WHEN
        Set<Long> results = resultSet(reader, exact(PROP_ID, value));

        // THEN
        assertEquals(asSet(nodeId), results);
        reader.close();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("implementations")
    void indexStringRangeQuery(IndexDescriptor index, IOFunction<DirectoryFactory, TextIndexAccessor> accessorFactory)
            throws Exception {
        init(index, accessorFactory);

        // GIVEN
        try (var reader = accessor.newValueReader(NO_USAGE_TRACKING)) {
            // WHEN
            var query = exists(PROP_ID);
            assertThatThrownBy(() -> resultsArray(reader, query))
                    .isInstanceOf(IndexNotApplicableKernelException.class)
                    .hasMessageContainingAll(
                            "Index query not supported for", IndexType.TEXT.name(), "index", "Query", query.toString());
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("implementations")
    void indexNumberRangeQueryMustThrow(
            IndexDescriptor index, IOFunction<DirectoryFactory, TextIndexAccessor> accessorFactory) throws Exception {
        init(index, accessorFactory);

        updateAndCommit(asList(add(1, "1"), add(2, "2"), add(3, "3"), add(4, "4"), add(5, "Double.NaN")));

        var reader = accessor.newValueReader(NO_USAGE_TRACKING);
        var query = range(PROP_ID, 2, true, 3, true);
        assertThatThrownBy(() -> resultsArray(reader, query))
                .isInstanceOf(IndexNotApplicableKernelException.class)
                .hasMessageContainingAll(
                        "Index query not supported for", IndexType.TEXT.name(), "index", "Query", query.toString());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("implementations")
    void indexReaderShouldHonorRepeatableReads(
            IndexDescriptor index, IOFunction<DirectoryFactory, TextIndexAccessor> accessorFactory) throws Exception {
        init(index, accessorFactory);

        // GIVEN
        updateAndCommit(singletonList(add(nodeId, value)));
        var reader = accessor.newValueReader(NO_USAGE_TRACKING);

        // WHEN
        updateAndCommit(singletonList(remove(nodeId, value)));

        // THEN
        assertEquals(asSet(nodeId), resultSet(reader, exact(PROP_ID, value)));
        reader.close();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("implementations")
    void multipleIndexReadersFromDifferentPointsInTimeCanSeeDifferentResults(
            IndexDescriptor index, IOFunction<DirectoryFactory, TextIndexAccessor> accessorFactory) throws Exception {
        init(index, accessorFactory);

        // WHEN
        updateAndCommit(singletonList(add(nodeId, value)));
        var firstReader = accessor.newValueReader(NO_USAGE_TRACKING);
        updateAndCommit(singletonList(add(nodeId2, value2)));
        var secondReader = accessor.newValueReader(NO_USAGE_TRACKING);

        // THEN
        assertEquals(asSet(nodeId), resultSet(firstReader, exact(PROP_ID, value)));
        assertEquals(asSet(), resultSet(firstReader, exact(PROP_ID, value2)));
        assertEquals(asSet(nodeId), resultSet(secondReader, exact(PROP_ID, value)));
        assertEquals(asSet(nodeId2), resultSet(secondReader, exact(PROP_ID, value2)));
        firstReader.close();
        secondReader.close();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("implementations")
    void canAddNewData(IndexDescriptor index, IOFunction<DirectoryFactory, TextIndexAccessor> accessorFactory)
            throws Exception {
        init(index, accessorFactory);

        // WHEN
        updateAndCommit(asList(add(nodeId, value), add(nodeId2, value2)));
        var reader = accessor.newValueReader(NO_USAGE_TRACKING);

        // THEN
        assertEquals(asSet(nodeId), resultSet(reader, exact(PROP_ID, value)));
        reader.close();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("implementations")
    void canChangeExistingData(IndexDescriptor index, IOFunction<DirectoryFactory, TextIndexAccessor> accessorFactory)
            throws Exception {
        init(index, accessorFactory);

        // GIVEN
        updateAndCommit(singletonList(add(nodeId, value)));

        // WHEN
        updateAndCommit(singletonList(change(nodeId, value, value2)));
        var reader = accessor.newValueReader(NO_USAGE_TRACKING);

        // THEN
        assertEquals(asSet(nodeId), resultSet(reader, exact(PROP_ID, value2)));
        assertEquals(emptySet(), resultSet(reader, exact(PROP_ID, value)));
        reader.close();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("implementations")
    void canRemoveExistingData(IndexDescriptor index, IOFunction<DirectoryFactory, TextIndexAccessor> accessorFactory)
            throws Exception {
        init(index, accessorFactory);

        // GIVEN
        updateAndCommit(asList(add(nodeId, value), add(nodeId2, value2)));

        // WHEN
        updateAndCommit(singletonList(remove(nodeId, value)));
        var reader = accessor.newValueReader(NO_USAGE_TRACKING);

        // THEN
        assertEquals(asSet(nodeId2), resultSet(reader, exact(PROP_ID, value2)));
        assertEquals(asSet(), resultSet(reader, exact(PROP_ID, value)));
        reader.close();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("implementations")
    void shouldStopSamplingWhenIndexIsDropped(
            IndexDescriptor index, IOFunction<DirectoryFactory, TextIndexAccessor> accessorFactory) throws Exception {
        init(index, accessorFactory);

        // given
        updateAndCommit(asList(add(nodeId, value), add(nodeId2, value2)));

        // when
        var indexReader = accessor.newValueReader(NO_USAGE_TRACKING);
        BinaryLatch dropLatch = new BinaryLatch();
        BinaryLatch sampleLatch = new BinaryLatch();

        LuceneIndexSampler indexSampler = spy((LuceneIndexSampler) indexReader.createSampler());
        doAnswer(inv -> {
                    var obj = inv.callRealMethod();
                    dropLatch.release(); // We have now started the sampling, let the index try to drop
                    sampleLatch.await(); // Wait for the drop to be blocked
                    return obj;
                })
                .when(indexSampler)
                .newTask();

        List<Future<?>> futures = new ArrayList<>();
        try (var reader = indexReader /* do not inline! */;
                IndexSampler sampler = indexSampler /* do not inline! */) {
            futures.add(threading.execute(
                    nothing -> {
                        try {
                            indexSampler.sampleIndex(CursorContext.NULL_CONTEXT, new AtomicBoolean());
                            fail("expected exception");
                        } catch (IndexNotFoundKernelException e) {
                            assertEquals("Index dropped while sampling.", e.getMessage());
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
        reader.query(results, QueryContext.NULL_CONTEXT, unconstrained(), queries);
        return results;
    }

    private static long[] resultsArray(ValueIndexReader reader, PropertyIndexQuery... queries)
            throws IndexNotApplicableKernelException {
        try (NodeValueIterator iterator = results(reader, queries)) {
            return PrimitiveLongCollections.asArray(iterator);
        }
    }

    private IndexEntryUpdate<?> add(long nodeId, Object value) {
        return IndexQueryHelper.add(nodeId, index, value);
    }

    private IndexEntryUpdate<?> remove(long nodeId, Object value) {
        return IndexQueryHelper.remove(nodeId, index, value);
    }

    private IndexEntryUpdate<?> change(long nodeId, Object valueBefore, Object valueAfter) {
        return IndexQueryHelper.change(nodeId, index, valueBefore, valueAfter);
    }

    private void updateAndCommit(List<IndexEntryUpdate<?>> nodePropertyUpdates) throws IndexEntryConflictException {
        try (IndexUpdater updater = accessor.newUpdater(IndexUpdateMode.ONLINE, CursorContext.NULL_CONTEXT, false)) {
            for (IndexEntryUpdate<?> update : nodePropertyUpdates) {
                updater.process(update);
            }
        }
    }
}
