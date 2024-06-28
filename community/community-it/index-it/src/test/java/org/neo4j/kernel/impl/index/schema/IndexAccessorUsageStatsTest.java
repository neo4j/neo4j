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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.allEntries;
import static org.neo4j.io.memory.ByteBufferFactory.heapBufferFactory;
import static org.neo4j.token.api.TokenHolder.TYPE_LABEL;
import static org.neo4j.token.api.TokenHolder.TYPE_PROPERTY_KEY;
import static org.neo4j.token.api.TokenHolder.TYPE_RELATIONSHIP_TYPE;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.set.ImmutableSet;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.HostedOnMode;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.impl.schema.TextIndexProvider;
import org.neo4j.kernel.api.impl.schema.trigram.TrigramIndexProvider;
import org.neo4j.kernel.api.index.EntityRange;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexUsageStats;
import org.neo4j.kernel.api.index.TokenIndexReader;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.api.schema.SchemaTestUtil;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.transaction.state.StaticIndexProviderMap;
import org.neo4j.kernel.impl.transaction.state.StaticIndexProviderMapFactory;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.scheduler.JobSchedulerExtension;
import org.neo4j.storageengine.api.schema.SimpleEntityTokenClient;
import org.neo4j.storageengine.api.schema.SimpleEntityValueClient;
import org.neo4j.test.extension.EphemeralNeo4jLayoutExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;
import org.neo4j.token.CreatingTokenHolder;
import org.neo4j.token.ReadOnlyTokenCreator;
import org.neo4j.token.TokenHolders;

@EphemeralPageCacheExtension
@EphemeralNeo4jLayoutExtension
@ExtendWith(JobSchedulerExtension.class)
@ExtendWith(LifeExtension.class)
public class IndexAccessorUsageStatsTest {
    private static final long trackedSinceMillis = 10000;
    private static final long deltaMillis = 5000;
    private static final long queryCount = 10;

    @Inject
    PageCache pageCache;

    @Inject
    FileSystemAbstraction fs;

    @Inject
    LifeSupport lifeSupport;

    @Inject
    DatabaseLayout databaseLayout;

    @Inject
    JobScheduler jobScheduler;

    // For testing
    private final FakeClock clock = Clocks.fakeClock(trackedSinceMillis, MILLISECONDS);
    private final DefaultIndexUsageTracking usageTracking = new DefaultIndexUsageTracking(clock);

    // For index setup
    private final Config config = Config.defaults();
    private final IndexDescriptor descriptor = IndexPrototype.forSchema(SchemaDescriptors.forLabel(1, 1))
            .withName("testIndex")
            .materialise(1);
    private final IndexSamplingConfig samplingConfig = new IndexSamplingConfig(config);
    private final TokenNameLookup nameLookup = SchemaTestUtil.SIMPLE_NAME_LOOKUP;
    private final ImmutableSet<OpenOption> openOptions = Sets.immutable.empty();
    private final StorageEngineIndexingBehaviour indexingBehaviour = StorageEngineIndexingBehaviour.EMPTY;

    @ParameterizedTest
    @MethodSource("propertyIndexAccessors")
    void propertyIndexShouldIncrementUsageCountOnQuery(IndexProviderDescriptor descriptor, PropertyIndexQuery query)
            throws IndexNotApplicableKernelException, IOException {
        // Given
        try (IndexAccessor accessor = createIndexAccessor(descriptor);
                var reader = accessor.newValueReader(usageTracking.track())) {
            // When
            for (int i = 0; i < queryCount; i++) {
                propertyQuery(reader, clock, query);
            }
        }
        var usageStats = usageTracking.getAndReset();
        var expectedLastUsedTime = clock.millis();
        clock.forward(deltaMillis, MILLISECONDS);

        // Then
        assertUsage(usageStats, expectedLastUsedTime, queryCount);
    }

    @ParameterizedTest
    @MethodSource("propertyIndexAccessors")
    void propertyIndexShouldIncrementUsageCountOnIndexSeek(IndexProviderDescriptor descriptor, PropertyIndexQuery query)
            throws IOException {
        // Given
        try (var indexAccessor = createIndexAccessor(descriptor);
                var reader = indexAccessor.newValueReader(usageTracking.track())) {
            // When
            for (int i = 0; i < queryCount; i++) {
                partitionedPropertyQuery(reader, query);
            }
        } catch (UnsupportedOperationException e) {
            Assumptions.assumeTrue(false, "Partitioned index seek is not supported by this reader");
        }
        var usageStats = usageTracking.getAndReset();
        var expectedLastUsedTime = clock.millis();
        clock.forward(deltaMillis, MILLISECONDS);

        // Then
        assertUsage(usageStats, expectedLastUsedTime, queryCount);
    }

    @ParameterizedTest
    @MethodSource("tokenIndexAccessors")
    void tokenIndexShouldIncrementUsageCountOnQuery(IndexProviderDescriptor providerDescriptor) throws IOException {
        // Given
        try (var indexAccessor = createIndexAccessor(providerDescriptor);
                var reader = indexAccessor.newTokenReader(usageTracking.track())) {
            // When
            for (int i = 0; i < queryCount; i++) {
                tokenQuery(reader, clock);
            }
        }
        var usageStats = usageTracking.getAndReset();
        var expectedLastUsedTime = clock.millis();
        clock.forward(deltaMillis, MILLISECONDS);

        // Then
        assertUsage(usageStats, expectedLastUsedTime, queryCount);
    }

    @ParameterizedTest
    @MethodSource("tokenIndexAccessors")
    void tokenIndexShouldNotIncrementUsageCountOnQueryWithRange(IndexProviderDescriptor providerDescriptor)
            throws IOException {
        // Given
        try (var indexAccessor = createIndexAccessor(providerDescriptor);
                var reader = indexAccessor.newTokenReader(usageTracking.track())) {
            // When
            for (int i = 0; i < queryCount; i++) {
                tokenQueryWithRange(reader, clock);
            }
        }
        var usageStats = usageTracking.getAndReset();
        clock.forward(deltaMillis, MILLISECONDS);

        // Then
        assertNoUsage(usageStats);
    }

    @ParameterizedTest
    @MethodSource("tokenIndexAccessors")
    void tokenIndexShouldIncrementUsageCountOnPartitionedEntityTokenScan(IndexProviderDescriptor providerDescriptor)
            throws IOException {
        // Given
        try (var indexAccessor = createIndexAccessor(providerDescriptor);
                var reader = indexAccessor.newTokenReader(usageTracking.track())) {
            // When
            for (int i = 0; i < queryCount; i++) {
                partitionedEntityTokenScan(reader, clock);
            }
        }
        var usageStats = usageTracking.getAndReset();
        var expectedLastUsedTime = clock.millis();
        clock.forward(deltaMillis, MILLISECONDS);

        // Then
        assertUsage(usageStats, expectedLastUsedTime, queryCount);
    }

    @ParameterizedTest
    @MethodSource("tokenIndexAccessors")
    void tokenIndexShouldIncrementUsageCountOnPartitionedEntityTokenScanWithLeadingPartition(
            IndexProviderDescriptor providerDescriptor) throws IOException {
        // Given
        try (var indexAccessor = createIndexAccessor(providerDescriptor);
                var reader = indexAccessor.newTokenReader(usageTracking.track())) {
            // When
            var leadingPartition = reader.entityTokenScan(1, CursorContext.NULL_CONTEXT, new TokenPredicate(1));
            for (int i = 0; i < queryCount; i++) {
                partitionedEntityTokenScanWithLeadingPartition(reader, clock, leadingPartition);
            }
        }
        var usageStats = usageTracking.getAndReset();
        var expectedLastUsedTime = clock.millis();
        clock.forward(deltaMillis, MILLISECONDS);

        // Then
        assertUsage(usageStats, expectedLastUsedTime, queryCount + 1);
    }

    private static Stream<Arguments> propertyIndexAccessors() {
        return Stream.of(
                Arguments.of(RangeIndexProvider.DESCRIPTOR, allEntries()),
                Arguments.of(PointIndexProvider.DESCRIPTOR, allEntries()),
                Arguments.of(TextIndexProvider.DESCRIPTOR, allEntries()),
                Arguments.of(TrigramIndexProvider.DESCRIPTOR, allEntries()),
                Arguments.of(FulltextIndexProviderFactory.DESCRIPTOR, PropertyIndexQuery.fulltextSearch("*")));
    }

    private static Stream<IndexProviderDescriptor> tokenIndexAccessors() {
        return Stream.of(TokenIndexProvider.DESCRIPTOR);
    }

    private IndexAccessor createIndexAccessor(IndexProviderDescriptor providerDescriptor) throws IOException {
        var providerMap = lifeSupport.add(createIndexProviderMap());
        var provider = providerMap.lookup(providerDescriptor);
        var completeDescriptor = provider.completeConfiguration(descriptor, indexingBehaviour);
        var populator = provider.getPopulator(
                completeDescriptor,
                samplingConfig,
                heapBufferFactory(1024),
                EmptyMemoryTracker.INSTANCE,
                nameLookup,
                openOptions,
                indexingBehaviour);
        try {
            populator.create();
        } finally {
            populator.close(true, CursorContext.NULL_CONTEXT);
        }
        return provider.getOnlineAccessor(
                completeDescriptor, samplingConfig, nameLookup, openOptions, indexingBehaviour);
    }

    private void tokenQuery(TokenIndexReader reader, FakeClock clock) {
        clock.forward(deltaMillis, MILLISECONDS);
        reader.query(
                new SimpleEntityTokenClient(),
                IndexQueryConstraints.unconstrained(),
                new TokenPredicate(1),
                CursorContext.NULL_CONTEXT);
    }

    private void tokenQueryWithRange(TokenIndexReader reader, FakeClock clock) {
        clock.forward(deltaMillis, MILLISECONDS);
        reader.query(
                new SimpleEntityTokenClient(),
                IndexQueryConstraints.unconstrained(),
                new TokenPredicate(1),
                EntityRange.from(5),
                CursorContext.NULL_CONTEXT);
    }

    private void partitionedEntityTokenScan(TokenIndexReader reader, FakeClock clock) {
        clock.forward(deltaMillis, MILLISECONDS);
        reader.entityTokenScan(1, CursorContext.NULL_CONTEXT, new TokenPredicate(1));
    }

    private void partitionedEntityTokenScanWithLeadingPartition(
            TokenIndexReader reader, FakeClock clock, PartitionedTokenScan leadingPartition) {
        clock.forward(deltaMillis, MILLISECONDS);
        reader.entityTokenScan(leadingPartition, new TokenPredicate(2));
    }

    private void partitionedPropertyQuery(ValueIndexReader reader, PropertyIndexQuery indexQuery) {
        clock.forward(deltaMillis, MILLISECONDS);
        reader.valueSeek(1, QueryContext.NULL_CONTEXT, indexQuery);
    }

    private void propertyQuery(ValueIndexReader reader, FakeClock clock, PropertyIndexQuery query)
            throws IndexNotApplicableKernelException {
        clock.forward(deltaMillis, MILLISECONDS);
        reader.query(
                new SimpleEntityValueClient(), QueryContext.NULL_CONTEXT, IndexQueryConstraints.unconstrained(), query);
    }

    private static void assertUsage(IndexUsageStats usageStats, long expectedLastUsedTime, long expectedQueryCount) {
        Assertions.assertThat(usageStats.readCount()).isEqualTo(expectedQueryCount);
        Assertions.assertThat(usageStats.lastRead()).isEqualTo(expectedLastUsedTime);
        Assertions.assertThat(usageStats.trackedSince()).isEqualTo(trackedSinceMillis);
    }

    private static void assertNoUsage(IndexUsageStats usageStats) {
        Assertions.assertThat(usageStats.readCount()).isEqualTo(0);
        Assertions.assertThat(usageStats.lastRead()).isEqualTo(0);
        Assertions.assertThat(usageStats.trackedSince()).isEqualTo(trackedSinceMillis);
    }

    private StaticIndexProviderMap createIndexProviderMap() {
        return StaticIndexProviderMapFactory.create(
                lifeSupport,
                config,
                pageCache,
                fs,
                NullLogService.getInstance(),
                new Monitors(),
                DatabaseReadOnlyChecker.writable(),
                HostedOnMode.SINGLE,
                RecoveryCleanupWorkCollector.ignore(),
                databaseLayout,
                getTokenHolders(),
                jobScheduler,
                CursorContextFactory.NULL_CONTEXT_FACTORY,
                PageCacheTracer.NULL);
    }

    private static TokenHolders getTokenHolders() {
        var propTokenHolder = new CreatingTokenHolder(ReadOnlyTokenCreator.READ_ONLY, TYPE_PROPERTY_KEY);
        var labelTokenHolder = new CreatingTokenHolder(ReadOnlyTokenCreator.READ_ONLY, TYPE_LABEL);
        var relTypetokenHolder = new CreatingTokenHolder(ReadOnlyTokenCreator.READ_ONLY, TYPE_RELATIONSHIP_TYPE);
        return new TokenHolders(propTokenHolder, labelTokenHolder, relTypetokenHolder);
    }
}
