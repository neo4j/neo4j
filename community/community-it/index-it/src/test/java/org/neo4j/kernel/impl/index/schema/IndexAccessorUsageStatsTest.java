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
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.allEntries;
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
import org.junit.jupiter.api.BeforeEach;
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
import org.neo4j.internal.schema.AllIndexProviderDescriptors;
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
import org.neo4j.kernel.KernelVersionProviders;
import org.neo4j.kernel.api.index.EntityRange;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
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
import org.neo4j.values.ElementIdMapper;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Values;

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
    private static final Config CONFIG = Config.defaults();
    private static final IndexDescriptor DESCRIPTOR = IndexPrototype.forSchema(SchemaDescriptors.forLabel(1, 1))
            .withName("testIndex")
            .materialise(1);
    private static final IndexSamplingConfig SAMPLING_CONFIG = new IndexSamplingConfig(CONFIG);
    private static final TokenNameLookup NAME_LOOKUP = SchemaTestUtil.SIMPLE_NAME_LOOKUP;
    private static final ImmutableSet<OpenOption> OPEN_OPTIONS = Sets.immutable.empty();
    private static final StorageEngineIndexingBehaviour INDEXING_BEHAVIOUR = StorageEngineIndexingBehaviour.EMPTY;
    private StaticIndexProviderMap providerMap;

    @BeforeEach
    void instantiateIndexProviderMap() {
        providerMap = lifeSupport.add(createIndexProviderMap());
    }

    @ParameterizedTest
    @MethodSource("propertyIndexAccessors")
    void propertyIndexShouldIncrementUsageCountOnQuery(IndexProviderDescriptor descriptor, PropertyIndexQuery query)
            throws IndexNotApplicableKernelException, IOException {
        // Given
        try (IndexAccessor accessor = createIndexAccessor(descriptor);
                ValueIndexReader reader = accessor.newValueReader(usageTracking)) {
            // When
            for (int i = 0; i < queryCount; i++) {
                propertyQuery(reader, clock, query);
            }
        }
        IndexUsageStats usageStats = usageTracking.getAndReset();
        long expectedLastUsedTime = clock.millis();
        clock.forward(deltaMillis, MILLISECONDS);

        // Then
        assertUsage(usageStats, expectedLastUsedTime, queryCount);
    }

    @ParameterizedTest
    @MethodSource("propertyIndexAccessors")
    void propertyIndexShouldIncrementUsageCountOnIndexSeek(
            IndexProviderDescriptor providerDescriptor, PropertyIndexQuery query)
            throws IOException, IndexNotApplicableKernelException {
        IndexProvider provider = providerMap.lookup(providerDescriptor);
        IndexDescriptor completeDescriptor = provider.completeConfiguration(DESCRIPTOR, INDEXING_BEHAVIOUR);
        assumeThat(completeDescriptor.getCapability().supportPartitionedScan(query))
                .isTrue();

        // Given
        try (IndexAccessor indexAccessor = createIndexAccessor(providerDescriptor);
                ValueIndexReader reader = indexAccessor.newValueReader(usageTracking)) {
            // When
            for (int i = 0; i < queryCount; i++) {
                partitionedPropertyQuery(reader, query);
            }
        } catch (UnsupportedOperationException e) {
            Assumptions.assumeTrue(false, "Partitioned index seek is not supported by this reader");
        }
        IndexUsageStats usageStats = usageTracking.getAndReset();
        long expectedLastUsedTime = clock.millis();
        clock.forward(deltaMillis, MILLISECONDS);

        // Then
        assertUsage(usageStats, expectedLastUsedTime, queryCount);
    }

    @ParameterizedTest
    @MethodSource("tokenIndexAccessors")
    void tokenIndexShouldIncrementUsageCountOnQuery(IndexProviderDescriptor providerDescriptor) throws IOException {
        // Given
        try (IndexAccessor indexAccessor = createIndexAccessor(providerDescriptor);
                TokenIndexReader reader = indexAccessor.newTokenReader(usageTracking)) {
            // When
            for (int i = 0; i < queryCount; i++) {
                tokenQuery(reader, clock);
            }
        }
        IndexUsageStats usageStats = usageTracking.getAndReset();
        long expectedLastUsedTime = clock.millis();
        clock.forward(deltaMillis, MILLISECONDS);

        // Then
        assertUsage(usageStats, expectedLastUsedTime, queryCount);
    }

    @ParameterizedTest
    @MethodSource("tokenIndexAccessors")
    void tokenIndexShouldNotIncrementUsageCountOnQueryWithRange(IndexProviderDescriptor providerDescriptor)
            throws IOException {
        // Given
        try (IndexAccessor indexAccessor = createIndexAccessor(providerDescriptor);
                TokenIndexReader reader = indexAccessor.newTokenReader(usageTracking)) {
            // When
            for (int i = 0; i < queryCount; i++) {
                tokenQueryWithRange(reader, clock);
            }
        }
        IndexUsageStats usageStats = usageTracking.getAndReset();
        clock.forward(deltaMillis, MILLISECONDS);

        // Then
        assertNoUsage(usageStats);
    }

    @ParameterizedTest
    @MethodSource("tokenIndexAccessors")
    void tokenIndexShouldIncrementUsageCountOnPartitionedEntityTokenScan(IndexProviderDescriptor providerDescriptor)
            throws IOException {
        // Given
        try (IndexAccessor indexAccessor = createIndexAccessor(providerDescriptor);
                TokenIndexReader reader = indexAccessor.newTokenReader(usageTracking)) {
            // When
            for (int i = 0; i < queryCount; i++) {
                partitionedEntityTokenScan(reader, clock);
            }
        }
        IndexUsageStats usageStats = usageTracking.getAndReset();
        long expectedLastUsedTime = clock.millis();
        clock.forward(deltaMillis, MILLISECONDS);

        // Then
        assertUsage(usageStats, expectedLastUsedTime, queryCount);
    }

    @ParameterizedTest
    @MethodSource("tokenIndexAccessors")
    void tokenIndexShouldIncrementUsageCountOnPartitionedEntityTokenScanWithLeadingPartition(
            IndexProviderDescriptor providerDescriptor) throws IOException {
        // Given
        try (IndexAccessor indexAccessor = createIndexAccessor(providerDescriptor);
                TokenIndexReader reader = indexAccessor.newTokenReader(usageTracking)) {
            // When
            PartitionedTokenScan leadingPartition =
                    reader.entityTokenScan(1, CursorContext.NULL_CONTEXT, new TokenPredicate(1));
            for (int i = 0; i < queryCount; i++) {
                partitionedEntityTokenScanWithLeadingPartition(reader, clock, leadingPartition);
            }
        }
        IndexUsageStats usageStats = usageTracking.getAndReset();
        long expectedLastUsedTime = clock.millis();
        clock.forward(deltaMillis, MILLISECONDS);

        // Then
        assertUsage(usageStats, expectedLastUsedTime, queryCount + 1);
    }

    private static Stream<Arguments> propertyIndexAccessors() {
        return Stream.of(
                Arguments.of(AllIndexProviderDescriptors.RANGE_DESCRIPTOR, allEntries()),
                Arguments.of(AllIndexProviderDescriptors.POINT_DESCRIPTOR, allEntries()),
                Arguments.of(
                        AllIndexProviderDescriptors.POINT_DESCRIPTOR,
                        PropertyIndexQuery.boundingBox(
                                0,
                                Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 1, 2),
                                Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 4, 5))),
                Arguments.of(AllIndexProviderDescriptors.TEXT_V1_DESCRIPTOR, allEntries()),
                Arguments.of(AllIndexProviderDescriptors.TEXT_V2_DESCRIPTOR, allEntries()),
                Arguments.of(AllIndexProviderDescriptors.TEXT_V3_DESCRIPTOR, allEntries()),
                Arguments.of(
                        AllIndexProviderDescriptors.FULLTEXT_V1_DESCRIPTOR, PropertyIndexQuery.fulltextSearch("*")),
                Arguments.of(
                        AllIndexProviderDescriptors.FULLTEXT_V2_DESCRIPTOR, PropertyIndexQuery.fulltextSearch("*")));
    }

    private static Stream<IndexProviderDescriptor> tokenIndexAccessors() {
        return Stream.of(AllIndexProviderDescriptors.TOKEN_DESCRIPTOR);
    }

    private IndexAccessor createIndexAccessor(IndexProviderDescriptor providerDescriptor) throws IOException {
        IndexProvider provider = providerMap.lookup(providerDescriptor);
        IndexDescriptor completeDescriptor = provider.completeConfiguration(DESCRIPTOR, INDEXING_BEHAVIOUR);
        IndexPopulator populator = provider.getPopulator(
                completeDescriptor,
                SAMPLING_CONFIG,
                SchemaTestUtil.defaultHeapBufferFactory(),
                EmptyMemoryTracker.INSTANCE,
                NAME_LOOKUP,
                ElementIdMapper.PLACEHOLDER,
                OPEN_OPTIONS,
                INDEXING_BEHAVIOUR);
        try {
            populator.create();
        } finally {
            populator.close(true, CursorContext.NULL_CONTEXT);
        }
        return provider.getOnlineAccessor(
                completeDescriptor,
                SAMPLING_CONFIG,
                NAME_LOOKUP,
                ElementIdMapper.PLACEHOLDER,
                OPEN_OPTIONS,
                INDEXING_BEHAVIOUR);
    }

    private static void tokenQuery(TokenIndexReader reader, FakeClock clock) {
        clock.forward(deltaMillis, MILLISECONDS);
        try (SimpleEntityTokenClient client = new SimpleEntityTokenClient()) {
            reader.query(
                    client, IndexQueryConstraints.unconstrained(), new TokenPredicate(1), CursorContext.NULL_CONTEXT);
        }
    }

    private static void tokenQueryWithRange(TokenIndexReader reader, FakeClock clock) {
        clock.forward(deltaMillis, MILLISECONDS);
        try (SimpleEntityTokenClient client = new SimpleEntityTokenClient()) {
            reader.query(
                    client,
                    IndexQueryConstraints.unconstrained(),
                    new TokenPredicate(1),
                    EntityRange.from(5),
                    CursorContext.NULL_CONTEXT);
        }
    }

    private static void partitionedEntityTokenScan(TokenIndexReader reader, FakeClock clock) {
        clock.forward(deltaMillis, MILLISECONDS);
        reader.entityTokenScan(1, CursorContext.NULL_CONTEXT, new TokenPredicate(1));
    }

    private static void partitionedEntityTokenScanWithLeadingPartition(
            TokenIndexReader reader, FakeClock clock, PartitionedTokenScan leadingPartition) {
        clock.forward(deltaMillis, MILLISECONDS);
        reader.entityTokenScan(leadingPartition, new TokenPredicate(2));
    }

    private void partitionedPropertyQuery(ValueIndexReader reader, PropertyIndexQuery indexQuery)
            throws IndexNotApplicableKernelException {
        clock.forward(deltaMillis, MILLISECONDS);
        reader.valueSeek(1, QueryContext.NULL_CONTEXT, indexQuery);
    }

    private static void propertyQuery(ValueIndexReader reader, FakeClock clock, PropertyIndexQuery query)
            throws IndexNotApplicableKernelException {
        clock.forward(deltaMillis, MILLISECONDS);
        try (SimpleEntityValueClient client = new SimpleEntityValueClient()) {
            reader.query(
                    client,
                    QueryContext.NULL_CONTEXT,
                    CursorContext.NULL_CONTEXT,
                    IndexQueryConstraints.unconstrained(),
                    query);
        }
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
                CONFIG,
                KernelVersionProviders.latestFromConfig(CONFIG),
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
        CreatingTokenHolder propTokenHolder =
                new CreatingTokenHolder(ReadOnlyTokenCreator.READ_ONLY, TYPE_PROPERTY_KEY);
        CreatingTokenHolder labelTokenHolder = new CreatingTokenHolder(ReadOnlyTokenCreator.READ_ONLY, TYPE_LABEL);
        CreatingTokenHolder relTypetokenHolder =
                new CreatingTokenHolder(ReadOnlyTokenCreator.READ_ONLY, TYPE_RELATIONSHIP_TYPE);
        return new TokenHolders(propTokenHolder, labelTokenHolder, relTypetokenHolder);
    }
}
