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
package org.neo4j.index;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unorderedValues;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.allEntries;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.api.KernelTransaction.Type.EXPLICIT;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.api.index.IndexUsageStats;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.assertion.Assert;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.time.FakeClock;

@ImpermanentDbmsExtension(configurationCallback = "configuration")
class IndexUsageStatsIT {
    private static final String LABEL = "Label";
    private static final String KEY = "key";
    private static final String INDEX_NAME = "myIndex";

    @Inject
    private GraphDatabaseAPI db;

    private long beforeCreateTime;
    private long beforeQueryTime;
    private final FakeClock fakeClock = new FakeClock(System.currentTimeMillis(), TimeUnit.MILLISECONDS);

    @ExtensionCallback
    void configuration(TestDatabaseManagementServiceBuilder builder) {
        builder.setClock(fakeClock);
    }

    @BeforeEach
    void setup() {
        beforeCreateTime = fakeClock.millis();
        createIndex();
        fakeClock.forward(100, TimeUnit.MILLISECONDS);
        beforeQueryTime = fakeClock.millis();
    }

    @Test
    void shouldGatherUsageStatsWhenUsingKernelAPI() throws KernelException {
        // when
        try (var tx = db.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            var ktx = tx.kernelTransaction();
            var index = ktx.schemaRead().indexGetForName(INDEX_NAME);
            try (var cursor = ktx.cursors().allocateNodeValueIndexCursor(NULL_CONTEXT, INSTANCE)) {
                var indexSession = ktx.dataRead().indexReadSession(index);
                ktx.dataRead().nodeIndexSeek(ktx.queryContext(), indexSession, cursor, unorderedValues(), allEntries());
                while (cursor.next()) {
                    // just go through it
                }
            }
            tx.commit();
        }

        // then
        triggerReportUsageStatistics();
        assertIndexUsageStats(stats -> {
            assertThat(stats.trackedSince()).isGreaterThanOrEqualTo(beforeCreateTime);
            assertThat(stats.lastRead()).isGreaterThanOrEqualTo(beforeQueryTime);
            assertThat(stats.readCount()).isEqualTo(1);
        });
    }

    @Test
    void shouldGatherUsageStatsWhenUsingCypher() throws IndexNotFoundKernelException {
        // when
        try (var tx = db.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            try (var result = tx.execute(format("MATCH (n:%s) WHERE n.%s='hello' RETURN n", LABEL, KEY))) {
                while (result.hasNext()) {
                    result.next();
                }
            }
            tx.commit();
        }

        // then
        triggerReportUsageStatistics();
        assertIndexUsageStats(stats -> {
            assertThat(stats.trackedSince()).isGreaterThanOrEqualTo(beforeCreateTime);
            assertThat(stats.lastRead()).isGreaterThanOrEqualTo(beforeQueryTime);
            assertThat(stats.readCount()).isEqualTo(1);
        });
    }

    @Test
    void shouldHaveRealValueForNotUsedYetIndex() throws IndexNotFoundKernelException {
        // then
        triggerReportUsageStatistics();
        assertIndexUsageStats(stats -> {
            assertThat(stats.trackedSince()).isGreaterThanOrEqualTo(beforeCreateTime);
            assertThat(stats.lastRead()).isEqualTo(0);
            assertThat(stats.readCount()).isEqualTo(0);
        });
    }

    @Test
    void shouldHaveDefaultValueForNonExistingIndex() throws IndexNotFoundKernelException {
        // then
        triggerReportUsageStatistics();
        try (var tx = db.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            var ktx = tx.kernelTransaction();

            final var descriptor = SchemaDescriptors.forLabel(1, 42);
            // pick an index ID above the 3 that currently do exist
            final var nonExistingIndex = TestIndexDescriptorFactory.forSchema(13, descriptor);

            var stats = ktx.schemaRead().indexUsageStats(nonExistingIndex);
            assertThat(stats.trackedSince()).isEqualTo(0);
            assertThat(stats.lastRead()).isEqualTo(0);
            assertThat(stats.readCount()).isEqualTo(0);
        }
    }

    @Test
    void shouldHaveDefaultValueIfNoPeriodicUpdateYet() throws IndexNotFoundKernelException {
        // when
        try (var tx = db.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            try (var result = tx.execute(format("MATCH (n:%s) WHERE n.%s='hello' RETURN n", LABEL, KEY))) {
                while (result.hasNext()) {
                    result.next();
                }
            }
            tx.commit();
        }

        // Don't trigger report and don't forward clock

        // then
        assertIndexUsageStats(stats -> {
            assertThat(stats.trackedSince()).isEqualTo(0);
            assertThat(stats.lastRead()).isEqualTo(0);
            assertThat(stats.readCount()).isEqualTo(0);
        });
    }

    @Test
    void shouldRunPeriodicUpdateWhenClockIsForwarded() {
        // when
        fakeClock.forward(IndexingService.USAGE_REPORT_FREQUENCY_SECONDS, TimeUnit.SECONDS);

        // then
        assertEventuallyIndexUsageStats(stats -> stats.trackedSince() >= beforeCreateTime);
    }

    @Test
    void shouldThrowWhenTryingToGetStatsFromDroppedIndex() throws KernelException {
        // given
        try (var tx = db.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            var ktx = tx.kernelTransaction();
            var index = ktx.schemaRead().indexGetForName(INDEX_NAME);
            try (var cursor = ktx.cursors().allocateNodeValueIndexCursor(NULL_CONTEXT, INSTANCE)) {
                var indexSession = ktx.dataRead().indexReadSession(index);
                ktx.dataRead().nodeIndexSeek(ktx.queryContext(), indexSession, cursor, unorderedValues(), allEntries());
                while (cursor.next()) {
                    // just go through it
                }
            }
            tx.commit();
        }
        triggerReportUsageStatistics();
        assertIndexUsageStats(stats -> {
            assertThat(stats.trackedSince()).isGreaterThanOrEqualTo(beforeCreateTime);
            assertThat(stats.lastRead()).isGreaterThanOrEqualTo(beforeQueryTime);
            assertThat(stats.readCount()).isEqualTo(1);
        });

        // when
        dropIndex();

        // then
        assertThatThrownBy(this::getIndexUsageStats).isInstanceOf(IndexNotFoundKernelException.class);
    }

    @Test
    void shouldResetIndexStatsBetweenDropAndRecreate() throws KernelException {
        // given
        try (var tx = db.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            var ktx = tx.kernelTransaction();
            var index = ktx.schemaRead().indexGetForName(INDEX_NAME);
            try (var cursor = ktx.cursors().allocateNodeValueIndexCursor(NULL_CONTEXT, INSTANCE)) {
                var indexSession = ktx.dataRead().indexReadSession(index);
                ktx.dataRead().nodeIndexSeek(ktx.queryContext(), indexSession, cursor, unorderedValues(), allEntries());
                while (cursor.next()) {
                    // just go through it
                }
            }
            tx.commit();
        }
        triggerReportUsageStatistics();
        assertIndexUsageStats(stats -> {
            assertThat(stats.trackedSince()).isGreaterThanOrEqualTo(beforeCreateTime);
            assertThat(stats.lastRead()).isGreaterThanOrEqualTo(beforeQueryTime);
            assertThat(stats.readCount()).isEqualTo(1);
        });

        // when
        dropIndex();
        createIndex();

        // then
        triggerReportUsageStatistics();
        assertIndexUsageStats(stats -> {
            assertThat(stats.trackedSince()).isGreaterThanOrEqualTo(beforeCreateTime);
            assertThat(stats.lastRead()).isEqualTo(0);
            assertThat(stats.readCount()).isEqualTo(0);
        });
    }

    private void createIndex() {
        try (var tx = db.beginTx()) {
            tx.schema()
                    .indexFor(Label.label(LABEL))
                    .on(KEY)
                    .withName(INDEX_NAME)
                    .create();
            tx.commit();
        }
        try (var tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(1, TimeUnit.MINUTES);
        }
    }

    private void dropIndex() {
        try (Transaction tx = db.beginTx()) {
            tx.schema().getIndexByName(INDEX_NAME).drop();
            tx.commit();
        }
    }

    private void triggerReportUsageStatistics() {
        db.getDependencyResolver().resolveDependency(IndexingService.class).reportUsageStatistics();
    }

    private void assertIndexUsageStats(Consumer<IndexUsageStats> asserter) throws IndexNotFoundKernelException {
        var usageStats = getIndexUsageStats();
        asserter.accept(usageStats);
    }

    private IndexUsageStats getIndexUsageStats() throws IndexNotFoundKernelException {
        try (var tx = db.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            var ktx = tx.kernelTransaction();
            var index = ktx.schemaRead().indexGetForName(INDEX_NAME);
            return ktx.schemaRead().indexUsageStats(index);
        }
    }

    private void assertEventuallyIndexUsageStats(Predicate<IndexUsageStats> asserter) {
        try (var tx = db.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            var ktx = tx.kernelTransaction();
            var index = ktx.schemaRead().indexGetForName(INDEX_NAME);
            Assert.assertEventually(() -> ktx.schemaRead().indexUsageStats(index), asserter, 1, TimeUnit.MINUTES);
        }
    }
}
