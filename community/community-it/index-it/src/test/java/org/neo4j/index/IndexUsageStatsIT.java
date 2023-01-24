/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.index;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unorderedValues;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.allEntries;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.api.KernelTransaction.Type.EXPLICIT;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.kernel.api.index.IndexUsageStats;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension
class IndexUsageStatsIT {
    private static final String LABEL = "Label";
    private static final String KEY = "key";
    private static final String INDEX_NAME = "myIndex";

    @Inject
    private DatabaseManagementService dbms;

    @Inject
    private GraphDatabaseAPI db;

    private long beforeCreateTime;
    private long beforeQueryTime;

    @BeforeEach
    void createIndex() {
        beforeCreateTime = System.currentTimeMillis();
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
        beforeQueryTime = System.currentTimeMillis();
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
            assertThat(stats.trackedSinceTime()).isGreaterThanOrEqualTo(beforeCreateTime);
            assertThat(stats.lastUsedTime()).isGreaterThanOrEqualTo(beforeQueryTime);
            assertThat(stats.queryCount()).isEqualTo(1);
        });
    }

    @Test
    void shouldGatherUsageStatsWhenUsingCypher() {
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
            assertThat(stats.trackedSinceTime()).isGreaterThanOrEqualTo(beforeCreateTime);
            assertThat(stats.lastUsedTime()).isGreaterThanOrEqualTo(beforeQueryTime);
            assertThat(stats.queryCount()).isEqualTo(1);
        });
    }

    private void triggerReportUsageStatistics() {
        db.getDependencyResolver().resolveDependency(IndexingService.class).reportUsageStatistics();
    }

    private void assertIndexUsageStats(Consumer<IndexUsageStats> asserter) {
        try (var tx = db.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            var ktx = tx.kernelTransaction();
            var index = ktx.schemaRead().indexGetForName(INDEX_NAME);
            var usageStats = db.getDependencyResolver()
                    .resolveDependency(IndexStatisticsStore.class)
                    .usageStats(index.getId());
            asserter.accept(usageStats);
        }
    }
}
