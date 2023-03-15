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

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.kernel.api.KernelTransaction.Type.EXPLICIT;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension()
public class IndexUsageStatsNotEnabledIT {
    @Inject
    private GraphDatabaseAPI db;

    @Disabled("Enable once index usage feature is hidden behind kernel version and we dont throw exception anymore")
    @Test
    void assertDefaultValuesWhenIndexUsageFeatureNonAvailable() throws IndexNotFoundKernelException {
        // Given
        var indexName = "index";
        try (Transaction tx = db.beginTx()) {
            tx.schema()
                    .indexFor(Label.label("Label"))
                    .on("prop")
                    .withName(indexName)
                    .create();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(1, TimeUnit.MINUTES);
        }

        triggerReportUsageStatistics();

        // When
        try (var tx = db.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            var ktx = tx.kernelTransaction();
            var index = ktx.schemaRead().indexGetForName(indexName);
            var stats = ktx.schemaRead().indexUsageStats(index);

            // Then
            assertThat(stats.trackedSince()).isEqualTo(0);
            assertThat(stats.lastRead()).isEqualTo(0);
            assertThat(stats.readCount()).isEqualTo(0);
        }
    }

    private void triggerReportUsageStatistics() {
        db.getDependencyResolver().resolveDependency(IndexingService.class).reportUsageStatistics();
    }
}
