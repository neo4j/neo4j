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
package org.neo4j.index.recovery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.exact;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.kernel.api.KernelTransaction.Type.EXPLICIT;
import static org.neo4j.test.DoubleLatch.awaitLatch;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.internal.kernel.api.IndexMonitor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@EphemeralTestDirectoryExtension
class IndexCreationRecoveryIT {
    @Inject
    private TestDirectory directory;

    @Inject
    private EphemeralFileSystemAbstraction fs;

    /**
     * This tests that this scenario won't happen:
     * - Db starts
     * - Creates index X
     * - X population finishes (its state on disk is now ONLINE, but schema store hasn't been checkpointed so will not have a record for it)
     * - Data that matches X gets committed
     * - Db crash (process exits)
     * - Db starts and goes into recovery
     * - IndexingService#init: doesn't see X in its initial indexes (read from schema store)
     * - Tx log recovery (re)creates X and makes it a recovering index proxy
     * - IndexingService#start:
     * 	- puts X in rebuildingDescriptors
     * 	- calls dropRecoveringIndexes on X (it's in rebuildingDescriptors), but RecoveringIndexProxy has a no-op drop)
     * 	- fires up index population, which will be managed by thread T now in the job scheduler
     * - Recovery makes a checkpoint where schema store (of course) gets checkpointed
     * - Recovery is shutting down its things, including calling IndexingService#stop which stops ongoing IndexPopulationJob instances,
     *   even those that haven't quite gotten started yet, i.e. won't have gotten to even dropping the index before starting population
     * - Post-recovery and database starts up
     * - IndexingService#init sees X as initial index and X is ONLINE (as it was right after population finished initially)
     * - ==> We're now in a state where X is ONLINE, recovered, but outdated
     */
    @Test
    void shouldEnsurePopulateIndexRecreation() throws Exception {
        // given
        var label = Label.label("L");
        var key = "k";
        var indexName = "mine";
        var nodeValue = "I'm here";
        var nodeId = new MutableLong();
        try (var fsSnapshot = createIndexedDataThenSnapshotFSAndShutDown(label, indexName, key, nodeValue, nodeId)) {
            // when
            var monitors = new Monitors();
            var barrier = new CountDownLatch(1);
            monitors.addMonitorListener(new IndexMonitor.MonitorAdapter() {
                @Override
                public void indexPopulationJobStarting(IndexDescriptor[] indexDescriptors) {
                    // This method is called by a spawned index population thread
                    if (containsOurIndex(indexDescriptors)) {
                        // Block here until this population has been stopped, so that the index population job
                        // will see the "stopped" state before even starting to do its thing
                        awaitLatch(barrier);
                    }
                }

                @Override
                public void populationCancelled(IndexDescriptor[] indexDescriptors, boolean storeScanHadStated) {
                    // This method is called by the recovery "main" thread as it's completing recovery
                    if (containsOurIndex(indexDescriptors)) {
                        // Unblock the population start above, now that we're sure that the "stopped" state is set
                        barrier.countDown();
                    }
                }

                private boolean containsOurIndex(IndexDescriptor[] indexDescriptors) {
                    return Arrays.stream(indexDescriptors)
                            .anyMatch(index -> index.getName().equals(indexName));
                }
            });
            var dbms = new TestDatabaseManagementServiceBuilder(directory.homePath())
                    .setFileSystem(new UncloseableDelegatingFileSystemAbstraction(fsSnapshot))
                    .setMonitors(monitors)
                    .build();
            try {
                // then
                var db = (GraphDatabaseAPI) dbms.database(DEFAULT_DATABASE_NAME);
                awaitIndexPopulations(db);
                try (var tx = db.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
                    var ktx = tx.kernelTransaction();
                    var index = ktx.schemaRead().indexGetForName(indexName);
                    var session = ktx.dataRead().indexReadSession(index);
                    try (var cursor =
                            ktx.cursors().allocateNodeValueIndexCursor(ktx.cursorContext(), ktx.memoryTracker())) {
                        var keyId = ktx.tokenRead().propertyKey(key);
                        ktx.dataRead()
                                .nodeIndexSeek(
                                        ktx.queryContext(), session, cursor, unconstrained(), exact(keyId, nodeValue));
                        assertThat(cursor.next()).isTrue();
                        assertThat(cursor.nodeReference()).isEqualTo(nodeId.longValue());
                    }
                }
            } finally {
                dbms.shutdown();
            }
        }
    }

    private EphemeralFileSystemAbstraction createIndexedDataThenSnapshotFSAndShutDown(
            Label label, String indexName, String key, Object value, MutableLong nodeId) {
        var dbms = new TestDatabaseManagementServiceBuilder(directory.homePath())
                .setFileSystem(new UncloseableDelegatingFileSystemAbstraction(fs))
                .build();
        EphemeralFileSystemAbstraction fsSnapshot;
        try {
            var db = dbms.database(DEFAULT_DATABASE_NAME);
            try (var tx = db.beginTx()) {
                tx.schema().indexFor(label).on(key).withName(indexName).create();
                tx.commit();
            }
            awaitIndexPopulations(db);
            try (var tx = db.beginTx()) {
                var node = tx.createNode(label);
                nodeId.setValue(node.getId());
                node.setProperty(key, value);
                tx.commit();
            }
            fsSnapshot = fs.snapshot();
        } finally {
            dbms.shutdown();
        }
        return fsSnapshot;
    }

    private static void awaitIndexPopulations(GraphDatabaseService db) {
        try (var tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(1, TimeUnit.MINUTES);
        }
    }
}
