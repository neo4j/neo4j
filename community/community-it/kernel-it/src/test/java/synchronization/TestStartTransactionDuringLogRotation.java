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
package synchronization;

import static org.neo4j.configuration.GraphDatabaseInternalSettings.dedicated_transaction_appender;
import static org.neo4j.configuration.GraphDatabaseSettings.logical_log_rotation_threshold;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.ByteUnit;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.rotation.monitor.LogRotationMonitor;
import org.neo4j.kernel.impl.transaction.log.rotation.monitor.LogRotationMonitorAdapter;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.OtherThread;
import org.neo4j.test.extension.OtherThreadExtension;

@DbmsExtension(configurationCallback = "configure")
@ExtendWith(OtherThreadExtension.class)
public class TestStartTransactionDuringLogRotation {
    @Inject
    public GraphDatabaseAPI database;

    @Inject
    private OtherThread t2;

    @Inject
    private Monitors monitors;

    private ExecutorService executor;
    private CountDownLatch startLogRotationLatch;
    private CountDownLatch completeLogRotationLatch;
    private AtomicBoolean writerStopped;
    private Label label;
    private Future<Void> rotationFuture;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(logical_log_rotation_threshold, ByteUnit.mebiBytes(1))
                .setConfig(dedicated_transaction_appender, false);
    }

    @BeforeEach
    void setUp() throws InterruptedException {
        executor = Executors.newCachedThreadPool();
        startLogRotationLatch = new CountDownLatch(1);
        completeLogRotationLatch = new CountDownLatch(1);
        writerStopped = new AtomicBoolean();

        LogRotationMonitor rotationListener = new LogRotationMonitorAdapter() {
            @Override
            public void startRotation(long currentLogVersion) {
                startLogRotationLatch.countDown();
                try {
                    completeLogRotationLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        monitors.addMonitorListener(rotationListener);
        label = Label.label("Label");

        rotationFuture = t2.execute(forceLogRotation(database));

        // Waiting for the writer task to start a log rotation
        startLogRotationLatch.await();

        // Then we should be able to start a transaction, though perhaps not be able to finish it.
        // This is what the individual test methods will be doing.
        // The test passes when transaction.close completes within the test timeout, that is, it didn't deadlock.
    }

    private Callable<Void> forceLogRotation(GraphDatabaseAPI db) {
        return () -> {
            try (Transaction tx = db.beginTx()) {
                tx.createNode(label).setProperty("a", 1);
                tx.commit();
            }

            db.getDependencyResolver()
                    .resolveDependency(LogFiles.class)
                    .getLogFile()
                    .getLogRotation()
                    .rotateLogFile(LogAppendEvent.NULL);
            return null;
        };
    }

    @AfterEach
    public void tearDown() throws Exception {
        rotationFuture.get();
        writerStopped.set(true);
        executor.shutdown();
    }

    @Test
    void logRotationMustNotObstructStartingReadTransaction() {
        try (Transaction tx = database.beginTx()) {
            tx.getNodeById(0);
            completeLogRotationLatch.countDown();
            tx.commit();
        }
    }

    @Test
    void logRotationMustNotObstructStartingWriteTransaction() {
        try (Transaction tx = database.beginTx()) {
            tx.createNode();
            completeLogRotationLatch.countDown();
            tx.commit();
        }
    }
}
