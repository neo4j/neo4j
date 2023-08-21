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
package org.neo4j.locking;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.lock_manager_verbose_deadlocks;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.Label;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension
class DynamicVerboseDeadlockIT {
    private static final Label LABEL = Label.label("Label");
    private static final String KEY = "key";
    private static final String INDEX_NAME = "my_index";

    @Inject
    private GraphDatabaseAPI db;

    @Test
    void shouldDynamicallyToggleVerboseDeadlockMessages() throws InterruptedException {
        assertThat(produceDeadlock(false)).hasMessageNotContaining("WAITING_FOR_EXCLUSIVE");
        assertThat(produceDeadlock(true)).hasMessageContaining("WAITING_FOR_EXCLUSIVE");
    }

    private DeadlockDetectedException produceDeadlock(boolean verboseDeadlocks) throws InterruptedException {
        db.getDependencyResolver()
                .resolveDependency(Config.class)
                .set(lock_manager_verbose_deadlocks, verboseDeadlocks);
        setUpData();

        var latch = new CountDownLatch(2);
        var tasks = new ArrayList<Callable<Object>>();
        for (int i = 0; i < 2; i++) {
            tasks.add(() -> {
                try (var tx = db.beginTx()) {
                    var index = tx.schema().getIndexByName(INDEX_NAME);
                    latch.countDown();
                    latch.await();
                    index.drop();
                    tx.commit();
                }
                return null;
            });
        }
        var executor = Executors.newFixedThreadPool(2);
        try {
            for (var future : executor.invokeAll(tasks)) {
                future.get();
            }
        } catch (ExecutionException e) {
            if (e.getCause() instanceof DeadlockDetectedException dde) {
                return dde;
            }
        } finally {
            executor.shutdown();
        }
        throw new RuntimeException("Was expecting a deadlock to occur");
    }

    private void setUpData() {
        try (var tx = db.beginTx()) {
            tx.schema().indexFor(LABEL).on(KEY).withName(INDEX_NAME).create();
            tx.commit();
        }
        try (var tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(1, TimeUnit.MINUTES);
        }
    }
}
