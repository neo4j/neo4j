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
package org.neo4j.kernel.impl.newapi.parallel;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.kernel.impl.api.parallel.ParallelAccessCheck.shouldPerformCheck;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.scheduler.CallableExecutor;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

@DbmsExtension
class ParallelAccessCheckTest {

    @Inject
    private GraphDatabaseAPI databaseAPI;

    @Inject
    private JobScheduler jobScheduler;

    private CallableExecutor executor;

    @BeforeAll
    static void beforeAll() {
        assumeTrue(shouldPerformCheck());
    }

    @BeforeEach
    void beforeEach() {
        executor = jobScheduler.executor(Group.CYPHER_WORKER);
    }

    @Test
    void testAllStoreHolderAccess() {
        try (var tx = databaseAPI.beginTx()) {
            var future = executor.submit(() ->
                    ((InternalTransaction) tx).kernelTransaction().dataRead().nodeExists(1));
            assertThatThrownBy(future::get)
                    .hasRootCauseInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(
                            "A resource that does not support parallel access is being accessed by a Cypher worker thread");
        }
    }

    @Test
    void testLockClient() {
        try (var tx = databaseAPI.beginTx()) {
            var future = executor.submit(() -> {
                ((InternalTransaction) tx).kernelTransaction().locks().acquireExclusiveNodeLock(1);
                return null;
            });
            assertThatThrownBy(future::get)
                    .hasRootCauseInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(
                            "A resource that does not support parallel access is being accessed by a Cypher worker thread");
        }
    }
}
