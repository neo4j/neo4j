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
package org.neo4j.kernel.recovery;

import static java.util.function.Predicate.not;
import static org.assertj.core.api.Assumptions.assumeThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.do_parallel_recovery;

import org.assertj.core.api.Condition;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.lock.LockGroup;
import org.neo4j.lock.LockService;
import org.neo4j.storageengine.api.CommandStream;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

class ParallelRecoveryIT extends RecoveryIT {

    @Override
    void additionalConfiguration(Config config) {
        super.additionalConfiguration(config);
        config.set(do_parallel_recovery, true);
    }

    @Override
    TestDatabaseManagementServiceBuilder additionalConfiguration(TestDatabaseManagementServiceBuilder builder) {
        return builder.setConfig(do_parallel_recovery, true);
    }

    @Override
    protected GraphDatabaseAPI createDatabase(long logThreshold) {
        GraphDatabaseAPI db = super.createDatabase(logThreshold);
        DependencyResolver dependencyResolver = db.getDependencyResolver();
        if (dependencyResolver.containsDependency(StorageEngine.class)) {
            StorageEngine storageEngine = dependencyResolver.resolveDependency(StorageEngine.class);
            assumeParallelRecoveryImplemented(storageEngine);
        }
        return db;
    }

    private void assumeParallelRecoveryImplemented(StorageEngine engine) {
        assumeThatThrownBy(() -> engine.lockRecoveryCommands(
                        mock(CommandStream.class),
                        mock(LockService.class),
                        mock(LockGroup.class),
                        TransactionApplicationMode.EXTERNAL))
                .is(new Condition<>(
                        not(UnsupportedOperationException.class::isInstance), "Supported operation"));
    }
}
