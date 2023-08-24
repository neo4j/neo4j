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
package org.neo4j.kernel.impl.transaction.log.files;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.collection.Dependencies.dependenciesOf;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.dynamic_read_only_failover;
import static org.neo4j.configuration.GraphDatabaseSettings.logical_log_rotation_threshold;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.nativeimpl.AbsentNativeAccess;
import org.neo4j.internal.nativeimpl.ErrorTranslator;
import org.neo4j.internal.nativeimpl.NativeCallResult;
import org.neo4j.io.ByteUnit;
import org.neo4j.kernel.impl.transaction.log.rotation.monitor.LogRotationMonitorAdapter;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.utils.TestDirectory;

@DbmsExtension(configurationCallback = "configure")
class DynamicReadOnlyFailoverIT {
    private static final String TEST_SCOPE = "preallocation test";
    private static final int NUMBER_OF_NODES = 100;

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private GraphDatabaseAPI database;

    @Inject
    private Config config;

    @Inject
    private Monitors monitors;

    private FailingNativeAccess nativeAccess;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        nativeAccess = new FailingNativeAccess();
        builder.setExternalDependencies(dependenciesOf(nativeAccess));
    }

    @Test
    void switchDatabaseToReadOnlyModeOnPreallocationFailure() {
        long initialRotationThreshold = ByteUnit.kibiBytes(128);
        Label marker = Label.label("marker");
        try (Transaction transaction = database.beginTx()) {
            for (int i = 0; i < NUMBER_OF_NODES; i++) {
                transaction.createNode(marker);
            }
            transaction.commit();
        }

        config.setDynamic(logical_log_rotation_threshold, initialRotationThreshold, TEST_SCOPE);
        monitors.addMonitorListener(new LogRotationMonitorAdapter() {
            @Override
            public void startRotation(long currentLogVersion) {
                config.setDynamic(logical_log_rotation_threshold, getUnavailableBytes(), TEST_SCOPE);
                nativeAccess.startFailing();
                super.startRotation(currentLogVersion);
            }
        });

        try (Transaction transaction = database.beginTx()) {
            Node node = transaction.createNode();
            node.setProperty("a", RandomStringUtils.randomAscii((int) (initialRotationThreshold + 100)));
            transaction.commit();
        }

        assertThatThrownBy(() -> {
                    try (Transaction transaction = database.beginTx()) {
                        transaction.createNode();
                        transaction.commit();
                    }
                })
                .hasMessageContaining("read-only");

        assertDoesNotThrow(() -> {
            try (Transaction transaction = database.beginTx()) {
                assertEquals(NUMBER_OF_NODES, Iterators.count(transaction.findNodes(marker)));
            }
        });

        assertThatThrownBy(() -> {
                    try (Transaction transaction = database.beginTx()) {
                        transaction.createNode();
                        transaction.commit();
                    }
                })
                .hasMessageContaining("read-only");
    }

    @Test
    void doNotSwitchDatabaseToReadOnlyModeWhenFailoverIsDisabled() {
        long initialRotationThreshold = ByteUnit.kibiBytes(128);
        Label marker = Label.label("marker");
        try (Transaction transaction = database.beginTx()) {
            for (int i = 0; i < NUMBER_OF_NODES; i++) {
                transaction.createNode(marker);
            }
            transaction.commit();
        }

        config.setDynamic(dynamic_read_only_failover, false, TEST_SCOPE);
        config.setDynamic(logical_log_rotation_threshold, initialRotationThreshold, TEST_SCOPE);
        monitors.addMonitorListener(new LogRotationMonitorAdapter() {
            @Override
            public void startRotation(long currentLogVersion) {
                config.setDynamic(logical_log_rotation_threshold, getUnavailableBytes(), TEST_SCOPE);
                super.startRotation(currentLogVersion);
            }
        });

        assertDoesNotThrow(() -> {
            try (Transaction transaction = database.beginTx()) {
                Node node = transaction.createNode();
                node.setProperty("a", RandomStringUtils.randomAscii((int) (initialRotationThreshold + 100)));
                transaction.commit();
            }
        });

        assertDoesNotThrow(() -> {
            try (Transaction transaction = database.beginTx()) {
                assertEquals(NUMBER_OF_NODES, Iterators.count(transaction.findNodes(marker)));
            }
        });

        assertDoesNotThrow(() -> {
            try (Transaction transaction = database.beginTx()) {
                transaction.createNode();
                transaction.commit();
            }
        });
    }

    private long getUnavailableBytes() {
        try {
            return Files.getFileStore(testDirectory.homePath()).getUsableSpace() + ByteUnit.gibiBytes(10);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static class FailingNativeAccess extends AbsentNativeAccess {
        private static final int ERROR_CODE = 28;
        private final AtomicBoolean fail = new AtomicBoolean();

        @Override
        public ErrorTranslator errorTranslator() {
            return callResult -> callResult.getErrorCode() == ERROR_CODE;
        }

        @Override
        public NativeCallResult tryPreallocateSpace(int fd, long bytes) {
            if (fail.get()) {
                return new NativeCallResult(ERROR_CODE, "20 minutes adventure");
            }
            return super.tryPreallocateSpace(fd, bytes);
        }

        public void startFailing() {
            fail.set(true);
        }
    }
}
