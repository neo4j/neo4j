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
package org.neo4j.noopens;

import static org.neo4j.configuration.GraphDatabaseSettings.TransactionStateMemoryAllocation.OFF_HEAP;
import static org.neo4j.configuration.GraphDatabaseSettings.TransactionStateMemoryAllocation.ON_HEAP;
import static org.neo4j.logging.LogAssertions.assertThat;

import java.lang.invoke.MethodHandles;
import java.nio.Buffer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.fs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@EphemeralTestDirectoryExtension
public class NoOpensIT {

    @Inject
    private TestDirectory testDir;

    @BeforeAll
    static void before() {
        assertByteBufferClosed();
    }

    @Test
    void warningFromPageCache() {
        runTest(
                GraphDatabaseSettings.tx_state_memory_allocation,
                ON_HEAP,
                "Reflection access to java.nio.DirectByteBuffer is not available, using fallback mode. "
                        + "This could have negative impact on performance and memory usage. Consider adding --add-opens=java.base/java.nio=ALL-UNNAMED to VM options.");
    }

    @Test
    void warningFromOffHeapTxState() {
        runTest(
                GraphDatabaseSettings.tx_state_memory_allocation,
                OFF_HEAP,
                "db.tx_state.memory_allocation is set to OFF_HEAP but unsafe access to java.nio.DirectByteBuffer is not available."
                        + " Defaulting to ON_HEAP.");
    }

    public static void assertByteBufferClosed() {
        try {
            MethodHandles.privateLookupIn(Buffer.class, MethodHandles.lookup());
        } catch (IllegalAccessException e) {
            return;
        }
        Assertions.fail("java.nio looks to be open for reflection. Re-run test without opening java.nio.");
    }

    private <T> void runTest(Setting<T> setting, T settingValue, String warningMessage) {
        DatabaseManagementService dbms = null;
        var logProvider = new AssertableLogProvider(true);
        try {
            dbms = new TestDatabaseManagementServiceBuilder(testDir.homePath())
                    .setFileSystem(new UncloseableDelegatingFileSystemAbstraction(testDir.getFileSystem()))
                    .setConfig(setting, settingValue)
                    .setInternalLogProvider(logProvider)
                    .build();
            var db = dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);

            try (var tx = db.beginTx()) {
                tx.createNode(Label.label("label")).setProperty("key", "value");
                tx.commit();
            }
        } finally {

            if (dbms != null) {
                dbms.shutdown();
            }
        }
        assertThat(logProvider).containsMessages(warningMessage);
    }
}
