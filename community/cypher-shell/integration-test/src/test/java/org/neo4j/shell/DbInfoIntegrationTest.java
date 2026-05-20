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
package org.neo4j.shell;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.assertion.Assert.assertNever;
import static org.neo4j.test.assertion.Assert.awaitUntilAsserted;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.neo4j.cypher.internal.CypherVersion;
import org.neo4j.shell.cli.AccessMode;
import org.neo4j.shell.completions.DbInfo;
import org.neo4j.shell.completions.DbInfoImpl;
import org.neo4j.shell.parameter.ParameterService;
import org.neo4j.shell.state.BoltStateHandler;

// NOTE! Consider adding tests to integration-test-expect instead of here.
@Timeout(value = 5, unit = MINUTES)
class DbInfoIntegrationTest extends TestHarness {
    static class TestDbInfo extends DbInfoImpl {
        AtomicInteger stopPollingCalls = new AtomicInteger(0);
        AtomicInteger resumePollingCalls = new AtomicInteger(0);

        public TestDbInfo(
                ParameterService parameterService, BoltStateHandler boltStateHandler, boolean enableCompletions) {
            super(parameterService, boltStateHandler, enableCompletions);
        }

        @Override
        public void resumePolling() {
            resumePollingCalls.incrementAndGet();
            super.resumePolling();
        }

        @Override
        public void stopPolling() {
            stopPollingCalls.incrementAndGet();
            super.stopPolling();
        }
    }

    @Disabled
    @Test
    void fillsInInformationInDbInfo() throws Exception {
        assumeAtLeastVersion("5.0.0");
        var testBuilder = (TestBuilder) buildTest();
        testBuilder
                .addArgs("-u", USER, "-p", PASSWORD, "--enable-autocompletions")
                .userInputLines(
                        ":param x => 1;",
                        "CREATE (n:A { name: \"Nacho\" });",
                        "CREATE (n:B);",
                        "CREATE (n:C);",
                        "CREATE ALIAS nacho IF NOT EXISTS FOR DATABASE neo4j;",
                        "CREATE USER foo IF NOT EXISTS SET PASSWORD 'something';")
                .run()
                .assertSuccessAndConnected();
        final var dbInfo = testBuilder.dbInfo;

        awaitUntilAsserted(() -> {
            assertThat(dbInfo.labels).contains("A", "B", "C");
            assertThat(dbInfo.propertyKeys).contains("name");
            assertThat(dbInfo.functions.get(CypherVersion.Cypher5)).contains("abs");
            assertThat(dbInfo.procedures.get(CypherVersion.Cypher5)).containsKey("dbms.info");
            assertThat(dbInfo.procedures
                            .get(CypherVersion.Cypher5)
                            .get("dbms.info")
                            .returnDescription())
                    .extracting(DbInfo.ReturnDescription::name)
                    .contains("name", "id", "creationDate");
            assertThat(dbInfo.aliasNames).contains("nacho");
            assertThat(dbInfo.roleNames).contains("PUBLIC");
            assertThat(dbInfo.databaseNames).contains("neo4j");
            assertThat(dbInfo.userNames).contains(USER, "foo");
            assertThat(dbInfo.parameters()).containsKey("x");
        });
    }

    // Note: needs server supporting cypher 25 with apoc installed to work
    @Test
    @Disabled
    void fillsVersionedInfoInDbInfo() throws Exception {
        assumeAtLeastVersion("2025.05.0");
        // assumeAtLeastVersion("5.26.0"); // Switch to this for testing pre-2025.03.0, also switching in
        // QueryPoller.startPolling.

        var testBuilder = (TestBuilder) buildTest();
        testBuilder
                .addArgs("-u", USER, "-p", PASSWORD, "--enable-autocompletions")
                .userInputLines(
                        ":param x => 1;",
                        "CREATE (n:A { name: \"Nacho\" });",
                        "CREATE (n:B);",
                        "CREATE (n:C);",
                        "CREATE ALIAS nacho IF NOT EXISTS FOR DATABASE neo4j;",
                        "CREATE USER foo IF NOT EXISTS SET PASSWORD 'something';")
                .run()
                .assertSuccessAndConnected();
        final var dbInfo = testBuilder.dbInfo;

        awaitUntilAsserted(() -> {
            assertThat(dbInfo.functions.get(CypherVersion.Cypher5)).contains("abs");
            assertThat(dbInfo.functions.get(CypherVersion.Cypher25)).contains("abs");

            assertThat(dbInfo.procedures.get(CypherVersion.Cypher5)).containsKey("dbms.info");
            assertThat(dbInfo.procedures.get(CypherVersion.Cypher25)).containsKey("dbms.info");
            assertThat(dbInfo.procedures
                            .get(CypherVersion.Cypher5)
                            .get("dbms.info")
                            .returnDescription())
                    .extracting(DbInfo.ReturnDescription::name)
                    .contains("name", "id", "creationDate");
            assertThat(dbInfo.procedures
                            .get(CypherVersion.Cypher25)
                            .get("dbms.info")
                            .returnDescription())
                    .extracting(DbInfo.ReturnDescription::name)
                    .contains("name", "id", "creationDate");
            assertThat(dbInfo.procedures.get(CypherVersion.Cypher5).get("dbms.upgradeStatus"))
                    .isNotNull();
            assertThat(dbInfo.procedures.get(CypherVersion.Cypher25).get("dbms.upgradeStatus"))
                    .isNull();
        });
    }

    @Test
    void doesNotFillDbInfoInOlderVersions() throws Exception {
        assumeVersionBefore("5.0.0");
        var testBuilder = (TestBuilder) buildTest();
        testBuilder
                .addArgs("-u", USER, "-p", PASSWORD, "--enable-autocompletions")
                .userInputLines(":param x => 1;", "CREATE (n:A { name: \"Nacho\" });", "CREATE (n:B);", "CREATE (n:C);")
                .run()
                .assertSuccessAndConnected();
        final var dbInfo = testBuilder.dbInfo;

        assertNever(
                () -> dbInfo,
                db -> dbInfo.labels.contains("A")
                        || dbInfo.propertyKeys.contains("name")
                        || dbInfo.functions.get(CypherVersion.Cypher5).contains("abs")
                        || dbInfo.procedures.get(CypherVersion.Cypher5).containsKey("dbms.info")
                        || dbInfo.aliasNames.contains("nacho")
                        || dbInfo.roleNames.contains("PUBLIC")
                        || dbInfo.databaseNames.contains("neo4j")
                        || dbInfo.userNames.contains("foo"),
                30,
                SECONDS);
    }

    @Test
    void doesNotFillDbInfoWhenCompletionsDisabled() throws Exception {
        assumeAtLeastVersion("5.0.0");
        var testBuilder = (TestBuilder) buildTest();
        testBuilder
                .addArgs("-u", USER, "-p", PASSWORD)
                .userInputLines(
                        ":param x => 1;",
                        "CREATE (n:A { name: \"Nacho\" });",
                        "CREATE (n:B);",
                        "CREATE (n:C);",
                        "CREATE ALIAS nacho IF NOT EXISTS FOR DATABASE neo4j;",
                        "CREATE USER foo IF NOT EXISTS SET PASSWORD 'something';")
                .run()
                .assertSuccessAndConnected();
        var dbInfo = testBuilder.dbInfo;
        assertNever(
                () -> dbInfo,
                db -> db.labels.contains("A")
                        || db.propertyKeys.contains("name")
                        || db.functions.get(CypherVersion.Cypher5).contains("abs")
                        || db.procedures.get(CypherVersion.Cypher5).containsKey("dbms.info")
                        || db.aliasNames.contains("nacho")
                        || db.roleNames.contains("PUBLIC")
                        || db.databaseNames.contains("neo4j")
                        || db.userNames.contains("foo"),
                30,
                SECONDS);
    }

    @Disabled
    @Test
    void stopsAndResumesPollingCorrectly() throws Exception {
        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            var isOutputInteractive = true;
            var boltStateHandler = new BoltStateHandler(isOutputInteractive, AccessMode.WRITE, Optional.empty());
            var paramService = ParameterService.create(boltStateHandler);
            var dbInfo = new TestDbInfo(paramService, boltStateHandler, true);
            var testBuilder = new TestBuilder(paramService, boltStateHandler, dbInfo, isOutputInteractive, false);

            executor.submit(() -> {
                try {
                    testBuilder
                            .addArgs("-u", USER, "-p", PASSWORD, "--enable-autocompletions")
                            .run();
                } catch (Exception e) {
                }
            });

            // Test that after some inactivity the poller has stopped
            assertEventually(() -> dbInfo, db -> db.stopPollingCalls.get() > 0, 2, MINUTES);

            dbInfo.resumePollingCalls = new AtomicInteger(0);
            testBuilder.terminal.write().println("CREATE (n:E);");

            // We check the polling has been resumed after the user has typed something
            assertEventually(() -> dbInfo, db -> db.resumePollingCalls.get() > 0, 2, MINUTES);

            testBuilder.closeMain();
        }
    }
}
