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
package org.neo4j.kernel.diagnostics.providers;

import static org.neo4j.configuration.GraphDatabaseSettings.initial_default_database;
import static org.neo4j.configuration.GraphDatabaseSettings.logs_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.max_concurrent_transactions;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.GraphDatabaseSettings.plugin_dir;
import static org.neo4j.configuration.GraphDatabaseSettings.server_logging_config_path;
import static org.neo4j.logging.LogAssertions.assertThat;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.ssl.SslPolicyConfig;
import org.neo4j.configuration.ssl.SslPolicyScope;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.InternalLog;

class ConfigDiagnosticsTest {
    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private final InternalLog log = logProvider.getLog(ConfigDiagnostics.class);

    @Test
    void dumpConfigValues() {
        Config config = Config.newBuilder()
                .set(initial_default_database, "testDb")
                .set(max_concurrent_transactions, 400)
                .build();

        ConfigDiagnostics configDiagnostics = new ConfigDiagnostics(config);
        configDiagnostics.dump(log::info);

        assertThat(logProvider)
                .containsMessages(
                        "DBMS provided settings:",
                        max_concurrent_transactions.name() + "=400",
                        initial_default_database.name() + "=testDb")
                .doesNotContainMessage("No provided DBMS settings.");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "bar", "/", "/bar", "/bar/"})
    void dumpPathConfigValues(String destination) {
        SslPolicyConfig sslPolicy = SslPolicyConfig.forScope(SslPolicyScope.BOLT);
        // For windows, we need to take into account the drive letter
        Path pth = Path.of(destination);

        Config config = Config.newBuilder()
                .set(HttpConnector.enabled, true)
                .set(plugin_dir, pth)
                .set(sslPolicy.enabled, true)
                .build();

        ConfigDiagnostics configDiagnostics = new ConfigDiagnostics(config);
        configDiagnostics.dump(log::info);

        // Relative destinations should be resolved against neo4j_home
        String expected = (pth.isAbsolute() ? pth : config.get(neo4j_home).resolve(pth)).toString();

        // Check that groups, unspecified and specified directories are listed, but not files (alphabetic order)
        assertThat(logProvider)
                .containsMessagesInOrder(
                        "Directories in use:",
                        sslPolicy.base_directory.name(),
                        logs_directory.name(),
                        plugin_dir.name() + "=" + expected)
                .doesNotContainMessage(server_logging_config_path.name());
    }

    @Test
    void dumpDefaultConfig() {
        Config config = Config.defaults();

        ConfigDiagnostics configDiagnostics = new ConfigDiagnostics(config);
        configDiagnostics.dump(log::info);

        assertThat(logProvider)
                .containsMessages("No provided DBMS settings.")
                .doesNotContainMessage("DBMS provided settings");
    }
}
