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
package org.neo4j.server.configuration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.logging.AssertableLogProvider.Level.WARN;
import static org.neo4j.logging.LogAssertions.assertThat;
import static org.neo4j.server.configuration.ConfigurableServerModules.TRANSACTIONAL_ENDPOINTS;
import static org.neo4j.server.configuration.ServerSettings.allow_telemetry;
import static org.neo4j.server.configuration.ServerSettings.http_auth_allowlist;
import static org.neo4j.server.configuration.ServerSettings.http_enabled_modules;
import static org.neo4j.server.configuration.ServerSettings.third_party_packages;
import static org.neo4j.server.configuration.ServerSettings.webserver_max_threads;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class ServerSettingsMigratorTest {
    @Inject
    private TestDirectory testDirectory;

    @Test
    void testWhitelistSettingsRename() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(confFile, List.of("dbms.security.http_auth_whitelist=a,b"));

        Config config = Config.newBuilder().fromFile(confFile).build();
        var logProvider = new AssertableLogProvider();
        config.setLogger(logProvider.getLog(Config.class));

        assertThat(logProvider)
                .forClass(Config.class)
                .forLevel(WARN)
                .containsMessageWithArguments(
                        "Use of deprecated setting '%s'. It is replaced by '%s'.",
                        "dbms.security.http_auth_whitelist", http_auth_allowlist.name());

        assertEquals(List.of("a", "b"), config.get(http_auth_allowlist));
    }

    @Test
    void migrateThirdPartyPackagesSetting() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(
                confFile, List.of(" dbms.unmanaged_extension_classes=org.neo4j.test.server.unmanaged=/examples/test"));

        Config config = Config.newBuilder().fromFile(confFile).build();
        var logProvider = new AssertableLogProvider();
        config.setLogger(logProvider.getLog(Config.class));

        assertThat(logProvider)
                .forClass(Config.class)
                .forLevel(WARN)
                .containsMessageWithArguments(
                        "Use of deprecated setting '%s'. It is replaced by '%s'.",
                        "dbms.unmanaged_extension_classes", third_party_packages.name());

        assertThat(config.get(third_party_packages))
                .hasSize(1)
                .contains(new ThirdPartyJaxRsPackage("org.neo4j.test.server.unmanaged", "/examples/test"));
    }

    @Test
    void migrateMaxThreads() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(confFile, List.of("dbms.threads.worker_count=9874"));

        Config config = Config.newBuilder().fromFile(confFile).build();
        assertEquals(9874, config.get(webserver_max_threads));
    }

    @Test
    void migrateEnabledModulesSettings() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(confFile, List.of("dbms.http_enabled_modules=TRANSACTIONAL_ENDPOINTS"));

        Config config = Config.newBuilder().fromFile(confFile).build();

        assertEquals(Set.of(TRANSACTIONAL_ENDPOINTS), config.get(http_enabled_modules));
    }

    @Test
    void migrateClientsTelemetrySetting() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(confFile, List.of("client.allow_telemetry=false"));

        Config config = Config.newBuilder().fromFile(confFile).build();

        assertFalse(config.get(allow_telemetry));
    }
}
