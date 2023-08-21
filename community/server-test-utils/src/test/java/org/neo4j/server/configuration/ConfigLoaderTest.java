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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.default_advertised_address;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.SettingValueParsers.TRUE;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.ConfigUtils;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.server.WebContainerTestUtils;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class ConfigLoaderTest {
    @Inject
    private TestDirectory testDirectory;

    @Test
    void shouldProvideAConfiguration() {
        // given
        Path configFile = ConfigFileBuilder.builder(testDirectory.homePath()).build();

        // when
        Config config = Config.newBuilder()
                .fromFile(configFile)
                .set(neo4j_home, testDirectory.homePath())
                .build();

        // then
        assertNotNull(config);
    }

    @Test
    void shouldUseSpecifiedConfigFile() {
        // given
        Path configFile = ConfigFileBuilder.builder(testDirectory.homePath())
                .withNameValue(default_advertised_address.name(), "bar")
                .build();

        // when
        Config testConf = Config.newBuilder()
                .fromFile(configFile)
                .set(neo4j_home, testDirectory.homePath())
                .build();

        // then
        final String EXPECTED_VALUE = "bar";
        assertEquals(EXPECTED_VALUE, testConf.get(default_advertised_address).toString());
    }

    @Test
    void shouldUseSpecifiedHomeDir() {
        // given
        Path configFile = ConfigFileBuilder.builder(testDirectory.homePath()).build();

        // when
        Config testConf = Config.newBuilder()
                .fromFile(configFile)
                .set(neo4j_home, testDirectory.homePath())
                .build();

        // then
        Assertions.assertEquals(
                testDirectory.absolutePath().toString(),
                testConf.get(neo4j_home).toString());
    }

    @Test
    void shouldUseWorkingDirForHomeDirIfUnspecified() {
        // given
        Path configFile = ConfigFileBuilder.builder(testDirectory.homePath()).build();

        // when
        Config testConf = Config.newBuilder().fromFile(configFile).build();

        // then
        assertEquals(
                Path.of(System.getProperty("user.dir")).toAbsolutePath().toString(),
                testConf.get(neo4j_home).toString());
    }

    @Test
    void shouldAcceptDuplicateKeysWithSameValue() {
        // given
        Path configFile = ConfigFileBuilder.builder(testDirectory.homePath())
                .withNameValue(default_advertised_address.name(), "bar")
                .withNameValue(default_advertised_address.name(), "bar")
                .build();

        // when
        Config testConf = Config.newBuilder()
                .fromFile(configFile)
                .set(neo4j_home, testDirectory.homePath())
                .build();

        // then
        assertNotNull(testConf);
        final String EXPECTED_VALUE = "bar";
        assertEquals(EXPECTED_VALUE, testConf.get(default_advertised_address).toString());
    }

    @Test
    void loadOfflineConfigShouldDisableBolt() {
        // given
        Path configFile = ConfigFileBuilder.builder(testDirectory.homePath())
                .withNameValue(BoltConnector.enabled.name(), TRUE)
                .build();

        // when
        Config testConf = Config.newBuilder()
                .fromFile(configFile)
                .set(neo4j_home, testDirectory.homePath())
                .build();
        ConfigUtils.disableAllConnectors(testConf);

        // then
        assertNotNull(testConf);
        assertEquals(false, testConf.get(BoltConnector.enabled));
    }

    @Test
    void loadOfflineConfigAddDisabledBoltConnector() {
        // given
        Path configFile = ConfigFileBuilder.builder(testDirectory.homePath()).build();

        // when
        Config testConf = Config.newBuilder()
                .fromFile(configFile)
                .set(neo4j_home, testDirectory.homePath())
                .build();
        ConfigUtils.disableAllConnectors(testConf);

        // then
        assertNotNull(testConf);
        assertEquals(false, testConf.get(BoltConnector.enabled));
    }

    @Test
    void shouldFindThirdPartyJaxRsPackages() throws IOException {
        // given
        Path file = WebContainerTestUtils.createTempConfigFile(testDirectory.homePath());

        try (BufferedWriter out = Files.newBufferedWriter(
                file, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            out.write(ServerSettings.third_party_packages.name());
            out.write("=");
            out.write("com.foo.bar=\"mount/point/foo\",");
            out.write("com.foo.baz=\"/bar\",");
            out.write("com.foo.foobarbaz=\"/\"");
            out.write(System.lineSeparator());
        }

        // when
        Config config = Config.newBuilder()
                .fromFile(file)
                .set(neo4j_home, testDirectory.homePath())
                .build();

        // then
        List<ThirdPartyJaxRsPackage> thirdpartyJaxRsPackages = config.get(ServerSettings.third_party_packages);
        assertNotNull(thirdpartyJaxRsPackages);
        assertEquals(3, thirdpartyJaxRsPackages.size());
    }

    @Test
    void shouldRetainRegistrationOrderOfThirdPartyJaxRsPackages() {
        // given
        Path configFile = ConfigFileBuilder.builder(testDirectory.homePath())
                .withNameValue(
                        ServerSettings.third_party_packages.name(),
                        "org.neo4j.extension.extension1=/extension1,org.neo4j.extension.extension2=/extension2,"
                                + "org.neo4j.extension.extension3=/extension3")
                .build();

        // when
        Config config = Config.newBuilder()
                .fromFile(configFile)
                .set(neo4j_home, testDirectory.homePath())
                .build();

        // then
        List<ThirdPartyJaxRsPackage> thirdpartyJaxRsPackages = config.get(ServerSettings.third_party_packages);

        assertEquals(3, thirdpartyJaxRsPackages.size());
        assertEquals("/extension1", thirdpartyJaxRsPackages.get(0).mountPoint());
        assertEquals("/extension2", thirdpartyJaxRsPackages.get(1).mountPoint());
        assertEquals("/extension3", thirdpartyJaxRsPackages.get(2).mountPoint());
    }

    @Test
    void shouldThrowWhenSpecifiedConfigFileDoesNotExist() {
        // Given
        Path nonExistentConfigFile = Path.of("/tmp/" + System.currentTimeMillis());

        // When
        assertThrows(IllegalArgumentException.class, () -> Config.newBuilder()
                .fromFile(nonExistentConfigFile)
                .set(neo4j_home, testDirectory.homePath())
                .build());
    }

    @Test
    void shouldWorkFineWhenSpecifiedConfigFileDoesNotExist() {
        // Given
        Path nonExistentConfigFile = Path.of("/tmp/" + System.currentTimeMillis());

        // When
        Config config = Config.newBuilder()
                .fromFileNoThrow(nonExistentConfigFile)
                .set(neo4j_home, testDirectory.homePath())
                .build();

        // Then
        assertNotNull(config);
    }

    @Test
    void shouldDefaultToCorrectValueForAuthStoreLocation() {
        Path configFile = ConfigFileBuilder.builder(testDirectory.homePath())
                .withoutSetting(GraphDatabaseSettings.data_directory)
                .build();
        Config config = Config.newBuilder()
                .fromFile(configFile)
                .set(neo4j_home, testDirectory.homePath())
                .build();

        assertThat(config.get(GraphDatabaseInternalSettings.auth_store_directory))
                .isEqualTo(
                        testDirectory.homePath().resolve("data").resolve("dbms").toAbsolutePath());
    }

    @Test
    void shouldSetAValueForAuthStoreLocation() {
        Path configFile = ConfigFileBuilder.builder(testDirectory.homePath())
                .withSetting(GraphDatabaseSettings.data_directory, "the-data-dir")
                .build();
        Config config = Config.newBuilder()
                .fromFile(configFile)
                .set(neo4j_home, testDirectory.homePath())
                .build();

        assertThat(config.get(GraphDatabaseInternalSettings.auth_store_directory))
                .isEqualTo(testDirectory
                        .homePath()
                        .resolve("the-data-dir")
                        .resolve("dbms")
                        .toAbsolutePath());
    }
}
