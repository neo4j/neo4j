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
package org.neo4j.configuration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.STRING;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.server.helpers.CommunityWebContainerBuilder;
import org.neo4j.server.helpers.TestWebContainer;
import org.neo4j.test.Race;

public class ConfigurationIT {

    @Test
    void shouldNotDeadlockWhenConcurrentlyAccessingSettings() {
        Race race = new Race();
        race.addContestant(GraphDatabaseSettings.neo4j_home::defaultValue, 1);
        race.addContestant(HttpConnector.advertised_address::defaultValue, 1);
        assertThatCode(() -> race.go(1, TimeUnit.MINUTES))
                .doesNotThrowAnyException(); // throws TimeoutException on deadlock
    }

    @Test
    void shouldBeAbleToEvaluateSettingFromWebServer() throws IOException {
        assumeTrue(curlAvailable(), "Curl required");
        TestWebContainer testWebContainer =
                CommunityWebContainerBuilder.serverOnRandomPorts().build();

        try {
            // Given
            Config config = Config.newBuilder()
                    .allowCommandExpansion()
                    .addSettingsClass(TestSettings.class)
                    .setRaw(Map.of(
                            TestSettings.stringSetting.name(), "$(curl -I '" + testWebContainer.getBaseUri() + "')"))
                    .build();
            // Then
            assertThat(config.get(TestSettings.stringSetting)).contains("200 OK");
        } finally {
            testWebContainer.shutdown();
        }
    }

    private static boolean curlAvailable() {
        try {
            Process process = Runtime.getRuntime().exec("curl --help");
            if (process.waitFor(10, TimeUnit.SECONDS)) {
                return process.exitValue() == 0;
            }
        } catch (IOException | InterruptedException ignored) {
        }
        return false;
    }

    private static final class TestSettings implements SettingsDeclaration {
        static final Setting<String> stringSetting =
                newBuilder("db.test.setting.string", STRING, "").build();
    }
}
