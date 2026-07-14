/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.export;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.export.aura.AuraConsole;
import org.neo4j.export.aura.AuraURLFactory;

class AuraURLFactoryTest {

    private static AuraURLFactory auraURLFactory;

    @BeforeEach
    void setUp() {
        auraURLFactory = new AuraURLFactory();
    }

    @Test
    void buildConsoleURLWithInvalidURI() {
        // given
        boolean devMode = false;
        assertThatThrownBy(() -> auraURLFactory.buildConsoleURI("hello.local", devMode, null))
                .isInstanceOf(CommandFailedException.class)
                .hasMessageContaining("Invalid Bolt URI 'hello.local'");
    }

    @Test
    void buildConsoleURInNonDevMode() {
        // given
        boolean devMode = false;

        // when/then
        assertThatThrownBy(() ->
                        auraURLFactory.buildConsoleURI("neo4j+s://rogue-env.databases.neo4j-abc.io", devMode, null))
                .isInstanceOf(CommandFailedException.class)
                .hasMessageContaining("Invalid Bolt URI 'neo4j+s://rogue-env.databases.neo4j-abc.io'");
    }

    @Test
    void buildConsoleURLWithValidProdURI() {
        // given
        boolean devMode = false;
        // when
        AuraConsole consoleUrl = auraURLFactory.buildConsoleURI("neo4j+s://rogue.databases.neo4j.io", devMode, null);
        // then
        assertThat(consoleUrl.getImportUrl()).hasToString("https://console.neo4j.io/v2/databases/rogue/import");
    }

    @Test
    void buildValidConsoleURInDevMode() {
        // given
        boolean devMode = true;
        // when
        AuraConsole consoleUrl =
                auraURLFactory.buildConsoleURI("neo4j+s://rogue-env.databases.neo4j-abc.io", devMode, null);
        // then
        assertThat(consoleUrl.getImportUrl()).hasToString("https://console-env.neo4j-abc.io/v2/databases/rogue/import");
    }

    @Test
    void buildValidConsoleURInDevModeForStaging() {
        // given
        boolean devMode = true;
        // when
        AuraConsole consoleUrl =
                auraURLFactory.buildConsoleURI("neo4j+s://b7ea95cc-staging.databases.neo4j.io", devMode);
        // then
        assertThat(consoleUrl.getImportUrl())
                .hasToString("https://console-staging.neo4j.io/v2/databases/b7ea95cc/import");
    }

    @Test
    void buildValidConsoleURInPrivMode() {
        // given
        boolean devMode = false;

        // when
        AuraConsole consoleUrl =
                auraURLFactory.buildConsoleURI("neo4j+s://rogue.production-orch-0001.neo4j.io", devMode, null);
        // then
        assertThat(consoleUrl.getImportUrl()).hasToString("https://console.neo4j.io/v2/databases/rogue/import");
    }

    @Test
    void buildValidConsoleURInPrivModeInNonProd() {
        // given
        boolean devMode = false;

        // when
        AuraConsole consoleUrl =
                auraURLFactory.buildConsoleURI("neo4j+s://rogue.env-orch-0001.neo4j-abc.io", devMode, null);

        // then
        assertThat(consoleUrl.getImportUrl()).hasToString("https://console-env.neo4j-abc.io/v2/databases/rogue/import");

        // when
        consoleUrl = auraURLFactory.buildConsoleURI("neo4j+s://rogue.staging-orch-0001.neo4j.io", devMode, null);

        // then
        assertThat(consoleUrl.getImportUrl()).hasToString("https://console-staging.neo4j.io/v2/databases/rogue/import");

        // when/then
        assertThatThrownBy(
                        () -> auraURLFactory.buildConsoleURI("neo4j+s://rogue.env-orch-0001.neo4j.io", devMode, null))
                .isInstanceOf(CommandFailedException.class)
                .hasMessageContaining("Invalid Bolt URI 'neo4j+s://rogue.env-orch-0001.neo4j.io'");
    }

    @Test
    void exceptionWithDevModeOnRealURI() {
        // given
        boolean devMode = true;
        // when/then
        assertThatThrownBy(() -> auraURLFactory.buildConsoleURI("neo4j+s://rogue.databases.neo4j.io", devMode, null))
                .isInstanceOf(CommandFailedException.class)
                .hasMessageContaining(
                        "Expected to find an environment running in dev mode in bolt URI: neo4j+s://rogue.databases.neo4j.io");
    }

    @Test
    void shouldRecognizeBothEnvironmentAndDatabaseIdFromBoltURI() {
        // given
        boolean devMode = false;
        AuraConsole consoleURL = auraURLFactory.buildConsoleURI(
                "bolt+routing://mydbid-testenvironment.databases.neo4j.io", devMode, null);
        // when
        assertThat(consoleURL.getImportUrl())
                .hasToString("https://console-testenvironment.neo4j.io/v2/databases/mydbid/import");
    }

    @Test
    public void testBuildConsoleURLWithInstanceURIAndDbId() {
        AuraConsole console =
                auraURLFactory.buildConsoleURI("neo4j+s://myinstance.instances.neo4j.io", false, "9daac4b4");
        assertEquals(
                "https://console.neo4j.io/v2/databases/9daac4b4/import",
                console.getImportUrl().toString());
    }

    @Test
    public void testBuildConsoleURLWithInstanceURIWithoutDbIdThrows() {
        assertThatThrownBy(() -> auraURLFactory.buildConsoleURI("neo4j+s://myinstance.instances.neo4j.io", false, null))
                .isInstanceOf(CommandFailedException.class)
                .hasMessageContaining("--to-dbid must be specified when providing an instance based URI");
    }

    @Test
    public void testBuildConsoleURLWithStagingInstanceURI() {
        AuraConsole console =
                auraURLFactory.buildConsoleURI("neo4j+s://4f378a7c-staging.instances.neo4j.io", false, "9daac4b4");
        assertEquals(
                "https://console-staging.neo4j.io/v2/databases/9daac4b4/import",
                console.getImportUrl().toString());
    }

    @Test
    public void testBuildConsoleURLWithDevInstanceURIInDevMode() {
        AuraConsole console =
                auraURLFactory.buildConsoleURI("neo4j+s://633a4165-devfoobar.instances.neo4j-dev.io", true, "633a4165");
        assertEquals(
                "https://console-devfoobar.neo4j-dev.io/v2/databases/633a4165/import",
                console.getImportUrl().toString());
    }

    @Test
    public void testBuildConsoleURLWithDevInstanceURIFailsInNonDevMode() {
        assertThatThrownBy(() -> auraURLFactory.buildConsoleURI(
                        "neo4j+s://633a4165-devfoobar.instances.neo4j-dev.io", false, "633a4165"))
                .isInstanceOf(CommandFailedException.class)
                .hasMessageContaining("Invalid Bolt URI 'neo4j+s://633a4165-devfoobar.instances.neo4j-dev.io'");
    }
}
