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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.export.aura.AuraConsole;
import org.neo4j.export.aura.AuraURLFactory;

public class AuraURLFactoryTest {

    private static AuraURLFactory auraURLFactory;

    @BeforeEach
    public void setUp() {
        auraURLFactory = new AuraURLFactory();
    }

    @Test
    public void testBuildConsoleURLWithInvalidURI() {
        // given
        boolean devMode = false;
        CommandFailedException exception = assertThrows(
                CommandFailedException.class, () -> auraURLFactory.buildConsoleURI("hello.local", devMode));

        assertEquals("Invalid Bolt URI 'hello.local'", exception.getMessage());
    }

    @Test
    public void testBuildConsoleURInNonDevMode() {
        // given
        boolean devMode = false;

        // when
        CommandFailedException exception = assertThrows(
                CommandFailedException.class,
                () -> auraURLFactory.buildConsoleURI("neo4j+s://rogue-env.databases.neo4j-abc.io", devMode));
        // then
        assertEquals("Invalid Bolt URI 'neo4j+s://rogue-env.databases.neo4j-abc.io'", exception.getMessage());
    }

    @Test
    public void testBuildConsoleURLWithValidProdURI() {
        // given
        boolean devMode = false;
        // when
        AuraConsole consoleUrl = auraURLFactory.buildConsoleURI("neo4j+s://rogue.databases.neo4j.io", devMode);
        // then
        assertEquals(
                "https://console.neo4j.io/v2/databases/rogue/import",
                consoleUrl.getImportUrl().toString());
    }

    @Test
    public void testBuildValidConsoleURInDevMode() {
        // given
        boolean devMode = true;
        // when
        AuraConsole consoleUrl = auraURLFactory.buildConsoleURI("neo4j+s://rogue-env.databases.neo4j-abc.io", devMode);
        // then
        assertEquals(
                "https://console-env.neo4j-abc.io/v2/databases/rogue/import",
                consoleUrl.getImportUrl().toString());
    }

    @Test
    public void testBuildValidConsoleURInPrivMode() {
        // given
        boolean devMode = false;

        // when
        AuraConsole consoleUrl =
                auraURLFactory.buildConsoleURI("neo4j+s://rogue.production-orch-0001.neo4j.io", devMode);
        // then
        assertEquals(
                "https://console.neo4j.io/v2/databases/rogue/import",
                consoleUrl.getImportUrl().toString());
    }

    @Test
    public void testBuildValidConsoleURInPrivModeInNonProd() {
        // given
        boolean devMode = false;

        // when
        AuraConsole consoleUrl = auraURLFactory.buildConsoleURI("neo4j+s://rogue.env-orch-0001.neo4j-abc.io", devMode);

        // then
        assertEquals(
                "https://console-env.neo4j-abc.io/v2/databases/rogue/import",
                consoleUrl.getImportUrl().toString());

        // when
        consoleUrl = auraURLFactory.buildConsoleURI("neo4j+s://rogue.staging-orch-0001.neo4j.io", devMode);

        // then
        assertEquals(
                "https://console-staging.neo4j.io/v2/databases/rogue/import",
                consoleUrl.getImportUrl().toString());

        // when
        CommandFailedException exception = assertThrows(
                CommandFailedException.class,
                () -> auraURLFactory.buildConsoleURI("neo4j+s://rogue.env-orch-0001.neo4j.io", devMode));

        // then
        assertEquals("Invalid Bolt URI 'neo4j+s://rogue.env-orch-0001.neo4j.io'", exception.getMessage());
    }

    @Test
    public void testExceptionWithDevModeOnRealURI() {
        // given
        boolean devMode = true;
        // when
        CommandFailedException exception = assertThrows(
                CommandFailedException.class,
                () -> auraURLFactory.buildConsoleURI("neo4j+s://rogue.databases.neo4j.io", devMode));

        // then
        assertEquals(
                "Expected to find an environment running in dev mode in bolt URI: neo4j+s://rogue.databases.neo4j.io",
                exception.getMessage());
    }

    @Test
    public void shouldRecognizeBothEnvironmentAndDatabaseIdFromBoltURI() throws CommandFailedException {
        // given
        boolean devMode = false;
        AuraConsole consoleURL =
                auraURLFactory.buildConsoleURI("bolt+routing://mydbid-testenvironment.databases.neo4j.io", devMode);
        // when
        assertEquals(
                "https://console-testenvironment.neo4j.io/v2/databases/mydbid/import",
                consoleURL.getImportUrl().toString());
    }
}
