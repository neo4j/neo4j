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
package org.neo4j.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.helpers.ArrayUtil.array;
import static org.neo4j.internal.helpers.collection.MapUtil.stringMap;
import static org.neo4j.server.CommandLineArgs.parse;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;

class CommandLineArgsTest {
    @Test
    void shouldPickUpSpecifiedConfigFile() {
        Path dir = Path.of("/some-dir").toAbsolutePath();
        Path expectedFile = dir.resolve(Config.DEFAULT_CONFIG_FILE_NAME);
        assertEquals(expectedFile, parse("--config-dir", dir.toString()).configFile);
        assertEquals(expectedFile, parse("--config-dir=" + dir).configFile);
    }

    @Test
    void shouldResolveConfigFileRelativeToWorkingDirectory() {
        Path expectedFile = Path.of("some-dir", Config.DEFAULT_CONFIG_FILE_NAME);
        assertEquals(expectedFile, parse("--config-dir", "some-dir").configFile);
        assertEquals(expectedFile, parse("--config-dir=some-dir").configFile);
    }

    @Test
    void shouldReturnNullIfConfigDirIsNotSpecified() {
        assertNull(parse().configFile);
    }

    @Test
    void shouldPickUpSpecifiedHomeDir() {
        Path homeDir = Path.of("/some/absolute/homedir").toAbsolutePath();

        assertEquals(homeDir, parse("--home-dir", homeDir.toString()).homeDir);
        assertEquals(homeDir, parse("--home-dir=" + homeDir).homeDir);
    }

    @Test
    void shouldReturnNullIfHomeDirIsNotSpecified() {
        assertNull(parse().homeDir);
    }

    @Test
    void shouldPickUpOverriddenConfigurationParameters() {
        // GIVEN
        String[] args = array("-c", "myoption=myvalue");

        // WHEN
        CommandLineArgs parsed = CommandLineArgs.parse(args);

        // THEN
        assertEquals(stringMap("myoption", "myvalue"), parsed.configOverrides);
    }

    @Test
    void shouldPickUpOverriddenBooleanConfigurationParameters() {
        // GIVEN
        String[] args = array("-c", "myoptionenabled");

        // WHEN
        CommandLineArgs parsed = CommandLineArgs.parse(args);

        // THEN
        assertEquals(stringMap("myoptionenabled", Boolean.TRUE.toString()), parsed.configOverrides);
    }

    @Test
    void shouldPickUpMultipleOverriddenConfigurationParameters() {
        // GIVEN
        String[] args = array(
                "-c", "my_first_option=first",
                "-c", "myoptionenabled",
                "-c", "my_second_option=second");

        // WHEN
        CommandLineArgs parsed = CommandLineArgs.parse(args);

        // THEN
        assertEquals(
                stringMap(
                        "my_first_option",
                        "first",
                        "myoptionenabled",
                        Boolean.TRUE.toString(),
                        "my_second_option",
                        "second"),
                parsed.configOverrides);
    }

    @Test
    void shouldPickUpExpandCommandsArgument() {
        // GIVEN
        String[] args = array("--expand-commands");

        // WHEN
        CommandLineArgs parsed = parse(args);

        // THEN
        assertTrue(parsed.expandCommands);
    }

    @Test
    void expandCommandsShouldBeDisabledByDefault() {
        // GIVEN
        String[] args = array("--console-mode");

        // WHEN
        CommandLineArgs parsed = parse(args);

        // THEN
        assertFalse(parsed.expandCommands);
        assertTrue(parsed.consoleMode);
    }
}
