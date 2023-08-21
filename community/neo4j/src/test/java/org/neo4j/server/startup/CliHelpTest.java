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
package org.neo4j.server.startup;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.PrintStream;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.neo4j.kernel.internal.Version;
import picocli.CommandLine;

class CliHelpTest extends ServerProcessTestBase {

    @Test
    void versionOptionShouldWorkOnRootLevel() {
        assertThat(execute("--version")).isEqualTo(0);
        assertThat(out.toString()).contains(Version.getNeo4jVersion());
    }

    @Test
    void versionShortOptionShouldWorkOnRootLevel() {
        assertThat(execute("-V")).isEqualTo(0);
        assertThat(out.toString()).contains(Version.getNeo4jVersion());
    }

    @Test
    void versionCommandShouldWorkOnRootLevel() {
        assertThat(execute("version")).isEqualTo(0);
        assertThat(out.toString()).contains(Version.getNeo4jVersion());
    }

    @Test
    void helpOptionShouldWorkOnRootLevel() {
        assertThat(execute("--help")).isEqualTo(0);
        assertThat(out.toString())
                .contains("Neo4j database administration tool.")
                // Let's check one option as an indication that the help is really printed
                .contains("--expand-commands   Allow command expansion in config value evaluation.");
    }

    @Test
    void helpShortOptionShouldWorkOnRootLevel() {
        assertThat(execute("-h")).isEqualTo(0);
        assertThat(out.toString())
                .contains("Neo4j database administration tool.")
                // Let's check one option as an indication that the help is really printed
                .contains("--expand-commands   Allow command expansion in config value evaluation.");
    }

    @Test
    void helpCommandShouldWorkOnRootLevel() {
        assertThat(execute("help")).isEqualTo(0);
        assertThat(out.toString())
                .contains("Neo4j database administration tool.")
                // Let's check one option as an indication that the help is really printed
                .contains("--expand-commands   Allow command expansion in config value evaluation.");
    }

    @Test
    void helpCommandWithSubcommandArgumentShouldWorkOnRootLevel() {
        assertThat(execute("help", "server")).isEqualTo(0);
        assertThat(out.toString())
                .contains("Usage: neo4j-admin server [COMMAND]")
                .contains("Server-wide administration tasks.")
                // Let's check one subcommand as an indication that the help is really printed
                .contains("console                Start server in console.");
    }

    @Test
    void helpOptionShouldWorkOnGroupLevel() {
        assertThat(execute("server", "--help")).isEqualTo(0);
        assertThat(out.toString())
                .contains("Server-wide administration tasks")
                // Let's check one subcommand as an indication that the help is really printed
                .contains("console                Start server in console.");
    }

    @Test
    void helpShortOptionShouldWorkOnGroupLevel() {
        assertThat(execute("server", "-h")).isEqualTo(0);
        assertThat(out.toString())
                .contains("Server-wide administration tasks")
                // Let's check one subcommand as an indication that the help is really printed
                .contains("console                Start server in console.");
    }

    @Test
    void helpCommandShouldWorkOnGroupLevel() {
        assertThat(execute("server", "help")).isEqualTo(0);
        assertThat(out.toString())
                .contains("Server-wide administration tasks")
                // Let's check one subcommand as an indication that the help is really printed
                .contains("console                Start server in console.");
    }

    @Test
    void helpCommandWithSubcommandArgumentShouldWorkOnGroupLevel() {
        assertThat(execute("server", "help", "memory-recommendation")).isEqualTo(0);
        assertThat(out.toString())
                .contains("Print Neo4j heap and pagecache memory settings recommendations")
                // Let's check one option as an indication that the help is really printed
                .contains("--docker            The recommended memory settings are produced in the form");
    }

    @Test
    void helpOptionShouldWorkOnSubcommandLevel() {
        assertThat(execute("server", "memory-recommendation", "--help")).isEqualTo(0);
        assertThat(out.toString())
                .contains("Print Neo4j heap and pagecache memory settings recommendations")
                // Let's check one option as an indication that the help is really printed
                .contains("--docker            The recommended memory settings are produced in the form");
    }

    @Test
    void helpShortOptionShouldWorkOnSubcommandLevel() {
        assertThat(execute("server", "memory-recommendation", "-h")).isEqualTo(0);
        assertThat(out.toString())
                .contains("Print Neo4j heap and pagecache memory settings recommendations")
                // Let's check one option as an indication that the help is really printed
                .contains("--docker            The recommended memory settings are produced in the form");
    }

    @EnabledOnOs(OS.WINDOWS)
    @Test
    void helpOptionShouldWorkForWindowsServiceCommand() {
        assertThat(execute("server", "windows-service", "--help")).isEqualTo(0);
        assertThat(out.toString())
                .contains("Neo4j windows service commands.")
                // Let's check one subcommand as an indication that the help is really printed
                .contains("install    Install the Windows service.");
    }

    @EnabledOnOs(OS.WINDOWS)
    @Test
    void helpShortOptionShouldWorkForWindowsServiceCommand() {
        assertThat(execute("server", "windows-service", "-h")).isEqualTo(0);
        assertThat(out.toString())
                .contains("Neo4j windows service commands.")
                // Let's check one subcommand as an indication that the help is really printed
                .contains("install    Install the Windows service.");
    }

    @EnabledOnOs(OS.WINDOWS)
    @Test
    void helpCommandShouldWorkForWindowsServiceCommand() {
        assertThat(execute("server", "windows-service", "help")).isEqualTo(0);
        assertThat(out.toString())
                .contains("Neo4j windows service commands.")
                // Let's check one subcommand as an indication that the help is really printed
                .contains("install    Install the Windows service.");
    }

    @EnabledOnOs(OS.WINDOWS)
    @Test
    void helpCommandWithSubcommandArgumentShouldWorkForWindowsServiceCommand() {
        assertThat(execute("server", "windows-service", "help", "install")).isEqualTo(0);
        assertThat(out.toString())
                .contains("neo4j-admin server windows-service install [-h] [--expand-commands] [--verbose]")
                .contains("Install the Windows service.");
    }

    @Test
    void missingFirstSubCommandShouldGiveReasonableSuggestions() {
        assertThat(execute("set-initial-password")).isEqualTo(2);
        assertThat(err.toString()).contains("Did you mean:", "dbms set-initial-password");

        clearOutAndErr();
        assertThat(execute("consistency-check")).isEqualTo(2);
        assertThat(err.toString()).contains("Did you mean:", "database check");
    }

    @Override
    protected CommandLine createCommand(
            PrintStream out,
            PrintStream err,
            Function<String, String> envLookup,
            Function<String, String> propLookup,
            Runtime.Version version) {
        var environment = new Environment(out, err, envLookup, propLookup, version);
        var command = new Neo4jAdminCommand(environment);
        return Neo4jAdminCommand.asCommandLine(command, environment);
    }
}
