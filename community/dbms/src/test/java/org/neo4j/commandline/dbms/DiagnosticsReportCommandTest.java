/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.commandline.dbms;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.neo4j.cli.ExecutionContext;
import picocli.CommandLine;

class DiagnosticsReportCommandTest {
    @Test
    void printUsageHelp() {
        final var baos = new ByteArrayOutputStream();
        final var command = new DiagnosticsReportCommand(new ExecutionContext(Path.of("."), Path.of(".")));
        try (var out = new PrintStream(baos)) {
            CommandLine.usage(command, new PrintStream(out), CommandLine.Help.Ansi.OFF);
        }
        assertThat(baos.toString().trim())
                .isEqualToIgnoringNewLines(
                        """
                Produces a zip/tar of the most common information needed for remote assessments.

                USAGE

                report [--expand-commands] [--force] [--list] [--verbose] [--pid=<pid>]
                       [--to=<path>] [<classifier>...]

                DESCRIPTION

                Will collect information about the system and package everything in an archive.
                If you specify 'all', everything will be included. You can also fine tune the
                selection by passing classifiers to the tool, e.g 'logs tx threads'.

                PARAMETERS

                      [<classifier>...]     Default: [config, logs, metrics, plugins, ps,
                                            sysprop, threads, tree, version]

                OPTIONS

                      --expand-commands   Allow command expansion in config value evaluation.
                      --force             Ignore disk full warning
                      --list              List all available classifiers
                      --pid=<pid>         Specify process id of running neo4j instance
                      --to=<path>         Destination directory for reports. Defaults to a
                                            system tmp directory.
                      --verbose           Enable verbose output.""");
    }
}
