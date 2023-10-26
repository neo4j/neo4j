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
package org.neo4j.commandline.dbms;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.neo4j.cli.ContextInjectingFactory;
import org.neo4j.cli.ExecutionContext;
import picocli.CommandLine;

class DiagnosticsReportCommandTest {
    @Test
    void printUsageHelp() {
        final var baos = new ByteArrayOutputStream();
        ExecutionContext ctx = new ExecutionContext(Path.of("."), Path.of("."));
        final var command = new DiagnosticsReportCommand(ctx);
        try (var out = new PrintStream(baos)) {
            CommandLine commandLine = new CommandLine(command, new ContextInjectingFactory(ctx));
            commandLine.usage(new PrintStream(out), CommandLine.Help.Ansi.OFF);
        }
        assertThat(baos.toString().trim())
                .isEqualToIgnoringNewLines(
                        """
                Produces a zip/tar of the most common information needed for remote assessments.

                USAGE

                report [-h] [--expand-commands] [--list] [--verbose] [--ignore-disk-space-check
                       [=true|false]] [--additional-config=<file>] [--database=<database>]
                       [--to-path=<path>] [<classifier>...] [COMMAND]

                DESCRIPTION

                Will collect information about the system and package everything in an archive.
                If you specify 'all', everything will be included. You can also fine tune the
                selection by passing classifiers to the tool, e.g 'logs tx threads'.

                PARAMETERS

                      [<classifier>...]     Default: [config, logs, metrics, plugins, ps,
                                            sysprop, threads, tree, version]

                OPTIONS

                      --additional-config=<file>
                                          Configuration file with additional configuration.
                      --database=<database>
                                          Name of the database to report for. Can contain * and
                                            ? for globbing. Note that * and ? have special
                                            meaning in some shells and might need to be escaped
                                            or used with quotes.
                                            Default: *
                      --expand-commands   Allow command expansion in config value evaluation.
                  -h, --help              Show this help message and exit.
                      --ignore-disk-space-check[=true|false]
                                          Ignore disk full warning.
                                            Default: false
                      --list              List all available classifiers.
                      --to-path=<path>    Destination directory for reports. Defaults to a
                                            system tmp directory.
                      --verbose           Enable verbose output.""");
    }
}
