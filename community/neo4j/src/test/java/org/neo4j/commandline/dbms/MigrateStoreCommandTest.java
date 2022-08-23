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

class MigrateStoreCommandTest {
    @Test
    void printUsageHelp() {
        final var baos = new ByteArrayOutputStream();
        final var command = new MigrateStoreCommand(new ExecutionContext(Path.of("."), Path.of(".")));
        try (var out = new PrintStream(baos)) {
            CommandLine.usage(command, new PrintStream(out), CommandLine.Help.Ansi.OFF);
        }
        assertThat(baos.toString().trim())
                .isEqualToIgnoringNewLines(
                        """
                         Migrate a database

                         USAGE

                         migrate [-h] [--expand-commands] [--verbose] [--additional-config=<path>]
                                 [--pagecache=<size>] [--to-format=<format name>] <database>

                         DESCRIPTION

                         Migrates a database from one format to another or between versions of the same
                         format. It always migrates the database to the latest combination of major and
                         minor version of the target format.

                         PARAMETERS

                               <database>           Name of the database to migrate. Can contain * and ?
                                                      for globbing.

                         OPTIONS

                               --additional-config=<path>
                                                    Configuration file with additional configuration.
                               --expand-commands    Allow command expansion in config value evaluation.
                           -h, --help               Show this help message and exit.
                               --pagecache=<size>   The size of the page cache to use for the migration
                                                      process. The general rule is that values up to the
                                                      size of the database proportionally increase
                                                      performance.
                                                      Default: 8m
                               --to-format=<format name>
                                                    Name of the format to migrate the store to. If this
                                                      option is not specified, the tool will migrate the
                                                      database store to the latest version of the format
                                                      it is currently on.
                               --verbose            Enable verbose output.""");
    }
}
