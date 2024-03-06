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
import static org.mockito.Mockito.mock;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.dbms.archive.Dumper;
import org.neo4j.io.fs.FileSystemAbstraction;
import picocli.CommandLine;

class DumpCommandTest {
    @Test
    void printUsageHelp() {
        final var baos = new ByteArrayOutputStream();
        final var command = new DumpCommand(new ExecutionContext(Path.of("."), Path.of("."))) {
            @Override
            protected Dumper createDumper(FileSystemAbstraction fs) {
                return mock(Dumper.class);
            }
        };
        try (var out = new PrintStream(baos)) {
            CommandLine.usage(command, new PrintStream(out), CommandLine.Help.Ansi.OFF);
        }
        assertThat(baos.toString().trim())
                .isEqualToIgnoringNewLines(
                        """
                Dump a database into a single-file archive.

                USAGE

                dump [-h] [--expand-commands] [--verbose] [--overwrite-destination
                     [=true|false]] [--additional-config=<file>] [--to-path=<path> |
                     --to-stdout] <database>

                DESCRIPTION

                Dump a database into a single-file archive. The archive can be used by the load
                command. <to-path> should be a directory (in which case a file called
                <database>.dump will be created), or --to-stdout can be supplied to use
                standard output. If neither --to-path or --to-stdout is supplied `server.
                directories.dumps.root` setting will be used as destination. It is not possible
                to dump a database that is mounted in a running Neo4j server.

                PARAMETERS

                      <database>          Name of the database to dump. Can contain * and ? for
                                            globbing. Note that * and ? have special meaning in
                                            some shells and might need to be escaped or used
                                            with quotes.

                OPTIONS

                      --additional-config=<file>
                                          Configuration file with additional configuration.
                      --expand-commands   Allow command expansion in config value evaluation.
                  -h, --help              Show this help message and exit.
                      --overwrite-destination[=true|false]
                                          Overwrite any existing dump file in the destination
                                            folder.
                                            Default: false
                      --to-path=<path>    Destination folder of database dump.
                      --to-stdout         Use standard output as destination for database dump.
                      --verbose           Enable verbose output.""");
    }
}
