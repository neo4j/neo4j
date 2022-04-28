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
                .isEqualTo(String.format("Migrate database store%n"
                        + "%n"
                        + "USAGE%n"
                        + "%n"
                        + "migrate-store [--expand-commands] [--verbose] [--additional-config=<path>]%n"
                        + "              [--database=<database>] [--format-family=<format family>]%n"
                        + "              [--pagecache=<size>]%n"
                        + "%n"
                        + "DESCRIPTION%n"
                        + "%n"
                        + "Migrate database store%n"
                        + "%n"
                        + "OPTIONS%n"
                        + "%n"
                        + "      --verbose            Enable verbose output.%n"
                        + "      --expand-commands    Allow command expansion in config value evaluation.%n"
                        + "      --database=<database>%n"
                        + "                           Name of the database whose store to migrate.%n"
                        + "                             Default: neo4j%n"
                        + "      --format-family=<format family>%n"
                        + "                           Format family to migrate the store to. This option%n"
                        + "                             is supported only in combination with%n"
                        + "                             --storage-engine RECORD%n"
                        + "      --pagecache=<size>   The size of the page cache to use for the backup%n"
                        + "                             process.%n"
                        + "                             Default: 8m%n"
                        + "      --additional-config=<path>%n"
                        + "                           Configuration file to supply additional%n"
                        + "                             configuration in."));
    }
}
