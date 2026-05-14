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
        assertThat(baos.toString().trim()).isEqualToIgnoringNewLines("""
                         Migrate a database

                         USAGE

                         migrate [-h] [--expand-commands] [--force-btree-indexes-to-range] [--verbose]
                                 [--additional-config=<file>] [--max-off-heap-memory=<size>]
                                 [--pagecache=<size>] [--to-format=standard|high_limit|aligned|block]
                                 <database>

                         DESCRIPTION

                         Migrates a database from one format to another or between versions of the same
                         format. It always migrates the database to the latest combination of major and
                         minor version of the target format.

                         PARAMETERS

                               <database>           Name of the database to migrate. Can contain * and ?
                                                      for globbing. Note that * and ? have special
                                                      meaning in some shells and might need to be
                                                      escaped or used with quotes.

                         OPTIONS

                               --additional-config=<file>
                                                    Configuration file with additional configuration.
                               --expand-commands    Allow command expansion in config value evaluation.
                               --force-btree-indexes-to-range
                                                    (Deprecated and will be removed. This option was
                                                      only applicable when migrating from v4 to v5.)
                                                      Special option for automatically turning all BTREE
                                                      indexes/constraints into RANGE. Be aware that
                                                      RANGE indexes are not always the optimal
                                                      replacement of BTREEs and performance may be
                                                      affected while the new indexes are populated. See
                                                      the Neo4j v5 migration guide online for more
                                                      information. The newly created indexes will be
                                                      populated in the background on the first database
                                                      start up following the migration and users should
                                                      monitor the successful completion of that process.
                           -h, --help               Show this help message and exit.
                               --max-off-heap-memory=<size>
                                                    Maximum off-heap memory that the command can use for
                                                      page cache and various caching data structures to
                                                      improve performance. Use this option to tune the
                                                      command memory usage; the command does not use the
                                                      server.memory.pagecache.size configuration setting
                                                      for this purpose. Values can be plain numbers,
                                                      such as 10000000, or, for example, 20G for 20
                                                      gigabytes, or 70%, which will amount to 70% of
                                                      currently free memory on the machine.
                                                      Default: 90%
                               --pagecache=<size>   (Deprecated in favor of --max-off-heap-memory) The
                                                      size of the page cache to use for the migration
                                                      process. The general rule is that values up to the
                                                      size of the database proportionally increase
                                                      performance.
                               --to-format=standard|high_limit|aligned|block
                                                    Name of the format to migrate the store to. If the
                                                      format is specified, the target database is
                                                      migrated to the latest known combination of MAJOR
                                                      and MINOR versions of the specified format. If not
                                                      specified, the tool migrates the target database
                                                      to the latest known combination of MAJOR and MINOR
                                                      versions of the current format.
                               --verbose            Enable verbose output.""");
    }
}
