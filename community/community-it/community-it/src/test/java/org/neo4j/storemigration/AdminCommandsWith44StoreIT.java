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
package org.neo4j.storemigration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.neo4j.cli.AbstractCommand;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.commandline.dbms.LoadCommand;
import org.neo4j.commandline.dbms.StoreInfoCommand;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.ZippedStoreCommunity;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import picocli.CommandLine;

/**
 * Tests how admin commands deal with being exposed to legacy 4.4 stores.
 * Only admin commands working with stores are tested.
 * Consistency check with legacy store is tested with other consistency check tests.
 * Store copy and migration commands are extensively tested in migration tests,
 * because using those commands is the official migration path.
 */
@Neo4jLayoutExtension
class AdminCommandsWith44StoreIT {

    @Inject
    private Neo4jLayout neo4jLayout;

    @Inject
    private FileSystemAbstraction fileSystemAbstraction;

    @Test
    void testStoreInfo() throws IOException {
        Path homeDir = neo4jLayout.homeDirectory();
        ZippedStoreCommunity.AF430_V44_ALL.unzip(homeDir);

        var result = runCommandFromSameJvm(StoreInfoCommand::new, "neo4j");
        assertEquals(0, result.exitCode());
        assertThat(result.out)
                .contains("Database name:                neo4j")
                .contains("Database in use:              false")
                .contains("Store format version:         record-aligned-0.1")
                .contains("Store format introduced in:   4.3.0")
                .contains("Store format superseded in:   5.0.0")
                .contains("Store needs recovery:         false");
    }

    @Test
    void testLod() throws IOException {
        InputStream source = getClass().getResourceAsStream("neo4j-44.dump");
        Path dumpFile = neo4jLayout.homeDirectory().resolve("test-db.dump");
        Files.copy(source, dumpFile);

        var result = runCommandFromSameJvm(
                LoadCommand::new,
                "test-db",
                "--from-path",
                neo4jLayout.homeDirectory().toString());

        assertEquals(0, result.exitCode());
    }

    private Result runCommandFromSameJvm(Function<ExecutionContext, AbstractCommand> commandFactory, String... args) {
        var homeDir = neo4jLayout.homeDirectory().toAbsolutePath();
        var configDir = homeDir.resolve("conf");
        var out = new Output();
        var err = new Output();

        var ctx = new ExecutionContext(homeDir, configDir, out.printStream, err.printStream, fileSystemAbstraction);

        var command = CommandLine.populateCommand(commandFactory.apply(ctx), args);

        try {
            int exitCode = command.call();
            return new Result(exitCode, out.toString(), err.toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static class Output {
        private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        private final PrintStream printStream = new PrintStream(buffer);

        @Override
        public String toString() {
            return buffer.toString(StandardCharsets.UTF_8);
        }
    }

    private record Result(int exitCode, String out, String err) {}
}
