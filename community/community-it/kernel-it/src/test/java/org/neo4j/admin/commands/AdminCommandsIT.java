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
package org.neo4j.admin.commands;

import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.neo4j.dbms.archive.Dumper.DUMP_EXTENSION;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import org.apache.commons.lang3.mutable.MutableObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.neo4j.cli.AbstractCommand;
import org.neo4j.cli.ContextInjectingFactory;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.commandline.admin.security.SetDefaultAdminCommand;
import org.neo4j.commandline.admin.security.SetInitialPasswordCommand;
import org.neo4j.commandline.dbms.DiagnosticsReportCommand;
import org.neo4j.commandline.dbms.DumpCommand;
import org.neo4j.commandline.dbms.LoadCommand;
import org.neo4j.commandline.dbms.MemoryRecommendationsCommand;
import org.neo4j.commandline.dbms.StoreInfoCommand;
import org.neo4j.commandline.dbms.UnbindCommand;
import org.neo4j.configuration.BootloaderSettings;
import org.neo4j.consistency.CheckCommand;
import org.neo4j.importer.ImportCommand;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import picocli.CommandLine;

@TestDirectoryExtension
// Config files created in our build server doesn't get the right permissions,
// However this test is only interested in the parsing of config entries with --expand-command option, so not necessary
// to run it on Windows.
@DisabledOnOs(OS.WINDOWS)
class AdminCommandsIT {
    @Inject
    private TestDirectory testDirectory;

    @Inject
    private FileSystemAbstraction fs;

    private ExecutionContext context;
    private Path dumpFolder;

    @BeforeEach
    void setup() throws Exception {
        final var out = mock(PrintStream.class);
        final var err = mock(PrintStream.class);
        final var confDir = testDirectory.directory("test.conf");
        final var home = testDirectory.homePath("home");
        dumpFolder = testDirectory.directory("dumpFolder");
        context = new ExecutionContext(home, confDir, out, err, testDirectory.getFileSystem());
        final var configFile = confDir.resolve("neo4j.conf");
        try (var outputStream = fs.openAsOutputStream(configFile, false);
                var printOut = new PrintStream(outputStream)) {
            printOut.printf("%s=%s%n", BootloaderSettings.initial_heap_size.name(), "$(expr 500)");
        }
        assertThat(fs).isInstanceOf(DefaultFileSystemAbstraction.class);
        Files.setPosixFilePermissions(configFile, Set.of(OWNER_READ, OWNER_WRITE));
    }

    @Test
    void shouldExpandCommands() throws IOException {
        final var testDb = "test";

        assertExpansionSuccess(new SetInitialPasswordCommand(context), "--expand-commands", "password");
        assertExpansionSuccess(new SetDefaultAdminCommand(context), "--expand-commands", "admin");
        assertExpansionSuccess(new StoreInfoCommand(context), "--expand-commands", "path");
        assertExpansionSuccess(new CheckCommand(context), "--expand-commands", "neo4j");
        assertExpansionSuccess(new DiagnosticsReportCommand(context), "--expand-commands");

        // ensure that a dump file exists
        final var dump = testDirectory.directory("dump");
        testDirectory.getFileSystem().mkdirs(dump);
        Files.writeString(
                dump.resolve(testDb + DUMP_EXTENSION),
                "ignored",
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE);
        assertExpansionSuccess(
                new LoadCommand(context),
                "--expand-commands",
                "--from-path=" + testDirectory.directory("dump").toAbsolutePath(),
                testDb);
        assertExpansionSuccess(new MemoryRecommendationsCommand(context), "--expand-commands");
        assertExpansionSuccess(
                new DumpCommand(context), "--expand-commands", testDb, "--to-path", dumpFolder.toString());

        // actual create the directory rather than rely on the load command to create it by accident
        fs.mkdirs(Neo4jLayout.of(testDirectory.homePath()).databasesDirectory());
        assertExpansionSuccess(new UnbindCommand(context), "--expand-commands");
    }

    @Test
    void shouldNotExpandCommands() {
        assertExpansionError(new SetInitialPasswordCommand(context), "password");
        assertExpansionError(new SetDefaultAdminCommand(context), "user");
        assertExpansionError(new StoreInfoCommand(context), "path");
        assertExpansionError(new CheckCommand(context), "neo4j");
        assertExpansionError(new DiagnosticsReportCommand(context));
        assertExpansionError(
                new LoadCommand(context),
                "--from-path=" + testDirectory.directory("dump").toAbsolutePath(),
                "test");
        assertExpansionError(new MemoryRecommendationsCommand(context));
        assertExpansionError(
                new ImportCommand.Full(context),
                "--nodes=" + testDirectory.createFile("foo.csv").toAbsolutePath());
        assertExpansionError(
                new ImportCommand.Incremental(context),
                "--force",
                "--nodes=" + testDirectory.createFile("foo.csv").toAbsolutePath());
        assertExpansionError(new DumpCommand(context), "test", "--to-path", dumpFolder.toString());
        assertExpansionError(new UnbindCommand(context));
    }

    private void assertExpansionSuccess(AbstractCommand command, String... args) {
        new CommandLine(command, new ContextInjectingFactory(context)).execute(args);
    }

    private void assertExpansionError(AbstractCommand command, String... args) {
        var exception = new MutableObject<Exception>();
        new CommandLine(command, new ContextInjectingFactory(context))
                .setExecutionExceptionHandler((ex, commandLine, parseResult) -> {
                    exception.setValue(ex);
                    return 1;
                })
                .execute(args);
        assertThat(exception.getValue())
                .hasMessageContaining("is a command, but config is not explicitly told to expand it.");
    }
}
