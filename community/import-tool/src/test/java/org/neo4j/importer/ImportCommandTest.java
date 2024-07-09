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
package org.neo4j.importer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.batchimport.api.input.IdType;
import org.neo4j.cli.ContextInjectingFactory;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.cloud.storage.SchemeFileSystemAbstraction;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.importer.ImportCommand.InputFilesGroup;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import picocli.CommandLine;
import picocli.CommandLine.Help;

@TestDirectoryExtension
class ImportCommandTest {
    @Inject
    private TestDirectory testDir;

    @Test
    void usageHelp() {
        final var command = new ImportCommand();
        final var help = getUsageHelp(command);
        // All non-hidden subcommands
        var subcommands = help.subcommands().keySet();
        // Incremental should not be shown in community
        var expectedSubcommands = Set.of("full", "help");
        assertThat(subcommands).isEqualTo(expectedSubcommands);
    }

    private static final String[] sharedOptions = {
        "-h",
        "--expand-commands",
        "--verbose",
        "--additional-config",
        "--report-file",
        "--id-type",
        "--input-encoding",
        "--ignore-extra-columns",
        "--multiline-fields",
        "--ignore-empty-strings",
        "--trim-strings",
        "--legacy-style-quoting",
        "--delimiter",
        "--array-delimiter",
        "--quote",
        "--read-buffer-size",
        "--max-off-heap-memory",
        "--high-parallel-io",
        "--threads",
        "--bad-tolerance",
        "--skip-bad-entries-logging",
        "--skip-bad-relationships",
        "--skip-duplicate-nodes",
        "--strict",
        "--normalize-types",
        "--nodes",
        "--relationships",
        "--auto-skip-subsequent-headers"
    };

    private static final String[] sharedPositionals = {"<database>"};

    @Test
    void printUsageHelpForSubcommandFull() {
        final var command = new ImportCommand.Full(getExecutionContext());
        final var help = getUsageHelp(command);
        final var options = getOptions(help);
        var expectedOptions = new ArrayList<>(List.of(sharedOptions));
        expectedOptions.addAll(List.of("--overwrite-destination", "--format"));
        final var positionals = getPositionals(help);
        final var expectedPositionals = List.of(sharedPositionals);

        assertThat(options.toArray()).containsOnly(expectedOptions.toArray());
        assertThat(positionals.toArray()).containsOnly(expectedPositionals.toArray());
    }

    @Test
    void printUsageHelpForSubcommandIncremental() {
        final var command = new ImportCommand.Incremental(getExecutionContext());
        final var help = getUsageHelp(command);
        final var options = getOptions(help);
        var expectedOptions = new ArrayList<>(List.of(sharedOptions));
        expectedOptions.addAll(List.of("--stage", "--force"));
        final var positionals = getPositionals(help);
        final var expectedPositionals = List.of(sharedPositionals);

        assertThat(options.toArray()).containsOnly(expectedOptions.toArray());
        assertThat(positionals.toArray()).containsOnly(expectedPositionals.toArray());
    }

    @Test
    void shouldAllowDifferentCasingForIdType() {
        var tempFileName = testDir.createFile("dummy").toString();
        var requiredArgs = List.of("--nodes", tempFileName, "--relationships", tempFileName);
        assertIdTypeAliases(requiredArgs, List.of("ACTUAL", "actual"), IdType.ACTUAL);
        assertIdTypeAliases(requiredArgs, List.of("STRING", "string"), IdType.STRING);
        assertIdTypeAliases(requiredArgs, List.of("INTEGER", "integer"), IdType.INTEGER);
    }

    private void assertIdTypeAliases(List<String> requiredArgs, List<String> aliases, IdType idType) {
        for (var alias : aliases) {
            var command = new ImportCommand.Full(getExecutionContext());
            var args = Stream.concat(Stream.of("--id-type", alias), requiredArgs.stream());
            new CommandLine(command).parseArgs(args.toArray(String[]::new));
            assertThat(command.idType).isEqualTo(idType);
        }
    }

    @Test
    void shouldAllowAliasesForIncrementalStage() {
        var tempFileName = testDir.createFile("dummy").toString();
        var requiredArgs = List.of("--force", "--nodes", tempFileName, "--relationships", tempFileName);

        assertIncrementalStageAliases(
                requiredArgs, List.of("prepare", "PREPARE", "1"), ImportCommand.IncrementalStage.prepare);
        assertIncrementalStageAliases(
                requiredArgs, List.of("build", "BUILD", "2"), ImportCommand.IncrementalStage.build);
        assertIncrementalStageAliases(
                requiredArgs, List.of("merge", "MERGE", "3"), ImportCommand.IncrementalStage.merge);
    }

    private void assertIncrementalStageAliases(
            List<String> requiredArgs, List<String> aliases, ImportCommand.IncrementalStage stage) {
        for (var alias : aliases) {
            var command = new ImportCommand.Incremental(getExecutionContext());
            var args = Stream.concat(Stream.of("--stage", alias), requiredArgs.stream());
            new CommandLine(command).parseArgs(args.toArray(String[]::new));
            assertThat(command.stage).isEqualTo(stage);
        }
    }

    private ExecutionContext getExecutionContext() {
        return new ExecutionContext(Path.of("."), Path.of("."));
    }

    private Help getUsageHelp(Object command) {
        final var ctx = getExecutionContext();
        return new CommandLine(command, new ContextInjectingFactory(ctx)).getHelp();
    }

    private Set<String> getOptions(Help help) {
        var options = new HashSet<String>();
        for (var option : help.commandSpec().options()) {
            if (option.hidden()) {
                continue;
            }
            // Pick the first name
            options.add(option.names()[0]);
        }
        return options;
    }

    private Set<String> getPositionals(Help help) {
        var positionals = new HashSet<String>();
        for (var positional : help.commandSpec().positionalParameters()) {
            if (positional.hidden()) {
                continue;
            }
            positionals.add(positional.paramLabel());
        }
        return positionals;
    }

    @Test
    void shouldKeepSpecifiedNeo4jHomeWhenAdditionalConfigIsPresent() {
        // given
        final var homeDir = testDir.directory("other", "place");
        final var additionalConfigFile = testDir.createFile("empty.conf");
        final var ctx = new ExecutionContext(
                homeDir, testDir.directory("conf"), System.out, System.err, testDir.getFileSystem());
        // Does not matter which command Full/Incremental
        final var command = new ImportCommand.Full(ctx);
        final var foo = testDir.createFile("foo.csv");

        CommandLine.populateCommand(
                command,
                "--additional-config",
                additionalConfigFile.toAbsolutePath().toString(),
                "--nodes=" + foo.toAbsolutePath());

        // when
        Config resultingConfig = command.loadNeo4jConfig("");

        // then
        assertEquals(homeDir, resultingConfig.get(GraphDatabaseSettings.neo4j_home));
    }

    @Test
    void shouldKeepSpecifiedNeo4jHomeWhenNoAdditionalConfigIsPresent() {
        // given
        final var homeDir = testDir.directory("other", "place");
        final var ctx = new ExecutionContext(
                homeDir, testDir.directory("conf"), System.out, System.err, testDir.getFileSystem());
        // Does not matter which command Full/Incremental
        final var command = new ImportCommand.Full(ctx);
        final var foo = testDir.createFile("foo.csv");

        CommandLine.populateCommand(command, "--nodes=" + foo.toAbsolutePath());

        // when
        Config resultingConfig = command.loadNeo4jConfig("");

        // then
        assertEquals(homeDir, resultingConfig.get(GraphDatabaseSettings.neo4j_home));
    }

    @Nested
    class ParseNodeFilesGroup {
        @Test
        void illegalEqualsPosition() {
            assertThrows(IllegalArgumentException.class, () -> ImportCommand.parseNodeFilesGroup("=foo.csv"));
            assertThrows(IllegalArgumentException.class, () -> ImportCommand.parseNodeFilesGroup("foo="));
        }

        @Test
        void validateFileExistence() {
            assertThrows(IllegalArgumentException.class, () -> ImportCommand.parseNodeFilesGroup("nonexisting.file")
                    .toPaths(testDir.getFileSystem()));
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void filesWithoutLabels(boolean useURIs) {
            final var foo = testDir.createFile("foo.csv");
            final var bar = testDir.createFile("bar.csv");
            final var pathStr = useURIs ? foo.toUri() + "," + bar.toUri() : foo + "," + bar;
            final var g = ImportCommand.parseNodeFilesGroup(pathStr);
            assertThat(g.key).isEmpty();
            assertPathsFound(testDir, g, foo, bar);
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void singleLabel(boolean useURIs) {
            final var foo = testDir.createFile("foo.csv");
            final var bar = testDir.createFile("bar.csv");
            final var pathStr = useURIs ? "BANANA=" + foo.toUri() + "," + bar.toUri() : "BANANA=" + foo + "," + bar;
            final var g = ImportCommand.parseNodeFilesGroup(pathStr);
            assertThat(g.key).containsOnly("BANANA");
            assertPathsFound(testDir, g, foo, bar);
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void multipleLabels(boolean useURIs) {
            final var foo = testDir.createFile("foo.csv");
            final var bar = testDir.createFile("bar.csv");
            final var pathStr = useURIs
                    ? ":APPLE::KIWI : BANANA=" + foo.toUri() + "," + bar.toUri()
                    : ":APPLE::KIWI : BANANA=" + foo + "," + bar;
            final var g = ImportCommand.parseNodeFilesGroup(pathStr);
            assertThat(g.key).containsOnly("BANANA", "KIWI", "APPLE");
            assertPathsFound(testDir, g, foo, bar);
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void filesRegex(boolean useURIs) {
            final var foo1 = testDir.createFile("foo-1.csv");
            final var foo2 = testDir.createFile("foo-2.csv");
            testDir.createFile("foo-X.csv");
            final var pathStr = useURIs
                    ? "BANANA=" + testDir.absolutePath().toUri() + "/foo-[0-9].csv"
                    : "BANANA=" + testDir.absolutePath() + File.separator + "foo-[0-9].csv";
            final var g = ImportCommand.parseNodeFilesGroup(pathStr);
            assertThat(g.key).containsOnly("BANANA");
            assertPathsFound(testDir, g, foo1, foo2);
        }
    }

    @Nested
    class ParseRelationshipFilesGroup {
        @Test
        void illegalEqualsPosition() {
            assertThrows(IllegalArgumentException.class, () -> ImportCommand.parseRelationshipFilesGroup("=foo.csv"));
            assertThrows(IllegalArgumentException.class, () -> ImportCommand.parseRelationshipFilesGroup("foo="));
        }

        @Test
        void validateFileExistence() {
            assertThrows(
                    IllegalArgumentException.class, () -> ImportCommand.parseRelationshipFilesGroup("nonexisting.file")
                            .toPaths(testDir.getFileSystem()));
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void filesWithoutLabels(boolean useURIs) {
            final var foo = testDir.createFile("foo.csv");
            final var bar = testDir.createFile("bar.csv");
            final var pathStr = useURIs ? foo.toUri() + "," + bar.toUri() : foo + "," + bar;
            final var g = ImportCommand.parseRelationshipFilesGroup(pathStr);
            assertThat(g.key).isEmpty();
            assertPathsFound(testDir, g, foo, bar);
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void withDefaultRelType(boolean useURIs) {
            final var foo = testDir.createFile("foo.csv");
            final var bar = testDir.createFile("bar.csv");
            final var pathStr = useURIs ? "BANANA=" + foo.toUri() + "," + bar.toUri() : "BANANA=" + foo + "," + bar;
            final var g = ImportCommand.parseRelationshipFilesGroup(pathStr);
            assertThat(g.key).isEqualTo("BANANA");
            assertPathsFound(testDir, g, foo, bar);
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void filesRegex(boolean useURIs) {
            final var foo1 = testDir.createFile("foo-1.csv");
            final var foo2 = testDir.createFile("foo-2.csv");
            testDir.createFile("foo-X.csv");
            final var pathStr = useURIs
                    ? "BANANA=" + testDir.absolutePath().toUri() + "/foo-[0-9].csv"
                    : "BANANA=" + testDir.absolutePath() + File.separator + "foo-[0-9].csv";
            final var g = ImportCommand.parseRelationshipFilesGroup(pathStr);
            assertThat(g.key).isEqualTo("BANANA");
            assertPathsFound(testDir, g, foo1, foo2);
        }
    }

    private static void assertPathsFound(TestDirectory dir, InputFilesGroup<?> group, Path... expectedPaths) {
        // the groups will have either local paths or file URIs so will be handled by the simple scheme system below
        try (var fs = new SchemeFileSystemAbstraction(dir.getFileSystem())) {
            assertThat(group.toPaths(fs)).containsOnly(expectedPaths);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }
}
