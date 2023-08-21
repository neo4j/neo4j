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
import static org.neo4j.cli.AbstractAdminCommand.COMMAND_CONFIG_FILE_NAME_PATTERN;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_scan_prefetch;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.neo4j.cli.AbstractAdminCommand;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import picocli.CommandLine;

@Neo4jLayoutExtension
class AdminCommandConfigurationTest {

    private static final String COMMAND_CONFIG_FILE_NAME = "dbms-test";
    private final ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errBuffer = new ByteArrayOutputStream();

    @Inject
    private Neo4jLayout layout;

    @Test
    void adminConfigShouldOverrideNeo4jConfig() throws Exception {
        creteConfigFile(neo4jConfig(), pagecache_memory.name() + "=1MB", pagecache_scan_prefetch.name() + "=0");
        creteConfigFile(adminConfig(), pagecache_memory.name() + "=2MB");
        TestCommand executedCommand = runTestCommand();

        assertThat(executedCommand.config.get(pagecache_memory)).isEqualTo(ByteUnit.mebiBytes(2));
        assertThat(executedCommand.config.get(pagecache_scan_prefetch)).isEqualTo(0);
    }

    @Test
    void commandConfigShouldOverrideAdminConfig() throws Exception {
        creteConfigFile(adminConfig(), pagecache_memory.name() + "=1MB", pagecache_scan_prefetch.name() + "=0");
        creteConfigFile(commandConfig(), pagecache_memory.name() + "=2MB");
        TestCommand executedCommand = runTestCommand();

        assertThat(executedCommand.config.get(pagecache_memory)).isEqualTo(ByteUnit.mebiBytes(2));
        assertThat(executedCommand.config.get(pagecache_scan_prefetch)).isEqualTo(0);
    }

    @Test
    void additionalConfigOptionShouldOverrideCommandConfig() throws Exception {
        Path additionalConfig = additionalConfig();
        creteConfigFile(commandConfig(), pagecache_memory.name() + "=1MB", pagecache_scan_prefetch.name() + "=0");
        creteConfigFile(additionalConfig, pagecache_memory.name() + "=2MB");
        TestCommand executedCommand = runTestCommand("--additional-config", additionalConfig.toString());

        assertThat(executedCommand.config.get(pagecache_memory)).isEqualTo(ByteUnit.mebiBytes(2));
        assertThat(executedCommand.config.get(pagecache_scan_prefetch)).isEqualTo(0);
    }

    @Test
    void shouldFailWhenSuppliedAdditionalConfigDoesNotExist() throws Exception {
        Path additionalConfig = additionalConfig();
        runTestCommand("--additional-config", additionalConfig.toString());
        assertThat(errBuffer.toString()).contains("additional-config.conf does not exist");
    }

    @Test
    void shouldListConfigFilesIfVerbose() throws Exception {
        Path neo4jConfig = neo4jConfig();
        Path adminConfig = adminConfig();
        Path commandConfig = commandConfig();
        Path additionalConfig = additionalConfig();
        creteConfigFile(neo4jConfig, pagecache_memory.name() + "=1MB");
        creteConfigFile(commandConfig, pagecache_memory.name() + "=3MB");
        creteConfigFile(adminConfig, pagecache_memory.name() + "=2MB");
        creteConfigFile(additionalConfig, pagecache_memory.name() + "=4MB");
        runTestCommand("--verbose", "--additional-config", additionalConfig.toString());

        assertThat(outBuffer.toString())
                .containsSubsequence(
                        "Configuration files used (ordered by priority)",
                        additionalConfig.toString(),
                        commandConfig.toString(),
                        adminConfig.toString(),
                        neo4jConfig.toString());
    }

    @Test
    void shouldOnlyListExistingConfigFiles() throws Exception {
        Path neo4jConfig = neo4jConfig();
        Path adminConfig = adminConfig();
        Path commandConfig = commandConfig();
        Path additionalConfig = additionalConfig();
        creteConfigFile(adminConfig, pagecache_memory.name() + "=2MB");
        creteConfigFile(additionalConfig, pagecache_memory.name() + "=4MB");
        runTestCommand("--verbose", "--additional-config", additionalConfig.toString());

        String output = outBuffer.toString();
        assertThat(output)
                .containsSubsequence(
                        "Configuration files used (ordered by priority)",
                        additionalConfig.toString(),
                        adminConfig.toString());
        assertThat(output).doesNotContain(neo4jConfig.toString(), commandConfig.toString());
    }

    private void creteConfigFile(Path path, String... lines) throws IOException {
        Files.createDirectories(path.getParent());

        String content = Arrays.stream(lines).collect(Collectors.joining(System.lineSeparator()));
        Files.writeString(path, content);
    }

    private Path neo4jConfig() {
        return configDir().resolve(Config.DEFAULT_CONFIG_FILE_NAME);
    }

    private Path adminConfig() {
        return configDir().resolve(AbstractAdminCommand.ADMIN_CONFIG_FILE_NAME);
    }

    private Path commandConfig() {
        return configDir().resolve(String.format(COMMAND_CONFIG_FILE_NAME_PATTERN, COMMAND_CONFIG_FILE_NAME));
    }

    private Path additionalConfig() {
        return layout.homeDirectory().resolve("additional-config.conf").toAbsolutePath();
    }

    private Path configDir() {
        return layout.homeDirectory().toAbsolutePath().resolve(Config.DEFAULT_CONFIG_DIR_NAME);
    }

    private TestCommand runTestCommand(String... args) throws Exception {
        var homeDir = layout.homeDirectory().toAbsolutePath();
        var configDir = homeDir.resolve(Config.DEFAULT_CONFIG_DIR_NAME);

        var ctx = new ExecutionContext(
                homeDir,
                configDir,
                new PrintStream(outBuffer),
                new PrintStream(errBuffer),
                new DefaultFileSystemAbstraction());
        var command = CommandLine.populateCommand(new TestCommand(ctx), args);
        command.call();
        return command;
    }

    private class TestCommand extends AbstractAdminCommand {

        private Config config;

        protected TestCommand(ExecutionContext ctx) {
            super(ctx);
        }

        @Override
        protected void execute() throws Exception {
            config = createPrefilledConfigBuilder().build();
        }

        @Override
        protected Optional<String> commandConfigName() {
            return Optional.of(COMMAND_CONFIG_FILE_NAME);
        }
    }
}
