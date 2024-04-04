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
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Supplier;
import org.apache.commons.lang3.SystemUtils;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.server.startup.Bootloader;
import org.neo4j.server.startup.EnhancedExecutionContext;
import org.neo4j.server.startup.Environment;
import org.neo4j.server.startup.ValidateConfigCommand;
import org.neo4j.server.startup.validation.ConfigValidationHelper;
import org.neo4j.server.startup.validation.ConfigValidationIssue;
import org.neo4j.server.startup.validation.ConfigValidator;
import org.neo4j.server.startup.validation.Neo4jConfigValidator;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import picocli.CommandLine;

@Neo4jLayoutExtension
class ValidateConfigCommandIT {

    private static final String SKIPPED_PART = "Skipped validation part";
    private static final String CONFIG_WITH_APOC_SETTING =
            """
                    dbms.ssl.policy.bolt.enabled=true
                    db.tx_log.rotation.retention_policy=3 days

                    db.tx_log.rotation.size=1G

                    #dbms.ssl.policy.bolt.base_directory=certificates/bolt

                    #Apoc setting that should not be here
                    apoc.export.file.enabled=true

                    #foo
                    server.jvm.additional=-XX:+UnlockDiagnosticVMOptions
                    server.jvm.additional=-XX:+DebugNonSafepoints

                    #A setting with no value
                    server.directories.data=

                    #Second apoc setting that should not be here
                    apoc.trigger.refresh=60000

                    #Other unrecognized setting that should be handled the regular way
                    unrecognized.setting

                    server.config.strict_validation.enabled=false
                    """;

    @Inject
    private Neo4jLayout neo4jLayout;

    @Test
    void shouldValidateValidNeo4jConfig() throws IOException {
        createConfigFileInDefaultLocation(MigrateConfigCommandTest.MIGRATED_CONFIG);
        var result = runValidateConfigCommand();
        assertEquals(0, result.exitCode);
        assertThat(result.out)
                .containsSubsequence(
                        "Validating Neo4j configuration",
                        "No issues found.",
                        SKIPPED_PART,
                        "No issues found.",
                        SKIPPED_PART,
                        "No issues found.");
    }

    @Test
    void shouldWarnOnApocSettingInNeo4jConfig() throws IOException {
        createConfigFileInDefaultLocation(CONFIG_WITH_APOC_SETTING);
        var result = runValidateConfigCommand();
        assertEquals(0, result.exitCode);
        assertThat(result.out)
                .containsSubsequence(
                        "Validating Neo4j configuration",
                        "3 issues found.",
                        SKIPPED_PART,
                        "No issues found.",
                        SKIPPED_PART,
                        "No issues found.")
                .contains(
                        "Warning: Setting 'apoc.export.file.enabled' for APOC was found in the configuration file. In Neo4j v5, APOC settings must be in their own configuration file called apoc.conf",
                        "Warning: Unrecognized setting. No declared setting with name: unrecognized.setting.",
                        "Warning: Setting 'apoc.trigger.refresh' for APOC was found in the configuration file. In Neo4j v5, APOC settings must be in their own configuration file called apoc.conf");
    }

    private Path createConfigFileInDefaultLocation(String content) throws IOException {
        return createConfigFile(Config.DEFAULT_CONFIG_DIR_NAME, content);
    }

    private Path createConfigFile(String dir, String content) throws IOException {
        content = maybeChangeLineSeparators(content);
        var configDir = neo4jLayout.homeDirectory().resolve(dir);
        Files.createDirectories(configDir);
        Path confFile = configDir.resolve(Config.DEFAULT_CONFIG_FILE_NAME);
        Files.writeString(confFile, content);
        return confFile;
    }

    private String maybeChangeLineSeparators(String text) {
        if (SystemUtils.IS_OS_WINDOWS) {
            return text.replace("\n", "\r\n");
        } else {
            return text;
        }
    }

    private Result runValidateConfigCommand(String... args) throws IOException {
        return runValidateConfigCommand(this.getClass().getClassLoader(), args);
    }

    private Result runValidateConfigCommand(ClassLoader classLoader, String... args) throws IOException {
        var homeDir = neo4jLayout.homeDirectory().toAbsolutePath();
        var configDir = homeDir.resolve(Config.DEFAULT_CONFIG_DIR_NAME);
        var out = new Output();
        var err = new Output();

        var environment = new Environment(
                out.printStream,
                err.printStream,
                (prop) -> "NEO4J_HOME".equals(prop) ? homeDir.toString() : null,
                (prop) -> null,
                Runtime.version());
        try (var bootloader = new Bootloader.Dbms(environment, false, false)) {

            var ctx = new EnhancedExecutionContext(
                    homeDir,
                    configDir,
                    out.printStream,
                    err.printStream,
                    new DefaultFileSystemAbstraction(),
                    () -> bootloader,
                    classLoader);

            ConfigValidator.Factory factory = getConfigValidator(bootloader.confFile());
            var helper = new ConfigValidationHelper(factory);

            var command = CommandLine.populateCommand(new ValidateConfigCommand(ctx, helper), args);

            try {
                int exitCode = command.call();
                return new Result(exitCode, out.toString(), err.toString());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private ConfigValidator.Factory getConfigValidator(Path configPath) {
        ConfigValidator NO_OP = new ConfigValidator() {
            @Override
            public List<ConfigValidationIssue> validate() {
                return List.of();
            }

            @Override
            public String getLabel() {
                return SKIPPED_PART;
            }
        };

        return new ConfigValidator.Factory() {
            @Override
            public ConfigValidator getNeo4jValidator(Supplier<Config> config) {
                return new Neo4jConfigValidator(config, configPath);
            }

            @Override
            public ConfigValidator getLog4jUserValidator(Supplier<Config> config) {
                return NO_OP;
            }

            @Override
            public ConfigValidator getLog4jServerValidator(Supplier<Config> config) {
                return NO_OP;
            }
        };
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
