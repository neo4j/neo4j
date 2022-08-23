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
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.lang3.SystemUtils;
import org.junit.jupiter.api.Test;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import picocli.CommandLine;

@Neo4jLayoutExtension
class MigrateConfigCommandTest {

    private static final String OLD_CONFIG =
            """
                    #Some initial comment

                    #Some comment on a setting that will migrate
                    dbms.ssl.policy.bolt.enabled=true
                    dbms.tx_log.rotation.retention_policy= 3 days

                    #A comment sitting between settings

                    dbms.allow_upgrade=true
                    dbms.connector.bolt.enabled=true
                    dbms.connector.bolt.enabled=false

                    #A removed setting
                    dbms.record_format=high_limit
                    db.tx_log.preallocate=true

                    #A non existing setting
                    setting.that.does.not.exist=true

                    # A multi
                    #line comment
                    dbms.tx_log.rotation.size= 1G

                    #dbms.ssl.policy.bolt.base_directory=certificates/bolt

                    #foo
                    another.setting.that.does.not.exist=true

                    dbms.jvm.additional=-XX:+UnlockDiagnosticVMOptions
                    dbms.jvm.additional=-XX:+DebugNonSafepoints

                    #A setting with no value
                    server.directories.data=

                    #Tail comment
                    """;
    private static final String MIGRATED_CONFIG =
            """
                    #Some initial comment

                    #Some comment on a setting that will migrate
                    dbms.ssl.policy.bolt.enabled=true
                    db.tx_log.rotation.retention_policy=3 days

                    #A comment sitting between settings

                    # dbms.allow_upgrade=true REMOVED SETTING
                    # server.bolt.enabled=true DUPLICATE SETTING
                    server.bolt.enabled=false

                    #A removed setting
                    # dbms.record_format=high_limit REMOVED SETTING
                    db.tx_log.preallocate=true

                    #A non existing setting
                    # setting.that.does.not.exist=true UNKNOWN SETTING

                    # A multi
                    #line comment
                    db.tx_log.rotation.size=1G

                    #dbms.ssl.policy.bolt.base_directory=certificates/bolt

                    #foo
                    # another.setting.that.does.not.exist=true UNKNOWN SETTING

                    server.jvm.additional=-XX:+UnlockDiagnosticVMOptions
                    server.jvm.additional=-XX:+DebugNonSafepoints

                    #A setting with no value
                    server.directories.data=

                    #Tail comment
                    """;

    @Inject
    private Neo4jLayout neo4jLayout;

    @Test
    void shouldPrintUsageHelp() {
        var baos = new ByteArrayOutputStream();
        var command = new MigrateConfigCommand(new ExecutionContext(Path.of("."), Path.of(".")));
        try (var out = new PrintStream(baos)) {
            CommandLine.usage(command, new PrintStream(out), CommandLine.Help.Ansi.OFF);
        }
        assertThat(baos.toString().trim())
                .isEqualToIgnoringNewLines(
                        """
                                Migrate server configuration from the previous major version.

                                USAGE

                                migrate-configuration [-h] [--expand-commands] [--verbose] [--from-path=<path>]
                                                      [--to-path=<path>]

                                DESCRIPTION

                                Migrate legacy configuration located in source configuration directory to the
                                current format. The new version will be written in a target configuration
                                directory. The default location for both the source and target configuration
                                directory is the configuration directory specified by NEO_CONF or the default
                                configuration directory for this installation. If the source and target
                                directories are the same, the original configuration files will be renamed.
                                Configuration provided using --additional-config option is not migrated.

                                OPTIONS

                                      --expand-commands    Allow command expansion in config value evaluation.
                                      --from-path=<path>   Path to the configuration directory used as a source
                                                             for the migration.
                                  -h, --help               Show this help message and exit.
                                      --to-path=<path>     Path to a directory where the migrated configuration
                                                             files should be written.
                                      --verbose            Enable verbose output.""");
    }

    @Test
    void shouldMigrateMinimalConfigWithAllSpecialCases() throws IOException {
        var configFile = createConfigFileInDefaultLocation(OLD_CONFIG);
        var result = runConfigMigrationCommand();
        assertEquals(0, result.exitCode);
        assertThat(Files.readString(configFile)).isEqualTo(maybeChangeLineSeparators(MIGRATED_CONFIG));
    }

    @Test
    void shouldLogChanges() throws IOException {
        createConfigFileInDefaultLocation(OLD_CONFIG);
        var result = runConfigMigrationCommand();
        assertEquals(0, result.exitCode);
        assertThat(result.err)
                .contains("server.bolt.enabled=true REMOVED DUPLICATE")
                .contains("setting.that.does.not.exist=true REMOVED UNKNOWN")
                .contains("another.setting.that.does.not.exist=true REMOVED UNKNOWN");

        assertThat(result.out)
                .contains("dbms.ssl.policy.bolt.enabled=true UNCHANGED")
                .contains(
                        "dbms.tx_log.rotation.retention_policy=3 days MIGRATED -> db.tx_log.rotation.retention_policy=3 days")
                .contains("dbms.allow_upgrade=true REMOVED")
                .contains("dbms.connector.bolt.enabled=false MIGRATED -> server.bolt.enabled=false")
                .contains("dbms.record_format=high_limit REMOVED")
                .contains("db.tx_log.preallocate=true UNCHANGED")
                .contains("dbms.tx_log.rotation.size=1G MIGRATED -> db.tx_log.rotation.size=1G")
                .contains(
                        "dbms.jvm.additional=-XX:+UnlockDiagnosticVMOptions MIGRATED -> server.jvm.additional=-XX:+UnlockDiagnosticVMOptions")
                .contains(
                        "dbms.jvm.additional=-XX:+DebugNonSafepoints MIGRATED -> server.jvm.additional=-XX:+DebugNonSafepoints")
                .contains("server.directories.data= UNCHANGED");
    }

    @Test
    void shouldRenameOverriddenFile() throws IOException {
        var configFile = createConfigFileInDefaultLocation("dbms.tx_log.rotation.size=1G");
        var result = runConfigMigrationCommand();
        assertEquals(0, result.exitCode);
        assertThat(Files.readString(configFile)).isEqualToIgnoringNewLines("db.tx_log.rotation.size=1G");
        var originalConfigFile = configFile.getParent().resolve("neo4j.conf.old");
        assertThat(Files.readString(originalConfigFile)).isEqualTo("dbms.tx_log.rotation.size=1G");
    }

    @Test
    void providedSourcePathMustExist() {
        var result = runConfigMigrationCommand(
                "--from-path", neo4jLayout.homeDirectory().resolve("somewhere").toString());
        assertEquals(1, result.exitCode);
        assertThat(result.err).contains("Provided path '").contains("somewhere' is not an existing directory");
    }

    @Test
    void sourceConfigFileMustExist() throws IOException {
        var sourceDir = neo4jLayout.homeDirectory().resolve("somewhere");
        Files.createDirectories(sourceDir);
        var result = runConfigMigrationCommand("--from-path", sourceDir.toString());
        assertEquals(1, result.exitCode);
        assertThat(result.err)
                .contains("Resolved source file '")
                .contains(Path.of("somewhere", "neo4j.conf") + "' does not exist");
    }

    @Test
    void providedTargetPathMustExist() throws IOException {
        createConfigFileInDefaultLocation("dbms.tx_log.rotation.size=1G");
        var result = runConfigMigrationCommand(
                "--to-path", neo4jLayout.homeDirectory().resolve("somewhere").toString());
        assertEquals(1, result.exitCode);
        assertThat(result.err).contains("Provided path '").contains("somewhere' is not an existing directory");
    }

    @Test
    void resolvedTargetPathMustExist() throws IOException {
        var originalConfigFile = createConfigFile("somewhere", "dbms.tx_log.rotation.size=1G");
        var result = runConfigMigrationCommand(
                "--from-path", originalConfigFile.getParent().toString());
        assertEquals(1, result.exitCode);
        assertThat(result.err).contains("Target path '").contains("conf' is not an existing directory");
    }

    @Test
    void shouldUseProvidedSourcePath() throws IOException {
        var originalConfigFile = createConfigFile("another-conf-dir", "dbms.tx_log.rotation.size=1G");
        Path targetDir = neo4jLayout.homeDirectory().resolve("conf");
        Files.createDirectories(targetDir);
        var result = runConfigMigrationCommand(
                "--from-path", originalConfigFile.getParent().toString());
        assertEquals(0, result.exitCode);
        var migratedConfigFile = targetDir.resolve("neo4j.conf");
        assertThat(Files.readString(migratedConfigFile)).isEqualToIgnoringNewLines("db.tx_log.rotation.size=1G");
        assertThat(Files.readString(originalConfigFile)).isEqualTo("dbms.tx_log.rotation.size=1G");
    }

    @Test
    void shouldUseProvidedTargetPath() throws IOException {
        var originalConfigFile = createConfigFileInDefaultLocation("dbms.tx_log.rotation.size=1G");
        Path targetDir = neo4jLayout.homeDirectory().resolve("another-conf-dir");
        Files.createDirectories(targetDir);
        var result = runConfigMigrationCommand("--to-path", targetDir.toString());
        assertEquals(0, result.exitCode);
        var migratedConfigFile = targetDir.resolve("neo4j.conf");
        assertThat(Files.readString(migratedConfigFile)).isEqualToIgnoringNewLines("db.tx_log.rotation.size=1G");
        assertThat(Files.readString(originalConfigFile)).isEqualTo("dbms.tx_log.rotation.size=1G");
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

    private Result runConfigMigrationCommand(String... args) {
        var homeDir = neo4jLayout.homeDirectory().toAbsolutePath();
        var configDir = homeDir.resolve(Config.DEFAULT_CONFIG_DIR_NAME);
        var out = new Output();
        var err = new Output();

        var ctx = new ExecutionContext(
                homeDir, configDir, out.printStream, err.printStream, new DefaultFileSystemAbstraction());

        var command = CommandLine.populateCommand(new MigrateConfigCommand(ctx), args);

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
