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

import static java.lang.System.lineSeparator;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.configuration.BootloaderSettings.additional_jvm;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.STRING;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarOutputStream;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import org.apache.commons.lang3.SystemUtils;
import org.junit.jupiter.api.Test;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.SettingMigrator;
import org.neo4j.configuration.SettingMigrators;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.logging.InternalLog;
import org.neo4j.server.startup.Bootloader;
import org.neo4j.server.startup.EnhancedExecutionContext;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.jar.JarBuilder;
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
    static final String MIGRATED_CONFIG =
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

    private static final String OLD_CONFIG_APOC =
            """
                    #A removed setting
                    dbms.record_format=high_limit
                    db.tx_log.preallocate=true

                    #Apoc setting that should have been moved to separate file
                    apoc.trigger.refresh=50000

                    #A non existing setting
                    setting.that.does.not.exist=true

                    #Apoc setting that should have been moved to separate file
                    apoc.export.file.enabled=true
                    """;

    private static final String MIGRATED_CONFIG_APOC =
            """
                    #A removed setting
                    # dbms.record_format=high_limit REMOVED SETTING
                    db.tx_log.preallocate=true

                    #Apoc setting that should have been moved to separate file
                    # apoc.trigger.refresh=50000 REMOVED SETTING

                    #A non existing setting
                    # setting.that.does.not.exist=true UNKNOWN SETTING

                    #Apoc setting that should have been moved to separate file
                    # apoc.export.file.enabled=true REMOVED SETTING
                    """;

    private static final String NEW_CONFIG_APOC =
            """
                    apoc.trigger.refresh=50000
                    apoc.export.file.enabled=true
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
        assertThat(readFileIgnoreJvmRecommendations(configFile)).isEqualTo(maybeChangeLineSeparators(MIGRATED_CONFIG));
        assertFalse(Files.exists(configFile.resolve("apoc.conf"))); // No apoc conf since there were no apoc settings.
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
                .contains("server.directories.data= UNCHANGED")
                .doesNotContain("APOC");
    }

    @Test
    void shouldMigrateApocSettings() throws IOException {
        var configFile = createConfigFileInDefaultLocation(OLD_CONFIG_APOC);
        var newApocConf = configFile.resolveSibling("apoc.conf");
        var result = runConfigMigrationCommand();
        assertEquals(0, result.exitCode);
        assertThat(readFileIgnoreJvmRecommendations(configFile))
                .isEqualTo(maybeChangeLineSeparators(MIGRATED_CONFIG_APOC));
        assertThat(Files.readString(newApocConf)).isEqualTo(maybeChangeLineSeparators(NEW_CONFIG_APOC));

        assertThat(result.err).contains("setting.that.does.not.exist=true REMOVED UNKNOWN");

        assertThat(result.out)
                .contains("dbms.record_format=high_limit REMOVED")
                .contains("db.tx_log.preallocate=true UNCHANGED")
                .contains("apoc.trigger.refresh=50000 REMOVED")
                .contains("apoc.export.file.enabled=true REMOVED")
                .contains("APOC settings were present in the neo4j.conf file.")
                .contains("APOC settings moved to separate file:");
    }

    @Test
    void shouldPreserveOldApocFiles() throws IOException {
        var configFile = createConfigFileInDefaultLocation(OLD_CONFIG_APOC);
        var apocConf = configFile.resolveSibling("apoc.conf");

        Files.writeString(apocConf, "Old apoc.conf", StandardCharsets.UTF_8);

        var result = runConfigMigrationCommand();
        assertEquals(0, result.exitCode);
        assertThat(readFileIgnoreJvmRecommendations(configFile))
                .isEqualTo(maybeChangeLineSeparators(MIGRATED_CONFIG_APOC));
        assertThat(Files.readString(apocConf)).isEqualTo(maybeChangeLineSeparators(NEW_CONFIG_APOC));
        Path oldApocConf = apocConf.resolveSibling(apocConf.getFileName() + ".old");
        assertThat(oldApocConf).exists();
        assertThat(Files.readString(oldApocConf, StandardCharsets.UTF_8)).contains("Old apoc.conf");

        assertThat(result.out)
                .contains("APOC settings were present in the neo4j.conf file.")
                .contains("Keeping original apoc.conf file at")
                .contains("APOC settings moved to separate file:");
    }

    @Test
    void shouldRenameOverriddenFile() throws IOException {
        var configFile = createConfigFileInDefaultLocation("dbms.tx_log.rotation.size=1G");
        var result = runConfigMigrationCommand();
        assertEquals(0, result.exitCode);
        assertThat(readFileIgnoreJvmRecommendations(configFile))
                .isEqualToIgnoringNewLines("db.tx_log.rotation.size=1G");
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
        assertThat(readFileIgnoreJvmRecommendations(migratedConfigFile))
                .isEqualToIgnoringNewLines("db.tx_log.rotation.size=1G");
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
        assertThat(readFileIgnoreJvmRecommendations(migratedConfigFile))
                .isEqualToIgnoringNewLines("db.tx_log.rotation.size=1G");
        assertThat(Files.readString(originalConfigFile)).isEqualTo("dbms.tx_log.rotation.size=1G");
    }

    @Test
    void shouldWorkWithSettingsInPlugins() throws IOException {
        var cfgFile = createConfigFileInDefaultLocation(MyPlugin.oldSetting + "=bar");
        var result = runConfigMigrationCommand(createPluginClassLoader());
        assertEquals(0, result.exitCode);
        assertThat(readFileIgnoreJvmRecommendations(cfgFile))
                .isEqualToIgnoringNewLines(MyPlugin.setting.name() + "=bar");
    }

    @Test
    void suggestedJvmAdditionalsShouldMatchTemplate() {
        // This is a test to notice when we change JVM arguments in the config template and decide if we want to add it
        // to migrated configs
        Path root = Path.of("").toAbsolutePath();
        while (!(root.getFileName().toString().equals("public")
                || root.getFileName().toString().equals("neo4j"))) {
            root = root.getParent();
        }
        // Not the most robust way to get hold of the template but I guess it works
        Path confTemplate = root.resolve(
                "packaging/standalone/standalone-community/src/main/distribution/text/community/conf/neo4j.conf");

        assertThat(confTemplate).exists();
        String jvmAdditionals =
                Config.newBuilder().fromFile(confTemplate).build().get(additional_jvm);

        Set<String> templateSettings =
                Arrays.stream(jvmAdditionals.split(lineSeparator())).collect(Collectors.toSet());
        List<String> ignoredFromTemplate = List.of(
                "-XX:+UseG1GC",
                "-XX:-OmitStackTraceInFastThrow",
                "-XX:+AlwaysPreTouch",
                "-XX:+UnlockExperimentalVMOptions",
                "-XX:+TrustFinalNonStaticFields",
                "-XX:+DisableExplicitGC",
                "-Djdk.nio.maxCachedBufferSize=1024",
                "-Dio.netty.tryReflectionSetAccessible=true",
                "-Djdk.tls.ephemeralDHKeySize=2048",
                "-Djdk.tls.rejectClientInitiatedRenegotiation=true",
                "-XX:FlightRecorderOptions=stackdepth=256",
                "-XX:+UnlockDiagnosticVMOptions",
                "-XX:+DebugNonSafepoints");
        templateSettings.removeAll(ignoredFromTemplate);
        Collection<String> jvmArgs = ConfigFileMigrator.recommendedJvmAdditionals().stream()
                .map(ConfigFileMigrator.JvmArg::arg)
                .collect(Collectors.toList());
        assertThat(templateSettings).containsExactlyInAnyOrderElementsOf(jvmArgs);
    }

    @Test
    void shouldSuggestMissingJvmAdditionalsWhenNoExists() throws IOException {
        var cfgFileWithoutAnyJvmAdditional =
                createConfigFileInDefaultLocation(GraphDatabaseSettings.filewatcher_enabled.name() + "=false");
        var result = runConfigMigrationCommand(createPluginClassLoader());
        assertEquals(0, result.exitCode);

        assertThat(Files.readString(cfgFileWithoutAnyJvmAdditional))
                .isEqualToIgnoringNewLines(
                        GraphDatabaseSettings.filewatcher_enabled.name() + "=false" + jvmRecommendations());

        assertThat(result.out)
                .contains(additional_jvm.name() + "=--add-opens=java.base/java.nio=ALL-UNNAMED RECOMMENDED")
                .contains(additional_jvm.name() + "=--add-opens=java.base/java.io=ALL-UNNAMED RECOMMENDED")
                .contains(additional_jvm.name() + "=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED RECOMMENDED")
                .contains(additional_jvm.name() + "=-Dlog4j2.disable.jmx=true RECOMMENDED");
    }

    @Test
    void shouldSuggestMissingJvmAdditionalsWhenSomeExists() throws IOException {
        String jvmSetting = additional_jvm.name();
        String gcJvm = String.format("%s=%s%n", jvmSetting, "-XX:+UseG1GC");
        String log4jJvm = String.format("%s=%s%n", jvmSetting, "-Dlog4j2.disable.jmx=false");
        String log4jJvmWithTrailingSpaceAndMismatchingValue =
                String.format("%s=%s%n", jvmSetting, "-Dlog4j2.disable.jmx=false ");
        var cfgFileWithSomeJvmAdditional =
                createConfigFileInDefaultLocation(gcJvm + log4jJvmWithTrailingSpaceAndMismatchingValue);
        var result = runConfigMigrationCommand(createPluginClassLoader());
        assertEquals(0, result.exitCode);

        String filteredRec = jvmRecommendations("-Dlog4j2.disable.jmx");
        assertThat(Files.readString(cfgFileWithSomeJvmAdditional))
                .isEqualToIgnoringNewLines(gcJvm + log4jJvm + filteredRec);

        assertThat(result.out)
                .contains(jvmSetting + "=-XX:+UseG1GC UNCHANGED")
                .contains(jvmSetting + "=-Dlog4j2.disable.jmx=false UNCHANGED")
                .contains(jvmSetting + "=--add-opens=java.base/java.nio=ALL-UNNAMED RECOMMENDED")
                .contains(jvmSetting + "=--add-opens=java.base/java.io=ALL-UNNAMED RECOMMENDED")
                .contains(jvmSetting + "=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED RECOMMENDED")
                .doesNotContain("-Dlog4j2.disable.jmx=true");
    }

    private String readFileIgnoreJvmRecommendations(Path configFile) throws IOException {
        String cfg = Files.readString(configFile);
        return cfg.replace(jvmRecommendations(), "");
    }

    private String jvmRecommendations(String... without) {
        Collection<String> jvmRecommendations = ConfigFileMigrator.recommendedJvmAdditionals().stream()
                .filter(jvmArg ->
                        Arrays.stream(without).noneMatch(s -> jvmArg.arg().contains(s)))
                .map(s -> additional_jvm.name() + "=" + s.arg())
                .toList();
        return String.join(lineSeparator(), jvmRecommendations) + lineSeparator();
    }

    private ClassLoader createPluginClassLoader() throws IOException {
        var cls = MyPlugin.class;
        Path jarFile = neo4jLayout.homeDirectory().resolve("plugins").resolve("MyPlugin.jar");
        Files.createDirectories(jarFile.getParent());
        try (JarOutputStream jarOut = new JarOutputStream(Files.newOutputStream(jarFile))) {
            String fileName = cls.getName().replace('.', '/') + ".class";
            jarOut.putNextEntry(new ZipEntry(fileName));
            jarOut.write(JarBuilder.classCompiledBytes(fileName));
            jarOut.closeEntry();
            jarOut.putNextEntry(new ZipEntry("META-INF/services/" + SettingsDeclaration.class.getName()));
            jarOut.write((cls.getName() + "\n").getBytes(StandardCharsets.UTF_8));
            jarOut.putNextEntry(new ZipEntry("META-INF/services/" + SettingMigrator.class.getName()));
            jarOut.write((cls.getName() + "\n").getBytes(StandardCharsets.UTF_8));
            jarOut.closeEntry();
        }
        return new URLClassLoader(new URL[] {jarFile.toUri().toURL()}, Bootloader.class.getClassLoader());
    }

    public static class MyPlugin implements SettingsDeclaration, SettingMigrator {
        public static final Setting<String> setting =
                newBuilder("db.my.plugin.setting", STRING, "foo").build();
        public static final String oldSetting = "db.old.my.plugin.setting";

        @Override
        public void migrate(Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            SettingMigrators.migrateSettingNameChange(values, log, oldSetting, setting);
        }
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
        return runConfigMigrationCommand(this.getClass().getClassLoader(), args);
    }

    private Result runConfigMigrationCommand(ClassLoader classLoader, String... args) {
        var homeDir = neo4jLayout.homeDirectory().toAbsolutePath();
        var configDir = homeDir.resolve(Config.DEFAULT_CONFIG_DIR_NAME);
        var out = new Output();
        var err = new Output();

        var ctx = new EnhancedExecutionContext(
                homeDir,
                configDir,
                out.printStream,
                err.printStream,
                new DefaultFileSystemAbstraction(),
                () -> null,
                classLoader);

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
