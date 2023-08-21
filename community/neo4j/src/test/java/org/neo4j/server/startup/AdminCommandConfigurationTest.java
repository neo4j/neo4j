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
package org.neo4j.server.startup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.neo4j.configuration.BootloaderSettings.additional_jvm;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.neo4j.cli.AbstractAdminCommand;
import org.neo4j.configuration.Config;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;

@Neo4jLayoutExtension
class AdminCommandConfigurationTest {

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final ByteArrayOutputStream err = new ByteArrayOutputStream();
    private final ProcessManager processManager = mock(ProcessManager.class);

    @Inject
    private Neo4jLayout layout;

    @Test
    void adminConfigShouldOverrideNeo4jConfig() throws Exception {
        creteConfigFile(neo4jConfig(), additional_jvm.name() + "=-Xmx100MB");
        creteConfigFile(adminConfig(), additional_jvm.name() + "=-Xmx200MB");

        assertThat(execute("database", "migrate", "*")).isEqualTo(0);

        ArgumentCaptor<List<String>> argsCaptor = ArgumentCaptor.forClass(List.class);
        verify(processManager).run(argsCaptor.capture(), any());
        assertThat(argsCaptor.getValue()).contains("-Xmx200MB").doesNotContain("-Xmx100MB");
    }

    @Test
    void commandConfigShouldOverrideAdminConfig() throws Exception {
        creteConfigFile(adminConfig(), additional_jvm.name() + "=-Xmx100MB");
        creteConfigFile(commandConfig(), additional_jvm.name() + "=-Xmx200MB");

        assertThat(execute("database", "migrate", "*")).isEqualTo(0);

        ArgumentCaptor<List<String>> argsCaptor = ArgumentCaptor.forClass(List.class);
        verify(processManager).run(argsCaptor.capture(), any());
        assertThat(argsCaptor.getValue()).contains("-Xmx200MB").doesNotContain("-Xmx100MB");
    }

    @Test
    void additionalConfigShouldOverrideCommandConfig() throws Exception {
        Path additionalConfig = additionalConfig();
        creteConfigFile(commandConfig(), additional_jvm.name() + "=-Xmx100MB");
        creteConfigFile(additionalConfig, additional_jvm.name() + "=-Xmx200MB");

        assertThat(execute("database", "migrate", "*", "--additional-config", additionalConfig.toString()))
                .isEqualTo(0);

        ArgumentCaptor<List<String>> argsCaptor = ArgumentCaptor.forClass(List.class);
        verify(processManager).run(argsCaptor.capture(), any());
        assertThat(argsCaptor.getValue()).contains("-Xmx200MB").doesNotContain("-Xmx100MB");
    }

    @Test
    void shouldFailWhenSuppliedAdditionalConfigDoesNotExist() {
        assertThat(execute(
                        "database",
                        "migrate",
                        "*",
                        "--additional-config",
                        additionalConfig().toString()))
                .isEqualTo(1);
        assertThat(err.toString()).contains("additional-config.conf does not exist");
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
        return configDir().resolve("neo4j-admin-database-migrate.conf");
    }

    private Path additionalConfig() {
        return layout.homeDirectory().resolve("additional-config.conf").toAbsolutePath();
    }

    private Path configDir() {
        return layout.homeDirectory().toAbsolutePath().resolve(Config.DEFAULT_CONFIG_DIR_NAME);
    }

    private int execute(String... args) {
        HashMap<String, String> environmentVariables = new HashMap<>();
        environmentVariables.putIfAbsent(
                Bootloader.ENV_NEO4J_HOME, layout.homeDirectory().toString());

        var environment = new Environment(
                new PrintStream(out),
                new PrintStream(err),
                environmentVariables::get,
                something -> null,
                Runtime.version());
        var command = new Neo4jAdminCommand(environment) {
            @Override
            protected Bootloader.Admin createAdminBootloader(String[] args) {
                var bootloader = spy(super.createAdminBootloader(args));
                doAnswer(inv -> processManager).when(bootloader).processManager();
                return bootloader;
            }
        };
        return Neo4jAdminCommand.asCommandLine(command, environment).execute(args);
    }
}
