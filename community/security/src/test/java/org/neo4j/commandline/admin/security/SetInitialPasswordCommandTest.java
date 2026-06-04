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
package org.neo4j.commandline.admin.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.commandline.admin.security.exception.InvalidPasswordException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.server.security.auth.CommunitySecurityModule;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.string.UTF8;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import picocli.CommandLine;

@EphemeralTestDirectoryExtension
class SetInitialPasswordCommandTest {
    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private TestDirectory testDir;

    private SetInitialPasswordCommand command;
    private Path authInitFile;

    @BeforeEach
    void setup() {
        command = new SetInitialPasswordCommand(new ExecutionContext(
                testDir.directory("home"),
                testDir.directory("conf"),
                mock(PrintStream.class),
                mock(PrintStream.class),
                fileSystem));

        authInitFile = CommunitySecurityModule.getInitialUserRepositoryFile(command.loadNeo4jConfig());
    }

    @Test
    void printUsageHelp() {
        final var baos = new ByteArrayOutputStream();
        try (var out = new PrintStream(baos)) {
            CommandLine.usage(command, new PrintStream(out), CommandLine.Help.Ansi.OFF);
        }
        assertThat(baos.toString().trim()).isEqualToIgnoringNewLines("""
                                USAGE

                                set-initial-password [-h] [--expand-commands] [--verbose]
                                                     [--require-password-change[=true|false]]
                                                     [--additional-config=<file>] <password>

                                DESCRIPTION

                                Sets the initial password of the initial admin user ('neo4j'). And removes the
                                requirement to change password on first login. IMPORTANT: this change will only
                                take effect if performed before the database is started for the first time.

                                PARAMETERS

                                      <password>

                                OPTIONS

                                      --additional-config=<file>
                                                          Configuration file with additional configuration.
                                      --expand-commands   Allow command expansion in config value evaluation.
                                  -h, --help              Show this help message and exit.
                                      --require-password-change[=true|false]
                                                          Require the user to change their password on first
                                                            login.
                                                            Default: false
                                      --verbose           Enable verbose output.""");
    }

    @Test
    void shouldSetInitialPassword() throws Throwable {
        // Given
        assertThat(fileSystem.fileExists(authInitFile)).isFalse();

        // When
        CommandLine.populateCommand(command, "12345678");
        command.execute();

        // Then
        assertAuthIniFile("12345678");
    }

    @Test
    void shouldFailToSetShortInitialPassword() {
        // Given
        assertThat(fileSystem.fileExists(authInitFile)).isFalse();

        // When
        CommandLine.populateCommand(command, "123");

        // Then
        assertThatExceptionOfType(InvalidPasswordException.class)
                .isThrownBy(() -> command.execute())
                .satisfies(e -> assertThat(e.getStackTrace()).isEmpty());
    }

    @Test
    void shouldFailToSetShortInitialPasswordOneCharUseTwoBytes() {
        // Given
        assertThat(fileSystem.fileExists(authInitFile)).isFalse();

        // When
        CommandLine.populateCommand(command, "neo4j*£");

        // Then
        assertThatExceptionOfType(InvalidPasswordException.class)
                .isThrownBy(() -> command.execute())
                .satisfies(e -> assertThat(e.getStackTrace()).isEmpty());
    }

    @Test
    void shouldFailToSetShortInitialPasswordCharactersUsingFourBytesEach() {
        // Given
        assertThat(fileSystem.fileExists(authInitFile)).isFalse();

        // When
        CommandLine.populateCommand(command, "𓃠𓃠𓃠𓃠");

        // Then
        assertThatExceptionOfType(InvalidPasswordException.class)
                .isThrownBy(() -> command.execute())
                .satisfies(e -> assertThat(e.getStackTrace()).isEmpty());
    }

    @Test
    void shouldOverwriteInitialPasswordFileIfExists() throws Throwable {
        // Given
        fileSystem.mkdirs(authInitFile.getParent());
        fileSystem.write(authInitFile);

        // When
        CommandLine.populateCommand(command, "12345678");
        command.execute();

        // Then
        assertAuthIniFile("12345678");
    }

    @Test
    void shouldNotWorkWithSamePassword() {
        CommandLine.populateCommand(command, "neo4j");

        // Then
        assertThatExceptionOfType(InvalidPasswordException.class).isThrownBy(() -> command.execute());
    }

    private void assertAuthIniFile(String password) throws Throwable {
        assertThat(fileSystem.fileExists(authInitFile)).isTrue();
        FileUserRepository userRepository = new FileUserRepository(
                fileSystem, authInitFile, NullLogProvider.getInstance(), EmptyMemoryTracker.INSTANCE);
        userRepository.start();
        User neo4j = userRepository.getUserByName(AuthManager.INITIAL_USER_NAME);
        assertThat(neo4j).isNotNull();
        assertThat(neo4j.credential().value().matchesPassword(UTF8.encode(password)))
                .isTrue();
        assertThat(neo4j.passwordChangeRequired()).isFalse();
    }
}
