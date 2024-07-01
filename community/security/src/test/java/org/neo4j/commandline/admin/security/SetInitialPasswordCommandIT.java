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

import static org.apache.commons.lang3.SystemUtils.IS_OS_WINDOWS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.string.UTF8;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import picocli.CommandLine;

@TestDirectoryExtension
@ExtendWith(DefaultFileSystemExtension.class)
class SetInitialPasswordCommandIT {
    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private TestDirectory testDirectory;

    private Path confDir;
    private Path homeDir;
    private PrintStream out;
    private PrintStream err;
    private Path authStoreDirectory;
    private Config config;
    private Path configPath;
    private static final String password = "password";

    private static final String successMessage = "Changed password for user 'neo4j'. "
            + "IMPORTANT: this change will only take effect if performed before the database is started for the first time.";

    @BeforeEach
    void setup() {
        Path graphDir = testDirectory.homePath().resolve(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        confDir = graphDir.resolve("conf");
        homeDir = graphDir.resolve("home");
        out = mock(PrintStream.class);
        err = mock(PrintStream.class);
        authStoreDirectory = homeDir.resolve("data").resolve("dbms");

        config = Config.newBuilder()
                .set(GraphDatabaseInternalSettings.auth_store_directory, authStoreDirectory)
                .build();
        configPath = confDir.resolve(Config.DEFAULT_CONFIG_FILE_NAME);
        try {
            createFile(configPath, config.toString(false));

            createFile(getAuthFile("auth.ini"), "");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        fileSystem.close();
    }

    @Test
    void shouldSetPassword() throws Throwable {
        executeCommand(password);
        assertAuthIniFile(password, false);

        verify(out).println(successMessage);
    }

    @Test
    void shouldSetPasswordWithRequirePasswordChange() throws Throwable {
        executeCommand(password, "--require-password-change");
        assertAuthIniFile(password, true);

        verify(out).println(successMessage);
    }

    @Test
    void shouldSetPasswordWithRequirePasswordChangeOtherOrder() throws Throwable {
        executeCommand("--require-password-change", password);
        assertAuthIniFile(password, true);

        verify(out).println(successMessage);
    }

    @Test
    void shouldOverwriteIfSetPasswordAgain() throws Throwable {
        executeCommand(password);
        assertAuthIniFile(password, false);
        executeCommand("muchBetter");
        assertAuthIniFile("muchBetter", false);

        verify(out, times(2)).println(successMessage);
    }

    @Test
    void shouldWorkWithSamePassword() throws Throwable {
        executeCommand(password);
        assertAuthIniFile(password, false);
        executeCommand(password);
        assertAuthIniFile(password, false);

        verify(out, times(2)).println(successMessage);
    }

    @Test
    void shouldNotErrorIfOnlyTheUnmodifiedDefaultNeo4jUserAlreadyExists() throws Throwable {
        // Given
        // Create a config file with auth_minimum_password_length set to 1
        config.set(GraphDatabaseSettings.auth_minimum_password_length, 1);
        createFile(configPath, config.toString(false));

        // Create an `auth` file with the default neo4j user
        executeCommand(AuthManager.INITIAL_PASSWORD);
        Path authFile = getAuthFile("auth");
        Path authIniFile = getAuthFile("auth.ini");
        fileSystem.copyFile(authIniFile, authFile);

        // When
        executeCommand("should-not-be-ignored");

        // Then
        assertAuthIniFile("should-not-be-ignored", false);
        verify(out, times(2)).println(successMessage);
    }

    private void assertAuthIniFile(String password, boolean passwordChangeRequired) throws Throwable {
        Path authIniFile = getAuthFile("auth.ini");
        assertTrue(fileSystem.fileExists(authIniFile));
        FileUserRepository userRepository = new FileUserRepository(
                fileSystem, authIniFile, NullLogProvider.getInstance(), EmptyMemoryTracker.INSTANCE);
        userRepository.start();
        User neo4j = userRepository.getUserByName(AuthManager.INITIAL_USER_NAME);
        assertNotNull(neo4j);
        assertTrue(neo4j.credential().matchesPassword(UTF8.encode(password)));
        assertThat(neo4j.passwordChangeRequired()).isEqualTo(passwordChangeRequired);
    }

    private Path getAuthFile(String name) {
        return authStoreDirectory.resolve(name);
    }

    private void executeCommand(String... args) throws IOException {
        final var ctx = new ExecutionContext(homeDir, confDir, out, err, fileSystem);
        final var command = new SetInitialPasswordCommand(ctx);
        CommandLine.populateCommand(command, args);
        command.execute();
    }

    private void createFile(Path path, String content) throws IOException {
        String updatedContent = IS_OS_WINDOWS ? content.replace("\\", "/") : content;
        Path tmpFilePath = fileSystem.createTempFile(testDirectory.homePath(), "", "");
        fileSystem.write(tmpFilePath).write(ByteBuffer.wrap(updatedContent.getBytes()));
        fileSystem.copyFile(tmpFilePath, path);
        fileSystem.delete(tmpFilePath);
    }
}
