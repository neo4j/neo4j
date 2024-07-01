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
package org.neo4j.server.security.auth;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.logging.AssertableLogProvider.Level.ERROR;
import static org.neo4j.logging.LogAssertions.assertThat;

import java.io.IOException;
import java.nio.file.CopyOption;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.DelegatingFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.string.UTF8;
import org.neo4j.test.DoubleLatch;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@EphemeralTestDirectoryExtension
class FileUserRepositoryTest {
    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private TestDirectory testDirectory;

    private final MemoryTracker memoryTracker = EmptyMemoryTracker.INSTANCE;
    private final InternalLogProvider logProvider = NullLogProvider.getInstance();
    private Path authFile;

    @BeforeEach
    void setUp() {
        authFile = testDirectory.directory("dbms").resolve("auth");
    }

    @Test
    void shouldStoreAndRetrieveUsersByName() throws Exception {
        // Given
        FileUserRepository users = new FileUserRepository(fs, authFile, logProvider, memoryTracker);
        User user = new User("jake", null, LegacyCredential.INACCESSIBLE, true, false);
        users.create(user);

        // When
        User result = users.getUserByName(user.name());

        // Then
        assertThat(result).isEqualTo(user);
    }

    @Test
    void shouldPersistUserWithoutId() throws Throwable {
        // Given
        FileUserRepository users = new FileUserRepository(fs, authFile, logProvider, memoryTracker);
        User user = new User("jake", null, LegacyCredential.INACCESSIBLE, true, false);
        users.create(user);

        users = new FileUserRepository(fs, authFile, logProvider, memoryTracker);
        users.start();

        // When
        User resultByName = users.getUserByName(user.name());

        // Then
        assertThat(resultByName).isEqualTo(user);
    }

    @Test
    void shouldNotPersistIdForUserWithValidId() throws Throwable {
        // Given
        FileUserRepository users = new FileUserRepository(fs, authFile, logProvider, memoryTracker);
        User user = new User("jake", "id", LegacyCredential.INACCESSIBLE, true, false);
        users.create(user);

        users = new FileUserRepository(fs, authFile, logProvider, memoryTracker);
        users.start();

        // When
        User resultByName = users.getUserByName(user.name());

        // Then
        User userWithoutId = new User("jake", null, LegacyCredential.INACCESSIBLE, true, false);
        assertThat(resultByName).isEqualTo(userWithoutId);
    }

    @Test
    void shouldNotAllowComplexNames() throws Exception {
        // Given
        FileUserRepository users = new FileUserRepository(fs, authFile, logProvider, memoryTracker);

        // When
        users.assertValidUsername("neo4j");
        users.assertValidUsername("johnosbourne");
        users.assertValidUsername("john_osbourne");

        assertThatThrownBy(() -> users.assertValidUsername(null))
                .isInstanceOf(InvalidArgumentsException.class)
                .hasMessage("The provided username is empty.");
        assertThatThrownBy(() -> users.assertValidUsername(""))
                .isInstanceOf(InvalidArgumentsException.class)
                .hasMessage("The provided username is empty.");
        assertThatThrownBy(() -> users.assertValidUsername(","))
                .isInstanceOf(InvalidArgumentsException.class)
                .hasMessage(
                        "Username ',' contains illegal characters. Use ascii characters that are not ',', ':' or whitespaces.");
        assertThatThrownBy(() -> users.assertValidUsername("with space"))
                .isInstanceOf(InvalidArgumentsException.class)
                .hasMessage(
                        "Username 'with space' contains illegal characters. Use ascii characters that are not ',', ':' or whitespaces.");
        assertThatThrownBy(() -> users.assertValidUsername("with:colon"))
                .isInstanceOf(InvalidArgumentsException.class)
                .hasMessage(
                        "Username 'with:colon' contains illegal characters. Use ascii characters that are not ',', ':' or whitespaces.");
        assertThatThrownBy(() -> users.assertValidUsername("withå"))
                .isInstanceOf(InvalidArgumentsException.class)
                .hasMessage(
                        "Username 'withå' contains illegal characters. Use ascii characters that are not ',', ':' or whitespaces.");
    }

    @Test
    void shouldRecoverIfCrashedDuringMove() throws Throwable {
        // Given
        final IOException exception = new IOException("simulated IO Exception on create");
        FileSystemAbstraction crashingFileSystem = new DelegatingFileSystemAbstraction(fs) {
            @Override
            public void renameFile(Path oldLocation, Path newLocation, CopyOption... copyOptions) throws IOException {
                if (authFile.getFileName().equals(newLocation.getFileName())) {
                    throw exception;
                }
                super.renameFile(oldLocation, newLocation, copyOptions);
            }
        };

        FileUserRepository users = new FileUserRepository(crashingFileSystem, authFile, logProvider, memoryTracker);
        users.start();
        User user = new User("jake", null, LegacyCredential.INACCESSIBLE, true, false);

        // When
        var e = assertThrows(IOException.class, () -> users.create(user));
        assertSame(exception, e);

        // Then
        assertFalse(crashingFileSystem.fileExists(authFile));
        assertThat(crashingFileSystem.listFiles(authFile.getParent()).length).isEqualTo(0);
    }

    @Test
    void shouldFailOnReadingInvalidEntries() throws Throwable {
        // Given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        fs.mkdir(authFile.getParent());
        // First line is correctly formatted, second line has an extra field
        FileRepositorySerializer.writeToFile(
                fs,
                authFile,
                UTF8.encode(
                        "admin:SHA-256,A42E541F276CF17036DB7818F8B09B1C229AAD52A17F69F4029617F3A554640F,FB7E8AE08A6A7C741F678AD22217808F:\n"
                                + "neo4j:fc4c600b43ffe4d5857b4439c35df88f:SHA-256,"
                                + "A42E541F276CF17036DB7818F8B09B1C229AAD52A17F69F4029617F3A554640F,FB7E8AE08A6A7C741F678AD22217808F:\n"));

        // When
        FileUserRepository users = new FileUserRepository(fs, authFile, logProvider, memoryTracker);

        var e = assertThrows(IllegalStateException.class, users::start);
        assertThat(e.getMessage()).startsWith("Failed to read authentication file: ");

        assertThat(users.numberOfUsers()).isEqualTo(0);
        assertThat(logProvider)
                .forClass(FileUserRepository.class)
                .forLevel(ERROR)
                .containsMessageWithArguments(
                        "Failed to read authentication file \"%s\" (%s)",
                        authFile.toAbsolutePath(), "wrong number of line fields, expected 3, got 4 [line 2]");
    }

    @Test
    void shouldProvideUserByUsernameEvenIfMidSetUsers() throws Throwable {
        // Given
        FileUserRepository users = new FileUserRepository(fs, authFile, logProvider, memoryTracker);
        users.create(new User("oskar", null, LegacyCredential.forPassword("hidden"), false, false));
        DoubleLatch latch = new DoubleLatch(2);

        // When
        var executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> setUsers = executor.submit(() -> {
                try {
                    users.setUsers(new HangingListSnapshot(latch, 10L, Collections.emptyList()));
                } catch (InvalidArgumentsException e) {
                    throw new RuntimeException(e);
                }
            });

            latch.startAndWaitForAllToStart();

            // Then
            assertNotNull(users.getUserByName("oskar"));

            latch.finish();
            setUsers.get();
        } finally {
            executor.shutdown();
        }
    }

    static class HangingListSnapshot extends ListSnapshot<User> {
        private final DoubleLatch latch;

        HangingListSnapshot(DoubleLatch latch, long timestamp, List<User> values) {
            super(timestamp, values);
            this.latch = latch;
        }

        @Override
        public long timestamp() {
            latch.start();
            latch.finishAndWaitForAllToFinish();
            return super.timestamp();
        }
    }
}
