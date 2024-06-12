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

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.server.security.FormatException;

/**
 * Stores user auth data. In memory, but backed by persistent storage so changes to this repository will survive
 * JVM restarts and crashes.
 */
public class FileUserRepository extends AbstractUserRepository implements FileRepository {
    private final Path authFile;
    private final FileSystemAbstraction fileSystem;

    // TODO: We could improve concurrency by using a ReadWriteLock

    private final InternalLog log;
    private final MemoryTracker memoryTracker;

    private final UserSerialization serialization = new UserSerialization();

    public FileUserRepository(
            FileSystemAbstraction fileSystem, Path path, InternalLogProvider logProvider, MemoryTracker memoryTracker) {
        this.fileSystem = fileSystem;
        this.authFile = path;
        this.memoryTracker = memoryTracker;
        this.log = logProvider.getLog(getClass());
    }

    @Override
    public void start() throws Exception {
        clear();

        FileRepository.assertNotMigrated(authFile, fileSystem, log);

        ListSnapshot<User> onDiskUsers = readPersistedUsers();
        if (onDiskUsers != null) {
            setUsers(onDiskUsers);
        }
    }

    @Override
    protected ListSnapshot<User> readPersistedUsers() throws IOException {
        if (fileSystem.fileExists(authFile)) {
            long readTime;
            List<User> readUsers;
            try {
                log.debug("Reading users from %s", authFile);
                readTime = fileSystem.lastModifiedTime(authFile);
                readUsers = serialization.loadRecordsFromFile(fileSystem, authFile, memoryTracker);
            } catch (FormatException e) {
                log.error("Failed to read authentication file \"%s\" (%s)", authFile.toAbsolutePath(), e.getMessage());
                throw new IllegalStateException("Failed to read authentication file: " + authFile);
            }

            return new ListSnapshot<>(readTime, readUsers);
        }
        log.debug("Did not find any file named %s in %s", authFile.getFileName(), authFile.getParent());
        return null;
    }

    @Override
    protected void persistUsers() throws IOException {
        log.debug("Persisting %s users into %s", users.size(), authFile);
        serialization.saveRecordsToFile(fileSystem, authFile, users);
    }

    @Override
    public ListSnapshot<User> getSnapshot() throws IOException {
        if (lastLoaded.get() < fileSystem.lastModifiedTime(authFile)) {
            return readPersistedUsers();
        }
        synchronized (this) {
            return new ListSnapshot<>(lastLoaded.get(), new ArrayList<>(users));
        }
    }
}
