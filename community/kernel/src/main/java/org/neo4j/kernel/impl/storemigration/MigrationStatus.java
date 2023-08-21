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
package org.neo4j.kernel.impl.storemigration;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileSystemUtils;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StoreVersionIdentifier;

public record MigrationStatus(
        MigrationState state, StoreVersionIdentifier versionToMigrateFrom, StoreVersionIdentifier versionToMigrateTo) {

    public MigrationStatus() {
        this(null, null, null);
    }

    public enum MigrationState {
        migrating,
        moving;

        public boolean isNeededFor(MigrationState current) {
            return current == null || this.ordinal() >= current.ordinal();
        }

        public void setMigrationStatus(
                FileSystemAbstraction fs,
                Path stateFile,
                StoreVersionIdentifier versionToMigrateFrom,
                StoreVersionIdentifier versionToMigrateTo,
                MemoryTracker memoryTracker)
                throws IOException {
            if (fs.fileExists(stateFile)) {
                try {
                    fs.truncate(stateFile, 0);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            String status = name()
                    + '\n'
                    + serializeStoreVersionIdentifier(versionToMigrateFrom)
                    + '\n'
                    + serializeStoreVersionIdentifier(versionToMigrateTo);
            FileSystemUtils.writeString(fs, stateFile, status, memoryTracker);
        }
    }

    public boolean expectedMigration(StoreVersionIdentifier versionToMigrateTo) {
        return versionToMigrateTo != null && versionToMigrateTo.equals(this.versionToMigrateTo);
    }

    public boolean migrationInProgress() {
        return state != null;
    }

    public static MigrationStatus readMigrationStatus(
            FileSystemAbstraction fs, Path stateFile, MemoryTracker memoryTracker) throws IOException {
        return readFromFile(fs, stateFile, memoryTracker);
    }

    private static MigrationStatus readFromFile(FileSystemAbstraction fs, Path path, MemoryTracker memoryTracker)
            throws IOException {
        List<String> lines = FileSystemUtils.readLines(fs, path, memoryTracker);
        if (lines == null || lines.isEmpty()) {
            return new MigrationStatus();
        }
        String state = lines.get(0).trim();
        StoreVersionIdentifier versionToMigrateFrom =
                parseStoreVersionIdentifier(lines.get(1).trim());
        StoreVersionIdentifier versionToMigrateTo =
                parseStoreVersionIdentifier(lines.get(2).trim());
        return new MigrationStatus(MigrationState.valueOf(state), versionToMigrateFrom, versionToMigrateTo);
    }

    private static String serializeStoreVersionIdentifier(StoreVersionIdentifier storeVersionIdentifier) {
        return String.format(
                "%s|%s|%s|%s",
                storeVersionIdentifier.getStorageEngineName(),
                storeVersionIdentifier.getFormatName(),
                storeVersionIdentifier.getMajorVersion(),
                storeVersionIdentifier.getMinorVersion());
    }

    private static StoreVersionIdentifier parseStoreVersionIdentifier(String string) {
        String[] parts = string.split("\\|");
        if (parts.length != 4) {
            throw new IllegalStateException("Failed to parse store version identifier: " + string);
        }

        try {
            return new StoreVersionIdentifier(
                    parts[0], parts[1], Integer.parseInt(parts[2]), Integer.parseInt(parts[3]));
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Failed to parse store version identifier: " + string);
        }
    }
}
