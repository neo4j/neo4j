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
package org.neo4j.kernel.impl.storemigration;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.io.IOUtils.lineIterator;

import java.io.IOException;
import java.io.Writer;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import org.neo4j.io.fs.FileSystemAbstraction;

public record MigrationStatus(MigrationState state, String versionToMigrateFrom, String versionToMigrateTo) {
    public enum MigrationState {
        migrating,
        moving;

        public boolean isNeededFor(MigrationState current) {
            return current == null || this.ordinal() >= current.ordinal();
        }

        public void setMigrationStatus(
                FileSystemAbstraction fs, Path stateFile, String versionToMigrateFrom, String versionToMigrateTo) {
            if (fs.fileExists(stateFile)) {
                try {
                    fs.truncate(stateFile, 0);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            try (Writer writer = fs.openAsWriter(stateFile, UTF_8, false)) {
                writer.write(name());
                writer.write('\n');
                writer.write(versionToMigrateFrom);
                writer.write('\n');
                writer.write(versionToMigrateTo);
                writer.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public boolean expectedMigration(String versionToMigrateTo) {
        return versionToMigrateTo != null && versionToMigrateTo.equals(this.versionToMigrateTo);
    }

    public boolean migrationInProgress() {
        return state != null;
    }

    public static MigrationStatus readMigrationStatus(FileSystemAbstraction fs, Path stateFile) {
        return readFromFile(fs, stateFile);
    }

    private static MigrationStatus readFromFile(FileSystemAbstraction fs, Path path) {
        try (var reader = fs.openAsReader(path, UTF_8)) {
            var lineIterator = lineIterator(reader);
            String state = lineIterator.next().trim();
            String versionToMigrateFrom = lineIterator.next().trim();
            String versionToMigrateTo = lineIterator.next().trim();
            return new MigrationStatus(MigrationState.valueOf(state), versionToMigrateFrom, versionToMigrateTo);
        } catch (NoSuchFileException e) {
            return new MigrationStatus(null, null, null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
