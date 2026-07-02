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
package org.neo4j.io.layout;

import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.pagecache.impl.muninn.StoreFile;
import org.neo4j.kernel.database.NormalizedDatabaseName;

/**
 * File layout representation of the particular database. Facade for any kind of file lookup for a particular database storage implementation.
 * Any file retrieved from a layout can be considered a canonical file.
 * <br/>
 * No assumption should be made about where and how files of a particular database are positioned and all those details should be encapsulated inside.
 *
 * @see Neo4jLayout
 * @see DatabaseFile
 */
public class PlainDatabaseLayout implements DatabaseLayout {
    private static final String DATABASE_LOCK_FILENAME = "database_lock";
    private static final String VECTOR_SUB_DIRECTORY = "vector";
    private static final String BACKUP_TOOLS_DIRECTORY = "tools";
    private static final String QUARANTINE_MARKER_FILENAME = "quarantine_marker";

    private final Path databaseDirectory;
    private final Neo4jLayout neo4jLayout;
    private final String databaseName;

    protected PlainDatabaseLayout(Neo4jLayout neo4jLayout, String databaseName) {
        var normalizedName = NormalizedDatabaseName.normalize(databaseName);
        this.neo4jLayout = neo4jLayout;
        this.databaseDirectory =
                FileUtils.getCanonicalFile(neo4jLayout.databasesDirectory().resolve(normalizedName));
        this.databaseName = normalizedName;
    }

    @Override
    public Path getTransactionLogsDirectory() {
        return neo4jLayout.transactionLogsRootDirectory().resolve(getDatabaseName());
    }

    @Override
    public Path getScriptDirectory() {
        return neo4jLayout.scriptRootDirectory().resolve(getDatabaseName());
    }

    @Override
    public Path databaseLockFile() {
        return databaseDirectory().resolve(DATABASE_LOCK_FILENAME);
    }

    @Override
    public Path quarantineFile() {
        return databaseDirectory().resolve(QUARANTINE_MARKER_FILENAME);
    }

    @Override
    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public Neo4jLayout getNeo4jLayout() {
        return neo4jLayout;
    }

    @Override
    public Path databaseDirectory() {
        return databaseDirectory;
    }

    @Override
    public Path backupToolsDirectory() {
        return databaseDirectory().resolve(BACKUP_TOOLS_DIRECTORY);
    }

    @Override
    public Path vectorStoresDirectory() {
        return databaseDirectory().resolve(VECTOR_SUB_DIRECTORY);
    }

    @Override
    public StoreFile metadataStore() {
        throw new IllegalStateException("Can not get the metadata store for a PlainDatabaseLayout.");
    }

    @Override
    public StoreFile indexStatisticsStore() {
        throw new IllegalStateException("Can not get the metadata store for a PlainDatabaseLayout.");
    }

    @Override
    public Path pathForExistsMarker() {
        throw new IllegalStateException("Can not get the exists marker path for a PlainDatabaseLayout.");
    }

    @Override
    public StoreFile pathForStore(CommonDatabaseStores store) {
        throw new IllegalStateException(
                "Can not get the path for the %s store from a PlainDatabaseLayout.".formatted(store.name()));
    }

    @Override
    public Optional<StoreFile> idFile(DatabaseFile file) {
        return file.hasIdFile() ? Optional.of(idFile(file.getName())) : Optional.empty();
    }

    @Override
    public StoreFile file(String name) {
        return new StoreFile(databaseDirectory.resolve(name));
    }

    @Override
    public Path path(String name) {
        return databaseDirectory.resolve(name);
    }

    @Override
    public StoreFile file(DatabaseFile databaseFile) {
        return file(databaseFile.getName());
    }

    @Override
    public Stream<StoreFile> allFiles(DatabaseFile databaseFile) {
        return Stream.concat(idFile(databaseFile).stream(), Stream.of(file(databaseFile)));
    }

    protected boolean isRecoverableStore(DatabaseFile file) {
        throw new IllegalStateException(
                "Can not determine whether the store '%s' is recoverable in a PlainDatabaseLayout"
                        .formatted(file.getName()));
    }

    private StoreFile idFile(String name) {
        return file(idFileName(name));
    }

    protected String idFileName(String name) {
        return name + DatabaseFile.ID_FILE_SUFFIX;
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseDirectory, neo4jLayout);
    }

    @Override
    public String toString() {
        return "PlainDatabaseLayout{" + "databaseDirectory=" + databaseDirectory + ", transactionLogsDirectory="
                + getTransactionLogsDirectory() + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PlainDatabaseLayout that = (PlainDatabaseLayout) o;
        return Objects.equals(databaseDirectory, that.databaseDirectory)
                && Objects.equals(neo4jLayout, that.neo4jLayout)
                && getTransactionLogsDirectory().equals(that.getTransactionLogsDirectory());
    }
}
