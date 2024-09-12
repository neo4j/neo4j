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

import static org.neo4j.io.layout.DatabaseFile.ID_FILE_SUFFIX;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
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
    private static final String BACKUP_TOOLS_FOLDER = "tools";
    private static final String QUARANTINE_MARKER_FILENAME = "quarantine_marker";

    private final Path databaseDirectory;
    private final Neo4jLayout neo4jLayout;
    private final String databaseName;

    protected PlainDatabaseLayout(Neo4jLayout neo4jLayout, String databaseName) {
        var normalizedName = new NormalizedDatabaseName(databaseName).name();
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
    public Path backupToolsFolder() {
        return databaseDirectory().resolve(BACKUP_TOOLS_FOLDER);
    }

    @Override
    public Path metadataStore() {
        throw new IllegalStateException("Can not get the metadata store for a PlainDatabaseLayout.");
    }

    @Override
    public Path indexStatisticsStore() {
        throw new IllegalStateException("Can not get the metadata store for a PlainDatabaseLayout.");
    }

    @Override
    public Path pathForExistsMarker() {
        throw new IllegalStateException("Can not get the exists marker path for a PlainDatabaseLayout.");
    }

    @Override
    public Path pathForStore(CommonDatabaseStores store) {
        throw new IllegalStateException(
                "Can not get the path for the %s store from a PlainDatabaseLayout.".formatted(store.name()));
    }

    @Override
    public Set<Path> idFiles() {
        return databaseFiles()
                .filter(DatabaseFile::hasIdFile)
                .flatMap(value -> idFile(value).stream())
                .collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public Set<Path> storeFiles() {
        return databaseFiles().map(this::file).collect(Collectors.toUnmodifiableSet());
    }

    /**
     * @return the store files required to be present for a database to be able to be recovered
     */
    @Override
    public Set<Path> mandatoryStoreFiles() {
        return databaseFiles()
                .filter(Predicate.not(this::isRecoverableStore))
                .map(this::file)
                .collect(Collectors.toUnmodifiableSet());
    }

    protected Stream<? extends DatabaseFile> databaseFiles() {
        throw new IllegalStateException("Can not access the database files from a PlainDatabaseLayout.");
    }

    @Override
    public Optional<Path> idFile(DatabaseFile file) {
        return file.hasIdFile() ? Optional.of(idFile(file.getName())) : Optional.empty();
    }

    @Override
    public Path file(String fileName) {
        return databaseDirectory.resolve(fileName);
    }

    @Override
    public Path file(DatabaseFile databaseFile) {
        return file(databaseFile.getName());
    }

    @Override
    public Stream<Path> allFiles(DatabaseFile databaseFile) {
        return Stream.concat(idFile(databaseFile).stream(), Stream.of(file(databaseFile)));
    }

    @Override
    public Path[] listDatabaseFiles(FileSystemAbstraction fs, Predicate<? super Path> filter) {
        try {
            return fs.listFiles(databaseDirectory, filter::test);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected Path idFile(String name) {
        return file(idFileName(name));
    }

    protected boolean isRecoverableStore(DatabaseFile file) {
        throw new IllegalStateException(
                "Can not determine whether the store '%s' is recoverable in a PlainDatabaseLayout"
                        .formatted(file.getName()));
    }

    private static String idFileName(String storeName) {
        return storeName + ID_FILE_SUFFIX;
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
