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
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;

/**
 * File layout representation of the particular database. Facade for any kind of file lookup for a particular database storage implementation.
 * Any file retrieved from a layout can be considered a canonical file.
 * <br/>
 * No assumption should be made about where and how files of a particular database are positioned and all those details should be encapsulated inside.
 *
 * @see Neo4jLayout
 * @see DatabaseFile
 */
public interface DatabaseLayout {
    static DatabaseLayout ofFlat(Path databaseDirectory) {
        Path canonical = FileUtils.getCanonicalFile(databaseDirectory);
        Path home = canonical.getParent();
        String dbName = canonical.getFileName().toString();
        return Neo4jLayout.ofFlat(home).databaseLayout(dbName);
    }

    static DatabaseLayout of(Config config) {
        return Neo4jLayout.of(config).databaseLayout(config.get(GraphDatabaseSettings.initial_default_database));
    }

    static DatabaseLayout of(Neo4jLayout neo4jLayout, String databaseName) {
        return new PlainDatabaseLayout(neo4jLayout, databaseName);
    }

    Path getTransactionLogsDirectory();

    Path getScriptDirectory();

    Path databaseLockFile();

    Path quarantineFile();

    String getDatabaseName();

    Neo4jLayout getNeo4jLayout();

    Path databaseDirectory();

    Path backupToolsFolder();

    Path metadataStore();

    Path indexStatisticsStore();

    Path pathForExistsMarker();

    Path pathForStore(CommonDatabaseStores store);

    Set<Path> idFiles();

    Set<Path> storeFiles();

    /**
     * @return the store files required to be present for a database to be able to be recovered
     */
    Set<Path> mandatoryStoreFiles();

    Optional<Path> idFile(DatabaseFile file);

    Path file(String fileName);

    Path file(DatabaseFile databaseFile);

    Stream<Path> allFiles(DatabaseFile databaseFile);

    Path[] listDatabaseFiles(FileSystemAbstraction fs, Predicate<? super Path> filter);
}
