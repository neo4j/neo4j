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
package org.neo4j.io.layout.recordstorage;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;

/**
 * Enumeration of storage implementation specific files for particular database.
 * Any internal details of this enumeration is hidden and should not be visible to anyone except of implementation of specific database layout.
 * Should be used only for referencing back specific files in the database layout based on different store types.
 * <br>
 * Any database file that represented here can have internal details like several actual file names, other internal characteristic that are store specific.
 *
 * @see DatabaseLayout
 * @see Neo4jLayout
 */
public enum RecordDatabaseFile implements DatabaseFile {
    NODE_STORE(RecordDatabaseFileNames.NODE_STORE),

    NODE_LABEL_STORE(RecordDatabaseFileNames.NODE_LABELS_STORE),

    PROPERTY_STORE(RecordDatabaseFileNames.PROPERTY_STORE),

    PROPERTY_ARRAY_STORE(RecordDatabaseFileNames.PROPERTY_ARRAY_STORE),

    PROPERTY_STRING_STORE(RecordDatabaseFileNames.PROPERTY_STRING_STORE),

    PROPERTY_KEY_TOKEN_STORE(RecordDatabaseFileNames.PROPERTY_KEY_TOKEN_STORE),

    PROPERTY_KEY_TOKEN_NAMES_STORE(RecordDatabaseFileNames.PROPERTY_KEY_TOKEN_NAMES_STORE),

    RELATIONSHIP_STORE(RecordDatabaseFileNames.RELATIONSHIP_STORE),

    RELATIONSHIP_GROUP_STORE(RecordDatabaseFileNames.RELATIONSHIP_GROUP_STORE),

    RELATIONSHIP_TYPE_TOKEN_STORE(RecordDatabaseFileNames.RELATIONSHIP_TYPE_TOKEN_STORE),

    RELATIONSHIP_TYPE_TOKEN_NAMES_STORE(RecordDatabaseFileNames.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE),

    LABEL_TOKEN_STORE(RecordDatabaseFileNames.LABEL_TOKEN_STORE),

    LABEL_TOKEN_NAMES_STORE(RecordDatabaseFileNames.LABEL_TOKEN_NAMES_STORE),

    SCHEMA_STORE(RecordDatabaseFileNames.SCHEMA_STORE),

    COUNTS_STORE(RecordDatabaseFileNames.COUNTS_STORE, false),

    RELATIONSHIP_GROUP_DEGREES_STORE(RecordDatabaseFileNames.RELATIONSHIP_GROUP_DEGREES_STORE, false),

    INDEX_STATISTICS_STORE(RecordDatabaseFileNames.INDEX_STATISTICS_STORE, false),

    METADATA_STORE(RecordDatabaseFileNames.METADATA_STORE, false),

    EXISTS_MARKER(METADATA_STORE);

    private final String name;
    private final boolean hasIdFile;

    RecordDatabaseFile(String name) {
        this(name, true);
    }

    RecordDatabaseFile(String name, boolean hasIdFile) {
        this.name = name;
        this.hasIdFile = hasIdFile;
    }

    // used for marker
    RecordDatabaseFile(RecordDatabaseFile file) {
        this.name = file.getName();
        this.hasIdFile = false;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean hasIdFile() {
        return hasIdFile;
    }

    /**
     * Determine database file for provided file name.
     *
     * @param name - database file name to map
     * @return an {@link Optional} that wraps the matching database file that matches to the specified name,
     * or {@link Optional#empty()} if the given file name does not match to any of database files.
     */
    public static Optional<RecordDatabaseFile> fileOf(String name) {
        Objects.requireNonNull(name);
        return ALL_FILES.stream()
                .filter(file -> file.getName().equals(name))
                // EXISTS_MARKER currently shares a name with METADATA_STORE, most likely user wants the METADATA_STORE
                .filter(Predicate.not(EXISTS_MARKER::equals))
                .findFirst();
    }

    private static final Set<RecordDatabaseFile> ALL_FILES = Set.of(values());

    static final Set<RecordDatabaseFile> STORE_FILES = ALL_FILES;

    static final Set<RecordDatabaseFile> RECOVERABLE_STORE_FILES =
            Set.of(COUNTS_STORE, RELATIONSHIP_GROUP_DEGREES_STORE, INDEX_STATISTICS_STORE);
}
