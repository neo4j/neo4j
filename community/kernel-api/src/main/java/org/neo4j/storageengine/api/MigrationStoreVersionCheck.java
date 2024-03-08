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
package org.neo4j.storageengine.api;

import org.neo4j.io.pagecache.context.CursorContext;

public interface MigrationStoreVersionCheck {
    /**
     * Figures out the migration target format version and checks if migration to that version is possible.
     * <p>
     * The migration target version will be the latest version (both latest major and minor) for the submitted format
     * if the submitted format is not {@code null}.
     * If the submitted format is {@code null}, it will be the latest version (both latest major and minor)
     * for the store format the store is currently on.
     */
    MigrationCheckResult getAndCheckMigrationTargetVersion(String formatToMigrateTo, CursorContext cursorContext);

    record MigrationCheckResult(
            MigrationOutcome outcome,
            StoreVersionIdentifier versionToMigrateFrom,
            StoreVersionIdentifier versionToMigrateTo,
            Exception cause) {}

    enum MigrationOutcome {
        // successful outcomes:
        /**
         * The target migration version is the same as the version the store is currently on.
         */
        NO_OP,
        /**
         * The target migration version has been determined and the migration is possible.
         */
        MIGRATION_POSSIBLE,

        // failure outcomes:
        /**
         * The version the store is currently could not be read or understood.
         * <p>
         * Logically, {@link MigrationCheckResult#versionToMigrateFrom()} and {@link MigrationCheckResult#versionToMigrateTo()}
         * are {@code null} for this outcome.
         */
        STORE_VERSION_RETRIEVAL_FAILURE,
        /**
         * The target migration version has been determined,
         * but the migration path from the version the store is currently on to the determined target migration version is not supported.
         */
        UNSUPPORTED_MIGRATION_PATH,
        /**
         * The target migration version has been determined, but it is no longer supported.
         * Since the migration target version is determined as the latest major and minor version combination for the target format,
         * it means that the target format is no longer supported by the current binaries as there is no supported version for the format.
         * The only possible step is migration to another format.
         */
        UNSUPPORTED_TARGET_VERSION,
        /**
         * The target migration version has been determined,
         * and the migration path from the version the store is currently on to the determined target migration version is supported,
         * but the target format will not fit the store because we are going to lower id limits.
         */
        UNSUPPORTED_MIGRATION_LIMITS
    }
}
