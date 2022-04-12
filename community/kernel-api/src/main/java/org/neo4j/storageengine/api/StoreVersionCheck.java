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
package org.neo4j.storageengine.api;

import java.util.Optional;

import org.neo4j.io.pagecache.context.CursorContext;

public interface StoreVersionCheck
{
    /**
     * Store version of an existing store (this instance knows which store it's about).
     *
     * @param cursorContext underlying page cursor context.
     * @return store version of the existing store.
     */
    Optional<String> storeVersion( CursorContext cursorContext );

    /**
     * Convert the a store version to String form.
     *
     * @param storeVersion the store version to convert
     * @return store version of the existing store.
     */
    String storeVersionToString( long storeVersion );

    /**
     * Figures out the migration target version and checks if migration to that version is possible.
     * <p>
     * The migration target version will be the latest store version (both latest major and minor) for the submitted format family
     * if the submitted format family is not {@code null}.
     * If the submitted format family is {@code null }, it will be the latest store version (both latest major and minor)
     * for the store family the store is currently on.
     */
    MigrationCheckResult getAndCheckMigrationTargetVersion( String formatFamily, CursorContext cursorContext );

    /**
     * Figures out the upgrade target version and checks if upgrade to that version is possible.
     * <p>
     * The upgrade target version is the latest minor format version for the combination of the format family
     * and the major version of the store format the store is currently on.
     */
    UpgradeCheckResult getAndCheckUpgradeTargetVersion( CursorContext cursorContext );

    record MigrationCheckResult(MigrationOutcome outcome, String versionToMigrateFrom, String versionToMigrateTo, Exception cause )
    {

    }

    enum MigrationOutcome
    {
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
         * Since the migration target version is determined as the latest major and minor version combination for the target format family,
         * it means that the target format family is no longer supported by the current binaries as there is no supported format version for the family.
         * The only possible step is migration to another format family.
         */
        UNSUPPORTED_TARGET_VERSION,
    }

    record UpgradeCheckResult(UpgradeOutcome outcome, String versionToUpgradeFrom, String versionToUpgradeTo, Exception cause )
    {

    }

    enum UpgradeOutcome
    {
        // successful outcomes:

        /**
         * The target upgrade version is the same as the version the store is currently on.
         */
        NO_OP,
        /**
         * The target upgrade version has been determined and the migration is possible.
         */
        UPGRADE_POSSIBLE,

        // failure outcomes:
        /**
         * The version the store is currently could not be read or understod.
         * <p>
         * Logically, {@link UpgradeCheckResult#versionToUpgradeFrom()} and {@link UpgradeCheckResult#versionToUpgradeTo()} are {@code null} for this outcome.
         */
        STORE_VERSION_RETRIEVAL_FAILURE,
        /**
         * The target upgrade version has been determined, but it is no longer supported.
         * Since the upgrade target version is determined as the latest minor version of the format the store is currently on,
         * it means that the current major version of the store format is no longer supported by the current binaries.
         * The only possible step is migration to another major version of the current format family or to another format family.
         */
        UNSUPPORTED_TARGET_VERSION
    }
}
