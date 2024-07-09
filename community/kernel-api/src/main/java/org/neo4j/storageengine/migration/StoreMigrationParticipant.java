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
package org.neo4j.storageengine.migration;

import java.io.IOException;
import org.neo4j.batchimport.api.IndexImporterFactory;
import org.neo4j.exceptions.KernelException;
import org.neo4j.exceptions.UnsatisfiedDependencyException;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.storageengine.api.StoreVersion;

public interface StoreMigrationParticipant {
    /**
     * Default empty implementation of StoreMigrationParticipant
     */
    StoreMigrationParticipant NOT_PARTICIPATING = new AbstractStoreMigrationParticipant("Nothing") {
        @Override
        public void migrate(
                DatabaseLayout directoryLayout,
                DatabaseLayout migrationLayout,
                ProgressListener progress,
                StoreVersion fromVersion,
                StoreVersion toVersion,
                IndexImporterFactory indexImporterFactory,
                LogTailMetadata tailMetadata) {
            // nop
        }

        @Override
        public void moveMigratedFiles(
                DatabaseLayout migrationLayout,
                DatabaseLayout directoryLayout,
                StoreVersion versionToMigrateFrom,
                StoreVersion versionToMigrateTo) {
            // nop
        }

        @Override
        public void cleanup(DatabaseLayout migrationLayout) {
            // nop
        }
    };

    /**
     * Performs migration of data this participant is responsible for if necessary.
     * Data to migrate sits in {@code sourceDirectory} and must not be modified.
     * Migrated data should go into {@code targetStoreDir}, where source and target dirs are
     * the highest level database store dirs.
     *
     * @param directoryLayout data to migrate.
     * @param migrationLayout place to migrate to.
     * @param progress migration progress monitor
     * @param fromVersion the version to migrate from
     * @param toVersion the version to migrate to
     * @param indexImporterFactory the factory to create an index updater to keep the indexes updated.
     * @param tailMetadata metadata about transaction log tail
     * @throws IOException if there was an error migrating.
     * @throws UnsatisfiedDependencyException if one or more dependencies were unsatisfied.
     */
    void migrate(
            DatabaseLayout directoryLayout,
            DatabaseLayout migrationLayout,
            ProgressListener progress,
            StoreVersion fromVersion,
            StoreVersion toVersion,
            IndexImporterFactory indexImporterFactory,
            LogTailMetadata tailMetadata)
            throws IOException, KernelException;

    /**
     * After a successful migration, move all affected files from {@code upgradeDirectory} over to
     * the {@code workingDirectory}, effectively activating the migration changes.
     *
     * @param migrationLayout directory where the {@link #migrate(DatabaseLayout, DatabaseLayout, ProgressListener, StoreVersion,
     * StoreVersion, IndexImporterFactory, LogTailMetadata) migration} put its files.
     * @param directoryLayout directory the store directory of the to move the migrated files to.
     * @param versionToMigrateFrom the version we have migrated from
     * @param versionToMigrateTo the version we want to migrate to
     * @throws IOException if unable to move one or more files.
     */
    void moveMigratedFiles(
            DatabaseLayout migrationLayout,
            DatabaseLayout directoryLayout,
            StoreVersion versionToMigrateFrom,
            StoreVersion versionToMigrateTo)
            throws IOException;

    /**
     * Called after migration and before {@link #cleanup(DatabaseLayout)} and includes transaction IDs before and after
     * migration (there may have been an "upgrade" transaction added after the call to migrate).
     *
     * @param databaseLayout layout of the migrated database.
     * @param toVersion version the store migrated to.
     * @param txIdBeforeMigration last transaction ID before migration started.
     * @param txIdAfterMigration last transaction ID after migration completed (could be higher than before the
     * migration started).
     * @throws IOException on I/o error.
     */
    void postMigration(
            DatabaseLayout databaseLayout, StoreVersion toVersion, long txIdBeforeMigration, long txIdAfterMigration)
            throws IOException;

    /**
     * Delete any file from {@code migrationLayout} produced during migration.
     * @param migrationLayout the directory where migrated files end up.
     * @throws IOException if unable to clean up one or more files.
     */
    void cleanup(DatabaseLayout migrationLayout) throws IOException;

    /**
     * @return descriptive name of this migration participant.
     */
    String getName();
}
