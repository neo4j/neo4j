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
package org.neo4j.kernel.impl.index.schema;

import java.io.IOException;
import org.neo4j.batchimport.api.IndexImporterFactory;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.storageengine.api.StoreVersion;
import org.neo4j.storageengine.migration.StoreMigrationParticipant;

public class NameOverridingStoreMigrationParticipant implements StoreMigrationParticipant {
    private final StoreMigrationParticipant delegate;
    private final String nameOverride;

    public NameOverridingStoreMigrationParticipant(StoreMigrationParticipant delegate, String nameOverride) {
        this.delegate = delegate;
        this.nameOverride = nameOverride;
    }

    @Override
    public void migrate(
            DatabaseLayout directoryLayout,
            DatabaseLayout migrationLayout,
            ProgressListener progress,
            StoreVersion fromVersion,
            StoreVersion toVersion,
            IndexImporterFactory indexImporterFactory,
            LogTailMetadata tailMetadata)
            throws IOException, KernelException {
        delegate.migrate(
                directoryLayout, migrationLayout, progress, fromVersion, toVersion, indexImporterFactory, tailMetadata);
    }

    @Override
    public void moveMigratedFiles(
            DatabaseLayout migrationLayout,
            DatabaseLayout directoryLayout,
            StoreVersion versionToMigrateFrom,
            StoreVersion versionToMigrateTo)
            throws IOException {
        delegate.moveMigratedFiles(migrationLayout, directoryLayout, versionToMigrateFrom, versionToMigrateTo);
    }

    @Override
    public void postMigration(
            DatabaseLayout databaseLayout, StoreVersion toVersion, long txIdBeforeMigration, long txIdAfterMigration)
            throws IOException {
        delegate.postMigration(databaseLayout, toVersion, txIdBeforeMigration, txIdAfterMigration);
    }

    @Override
    public void cleanup(DatabaseLayout migrationLayout) throws IOException {
        delegate.cleanup(migrationLayout);
    }

    @Override
    public String getName() {
        return nameOverride;
    }
}
