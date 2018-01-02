/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import java.io.File;
import java.io.IOException;

import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.impl.util.UnsatisfiedDependencyException;

public interface StoreMigrationParticipant
{
    /**
     * Performs migration of data this participant is responsible for if necessary.
     *
     * Data to migrate sits in {@code sourceDirectory} and must not be modified.
     * Migrated data should go into {@code targetStoreDir}, where source and target dirs are
     * highest level database store dirs.
     *
     * @param storeDir data to migrate.
     * @param migrationDir place to migrate to.
     * @param schemaIndexProvider The SchemaIndexProvider for the migrating database.
     * @param versionToMigrateFrom the version to migrate from
     * @throws IOException if there was an error migrating.
     * @throws UnsatisfiedDependencyException if one or more dependencies were unsatisfied.
     */
    void migrate( File storeDir, File migrationDir, SchemaIndexProvider schemaIndexProvider,
            String versionToMigrateFrom ) throws IOException;

    /**
     * After a successful migration, move all affected files from {@code upgradeDirectory} over to
     * the {@code workingDirectory}, effectively activating the migration changes.
     * @param migrationDir directory where the
     * {@link #migrate(File, File, SchemaIndexProvider, String) migration}
     * put its files.
     * @param storeDir directory the store directory of the to move the migrated files to.
     * @param versionToMigrateFrom the version we have migrated from
     * @throws IOException if unable to move one or more files.
     */
    void moveMigratedFiles( File migrationDir, File storeDir, String versionToMigrateFrom ) throws IOException;

    /**
     * After a successful migration, and having moved all affected files from {@code upgradeDirectory} over to
     * the {@code workingDirectory}, this will rebuild counts if needed.

     * @param storeDir directory the store directory of the to move the migrated files to.
     * @param versionToMigrateFrom the version we have migrated from
     * @throws IOException if unable to move one or more files.
     */
    void rebuildCounts( File storeDir, String versionToMigrateFrom ) throws IOException;

    /**
     * Delete any file from {@code migrationDir} produced during migration.
     * @param migrationDir the directory where migrated files end up.
     * @throws IOException if unable to clean up one or more files.
     */
    void cleanup( File migrationDir ) throws IOException;

    StoreMigrationParticipant NOT_PARTICIPATING = new StoreMigrationParticipant()
    {
        @Override
        public void migrate( File sourceStoreDir, File targetStoreDir, SchemaIndexProvider schemaIndexProvider,
                String versionToMigrateFrom ) throws IOException, UnsatisfiedDependencyException
        {
            throw new UnsupportedOperationException( "Should not have been called" );
        }

        @Override
        public void moveMigratedFiles( File migrationDirectory, File workingDirectory, String versionToUpgradeFrom ) throws IOException
        {
            throw new UnsupportedOperationException( "Should not have been called" );
        }

        @Override
        public void rebuildCounts( File storeDirectory, String versionToMigrateFrom )
        {
        }

        @Override
        public void cleanup( File migrationDir ) throws IOException
        { // nothing to do
        }
    };
}
