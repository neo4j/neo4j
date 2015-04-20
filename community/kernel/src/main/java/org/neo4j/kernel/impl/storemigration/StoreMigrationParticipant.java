/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.graphdb.Resource;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.impl.util.UnsatisfiedDependencyException;

public interface StoreMigrationParticipant extends Resource
{
    /**
     * Checks to see if the data this participant is responsible for needs migration.
     *
     * @param storeDir directory where the data exists.
     * @return whether or not migration is required.
     * @throws IOException if there was an error checking need for migration, or for example if this participant
     * would have liked to do a migration, but the underlying data makes it impossible.
     */
    boolean needsMigration( File storeDir ) throws IOException;

    /**
     * Performs migration of data this participant is responsible for.
     *
     * Data to migrate sits in {@code sourceDirectory} and must not be modified.
     * Migrated data should go into {@code targetStoreDir}, where source and target dirs are
     * highest level database store dirs.
     *
     * @param storeDir data to migrate.
     * @param migrationDir place to migrate to.
     * @param schemaIndexProvider The SchemaIndexProvider for the migrating database.
     * @param pageCache A page cache instance the participant can use if need be.
     * @throws IOException if there was an error migrating.
     * @throws UnsatisfiedDependencyException if one or more dependencies were unsatisfied.
     */
    void migrate( File storeDir, File migrationDir, SchemaIndexProvider schemaIndexProvider, PageCache pageCache ) throws IOException;

    /**
     * After a successful migration, move all affected files from {@code upgradeDirectory} over to
     * the {@code workingDirectory}, effectively activating the migration changes.
     * @param migrationDir directory where the
     * {@link #migrate(java.io.File, java.io.File, org.neo4j.kernel.api.index.SchemaIndexProvider, org.neo4j.io.pagecache.PageCache) migration}
     * put its files.
     * @param storeDir directory the store directory of the to move the migrated files to.
     * @throws IOException if unable to move one or more files.
     */
    void moveMigratedFiles( File migrationDir, File storeDir ) throws IOException;

    /**
     * Closes any resources kept open by this migration participant.
     */
    @Override
    void close();

    /**
     * Delete any file from {@code migrationDir} produced during migration.
     * @param migrationDir the directory where migrated files end up.
     * @throws IOException if unable to clean up one or more files.
     */
    void cleanup( File migrationDir ) throws IOException;

    public static final StoreMigrationParticipant NOT_PARTICIPATING = new StoreMigrationParticipant()
    {
        @Override
        public boolean needsMigration( File sourceStoreDir ) throws IOException
        {   // Default to not needing migration
            return false;
        }

        @Override
        public void migrate( File sourceStoreDir, File targetStoreDir, SchemaIndexProvider schemaIndexProvider,
                             PageCache pageCache )
                throws IOException, UnsatisfiedDependencyException
        {
            throw new UnsupportedOperationException( "Should not have been called" );
        }

        @Override
        public void moveMigratedFiles( File migrationDirectory, File workingDirectory ) throws IOException
        {
            throw new UnsupportedOperationException( "Should not have been called" );
        }

        @Override
        public void close()
        { // nothing to do
        }

        @Override
        public void cleanup( File migrationDir ) throws IOException
        { // nothing to do
        }
    };
}
