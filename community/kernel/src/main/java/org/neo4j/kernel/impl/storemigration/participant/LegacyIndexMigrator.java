/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.storemigration.participant;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.kernel.impl.storemigration.monitoring.MigrationProgressMonitor;
import org.neo4j.kernel.spi.legacyindex.IndexImplementation;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * Migrates legacy lucene indexes between different neo4j versions.
 * Participates in store upgrade as one of the migration participants.
 */
public class LegacyIndexMigrator extends AbstractStoreMigrationParticipant
{
    private static final String LUCENE_LEGACY_INDEX_PROVIDER_NAME = "lucene";

    private final StoreMigrationParticipant delegate;

    public LegacyIndexMigrator( Map<String,IndexImplementation> indexProviders, LogProvider logProvider )
    {
        this( getLegacyStoreMigrator( indexProviders, logProvider.getLog( LegacyIndexMigrator.class ) ) );
    }

    public LegacyIndexMigrator( StoreMigrationParticipant delegate )
    {
        super( "Legacy indexes" );
        this.delegate = delegate;
    }

    @Override
    public void migrate( File storeDir, File migrationDir, MigrationProgressMonitor.Section progressMonitor,
            String versionToMigrateFrom, String versionToMigrateTo ) throws IOException
    {
        delegate.migrate( storeDir, migrationDir, progressMonitor, versionToMigrateFrom, versionToMigrateTo );
    }

    @Override
    public void moveMigratedFiles( File migrationDir, File storeDir, String versionToMigrateFrom,
            String versionToMigrateTo ) throws IOException
    {
        delegate.moveMigratedFiles( migrationDir, storeDir, versionToMigrateFrom, versionToMigrateTo );
    }

    @Override
    public void cleanup( File migrationDir ) throws IOException
    {
        delegate.cleanup( migrationDir );
    }

    private static StoreMigrationParticipant getLegacyStoreMigrator( Map<String,IndexImplementation> indexProviders,
            Log log )
    {
        IndexImplementation indexImplementation = indexProviders.get( LUCENE_LEGACY_INDEX_PROVIDER_NAME );
        if ( indexImplementation != null )
        {
            return indexImplementation.getStoreMigrator();
        }
        else
        {
            log.debug( "Lucene index provider not found, will do nothing during migration." );
            return AbstractStoreMigrationParticipant.NOT_PARTICIPATING;
        }
    }
}
