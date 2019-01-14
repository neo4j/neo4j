/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.storemigration.participant;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.format.CapabilityType;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.util.monitoring.ProgressReporter;
import org.neo4j.kernel.spi.explicitindex.IndexImplementation;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.upgrade.lucene.ExplicitIndexMigrationException;
import org.neo4j.upgrade.lucene.LuceneExplicitIndexUpgrader;
import org.neo4j.upgrade.lucene.LuceneExplicitIndexUpgrader.Monitor;

/**
 * Migrates explicit lucene indexes between different neo4j versions.
 * Participates in store upgrade as one of the migration participants.
 */
public class ExplicitIndexMigrator extends AbstractStoreMigrationParticipant
{
    private static final String LUCENE_EXPLICIT_INDEX_PROVIDER_NAME = "lucene";
    private final Map<String,IndexImplementation> indexProviders;
    private final FileSystemAbstraction fileSystem;
    private File migrationExplicitIndexesRoot;
    private File originalExplicitIndexesRoot;
    private final Log log;
    private boolean explicitIndexMigrated;

    public ExplicitIndexMigrator( FileSystemAbstraction fileSystem, Map<String,IndexImplementation> indexProviders,
            LogProvider logProvider )
    {
        super( "Explicit indexes" );
        this.fileSystem = fileSystem;
        this.indexProviders = indexProviders;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void migrate( File storeDir, File migrationDir, ProgressReporter progressMonitor,
            String versionToMigrateFrom, String versionToMigrateTo ) throws IOException
    {
        IndexImplementation indexImplementation = indexProviders.get( LUCENE_EXPLICIT_INDEX_PROVIDER_NAME );
        if ( indexImplementation != null )
        {
            RecordFormats from = RecordFormatSelector.selectForVersion( versionToMigrateFrom );
            RecordFormats to = RecordFormatSelector.selectForVersion( versionToMigrateTo );
            if ( !from.hasCompatibleCapabilities( to, CapabilityType.INDEX ) )
            {
                originalExplicitIndexesRoot = indexImplementation.getIndexImplementationDirectory( storeDir );
                migrationExplicitIndexesRoot = indexImplementation.getIndexImplementationDirectory( migrationDir );
                if ( isNotEmptyDirectory( originalExplicitIndexesRoot ) )
                {
                    migrateExplicitIndexes( progressMonitor );
                    explicitIndexMigrated = true;
                }
            }
        }
        else
        {
            log.debug( "Lucene index provider not found, nothing to migrate." );
        }
    }

    @Override
    public void moveMigratedFiles( File migrationDir, File storeDir, String versionToMigrateFrom,
            String versionToMigrateTo )
            throws IOException
    {
        if ( explicitIndexMigrated )
        {
            fileSystem.deleteRecursively( originalExplicitIndexesRoot );
            fileSystem.moveToDirectory( migrationExplicitIndexesRoot, originalExplicitIndexesRoot.getParentFile() );
        }
    }

    @Override
    public void cleanup( File migrationDir ) throws IOException
    {
        if ( isIndexMigrationDirectoryExists() )
        {
            fileSystem.deleteRecursively( migrationExplicitIndexesRoot );
        }
    }

    private boolean isIndexMigrationDirectoryExists()
    {
        return migrationExplicitIndexesRoot != null && fileSystem.fileExists( migrationExplicitIndexesRoot );
    }

    private void migrateExplicitIndexes( ProgressReporter progressMonitor ) throws IOException
    {
        try
        {
            fileSystem.copyRecursively( originalExplicitIndexesRoot, migrationExplicitIndexesRoot );
            Path indexRootPath = migrationExplicitIndexesRoot.toPath();
            LuceneExplicitIndexUpgrader indexUpgrader = createLuceneExplicitIndexUpgrader( indexRootPath, progressMonitor );
            indexUpgrader.upgradeIndexes();
        }
        catch ( ExplicitIndexMigrationException lime )
        {
            log.error( "Migration of explicit indexes failed. Index: " + lime.getFailedIndexName() + " can't be " +
                       "migrated.", lime );
            throw new IOException( "Explicit index migration failed.", lime );
        }
    }

    private boolean isNotEmptyDirectory( File file )
    {
        if ( fileSystem.isDirectory( file ) )
        {
            File[] files = fileSystem.listFiles( file );
            return files != null && files.length > 0;
        }
        return false;
    }

    LuceneExplicitIndexUpgrader createLuceneExplicitIndexUpgrader( Path indexRootPath,
            ProgressReporter progressMonitor )
    {
        return new LuceneExplicitIndexUpgrader( indexRootPath, progressMonitor( progressMonitor ) );
    }

    private Monitor progressMonitor( ProgressReporter progressMonitor )
    {
        return new Monitor()
        {
            @Override
            public void starting( int total )
            {
                progressMonitor.start( total );
            }

            @Override
            public void migrated( String name )
            {
                progressMonitor.progress( 1 );
            }
        };
    }
}
