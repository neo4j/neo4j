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
package org.neo4j.index.impl.lucene.legacy;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.format.CapabilityType;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.storemigration.monitoring.MigrationProgressMonitor;
import org.neo4j.kernel.impl.storemigration.participant.AbstractStoreMigrationParticipant;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.upgrade.lucene.LegacyIndexMigrationException;
import org.neo4j.upgrade.lucene.LuceneLegacyIndexUpgrader;

import static org.neo4j.helpers.collection.Iterables.asSet;
import static org.neo4j.helpers.collection.Iterables.iterable;
import static org.neo4j.kernel.impl.store.format.StoreVersion.HIGH_LIMIT_V3_0_0;
import static org.neo4j.kernel.impl.store.format.StoreVersion.HIGH_LIMIT_V3_0_6;
import static org.neo4j.kernel.impl.store.format.StoreVersion.STANDARD_V3_0;

/**
 * Store migration participant that responsible for legacy index migration.
 * Aware when and how to perform lucene indexes cross version migration and sort field restoration.
 */
public class LuceneLegacyIndexMigrator extends AbstractStoreMigrationParticipant
{

    private static final Set<String> AFFECTED_VERSIONS = asSet( iterable( STANDARD_V3_0.versionString(),
            HIGH_LIMIT_V3_0_0.versionString(),
            HIGH_LIMIT_V3_0_6.versionString() ) );

    private File migrationLegacyIndexesRoot;
    private File originalLegacyIndexesRoot;
    private final FileSystemAbstraction fileSystem;
    private final LegacyIndexFieldsRestorer fieldsRestorer;

    private final Log log;
    private boolean legacyIndexMigrated = false;

    public LuceneLegacyIndexMigrator( Config config, FileSystemAbstraction fileSystem, LogProvider logProvider )
    {
        this( fileSystem, new LegacyIndexFieldsRestorer( config, fileSystem, logProvider ), logProvider );
    }

    public LuceneLegacyIndexMigrator(FileSystemAbstraction fileSystem, LegacyIndexFieldsRestorer fieldsRestorer,
            LogProvider logProvider)
    {
        super( "Legacy index");
        this.fileSystem = fileSystem;
        this.log = logProvider.getLog( getClass() );
        this.fieldsRestorer = fieldsRestorer;
    }

    @Override
    public void migrate( File storeDir, File migrationDir, MigrationProgressMonitor.Section progressMonitor,
            String versionToMigrateFrom, String versionToMigrateTo ) throws IOException
    {
        RecordFormats from = RecordFormatSelector.selectForVersion( versionToMigrateFrom );
        RecordFormats to = RecordFormatSelector.selectForVersion( versionToMigrateTo );
        originalLegacyIndexesRoot = LuceneDataSource.getLuceneIndexStoreDirectory( storeDir );
        if ( isNotEmptyDirectory( originalLegacyIndexesRoot ) )
        {
            migrationLegacyIndexesRoot = LuceneDataSource.getLuceneIndexStoreDirectory( migrationDir );
            long numberOfIndexes = getNumberOfIndexes();
            if ( !from.hasSameCapabilities( to, CapabilityType.INDEX ) )
            {
                progressMonitor.start( numberOfIndexes << 1 );
                migrateLegacyIndexes( progressMonitor );

                File fieldMigrationDirectory = createIndexFieldMigrationDirectory( migrationDir );
                fieldsRestorer.restoreIndexSortFields( storeDir, migrationDir, fieldMigrationDirectory, progressMonitor );

                fileSystem.deleteRecursively( migrationLegacyIndexesRoot );
                fileSystem.moveToDirectory( LuceneDataSource.getLuceneIndexStoreDirectory( fieldMigrationDirectory ), migrationDir );

                legacyIndexMigrated = true;
            }
            else if ( isVersionAffected( versionToMigrateFrom ) )
            {
                progressMonitor.start( numberOfIndexes );
                fieldsRestorer.restoreIndexSortFields( storeDir, storeDir, migrationDir, progressMonitor );
                legacyIndexMigrated = true;
            }
        }
    }

    private File createIndexFieldMigrationDirectory( File migrationDir )
    {
        for ( int index = 0; ; index++ )
        {
            File sortFieldUpgradeDirectory = new File( migrationDir, getSuffix( index ) );
            if ( !sortFieldUpgradeDirectory.exists() && fileSystem.mkdir( sortFieldUpgradeDirectory ) )
            {
                return sortFieldUpgradeDirectory;
            }
        }
    }

    private String getSuffix( int index )
    {
        return "sortFieldUpgrade" + (index == 0 ? StringUtils.EMPTY : String.valueOf( index ));
    }

    @Override
    public void moveMigratedFiles( File migrationDir, File storeDir, String versionToMigrateFrom,
            String versionToMigrateTo )
            throws IOException
    {
        if ( legacyIndexMigrated )
        {
            fileSystem.deleteRecursively( originalLegacyIndexesRoot );
            fileSystem.moveToDirectory( migrationLegacyIndexesRoot, originalLegacyIndexesRoot.getParentFile() );
        }
    }

    @Override
    public void cleanup( File migrationDir ) throws IOException
    {
        if ( isIndexMigrationDirectoryExists() )
        {
            fileSystem.deleteRecursively( migrationLegacyIndexesRoot );
        }
    }

    private boolean isVersionAffected( String versionToMigrateFrom )
    {
        return AFFECTED_VERSIONS.contains( versionToMigrateFrom );
    }

    private boolean isIndexMigrationDirectoryExists()
    {
        return migrationLegacyIndexesRoot != null && fileSystem.fileExists( migrationLegacyIndexesRoot );
    }

    private void migrateLegacyIndexes( MigrationProgressMonitor.Section progressMonitor ) throws IOException
    {
        try
        {
            fileSystem.copyRecursively( originalLegacyIndexesRoot, migrationLegacyIndexesRoot );
            Path indexRootPath = migrationLegacyIndexesRoot.toPath();
            LuceneLegacyIndexUpgrader indexUpgrader = createLuceneLegacyIndexUpgrader( indexRootPath, progressMonitor );
            indexUpgrader.upgradeIndexes();
        }
        catch ( LegacyIndexMigrationException lime )
        {
            log.error( "Migration of legacy indexes failed. Index: " + lime.getFailedIndexName() + " can't be " +
                    "migrated.", lime );
            throw new IOException( "Legacy index migration failed.", lime );
        }
    }

    LuceneLegacyIndexUpgrader createLuceneLegacyIndexUpgrader( Path indexRootPath,
            MigrationProgressMonitor.Section progressMonitor )
    {
        return new LuceneLegacyIndexUpgrader( indexRootPath, progressMonitor( progressMonitor ) );
    }

    private LuceneLegacyIndexUpgrader.Monitor progressMonitor( MigrationProgressMonitor.Section progressMonitor )
    {
        return new LuceneLegacyIndexUpgrader.Monitor()
        {
            @Override
            public void migrated( String name )
            {
                progressMonitor.progress( 1 );
            }
        };
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

    long getNumberOfIndexes() throws IOException
    {
        try ( Stream<Path> pathStream = Files.walk( originalLegacyIndexesRoot.toPath() ) )
        {
            return pathStream.filter( path -> LuceneUtil.containsIndexFiles( fileSystem, path.toFile() ) ).count();
        }
    }
}
