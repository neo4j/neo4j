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

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.CountsComputer;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.MetaDataStore.Position;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreFailureException;
import org.neo4j.kernel.impl.store.StoreFile;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.StoreVersion;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_3;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.ReadOnlyIdGeneratorFactory;
import org.neo4j.kernel.impl.storemigration.ExistingTargetStrategy;
import org.neo4j.kernel.impl.storemigration.StoreFileType;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.util.monitoring.ProgressReporter;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;

import static org.neo4j.kernel.impl.store.MetaDataStore.DEFAULT_NAME;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.selectForVersion;
import static org.neo4j.kernel.impl.storemigration.FileOperation.DELETE;
import static org.neo4j.kernel.impl.storemigration.FileOperation.MOVE;

/**
 * Rebuilds the count store during migration.
 * <p>
 * Since the database may or may not reside in the upgrade directory, depending on whether the new format has
 * different capabilities or not, we rebuild the count store using the information the store directory if we fail to
 * open the store in the upgrade directory.
 * <p>
 * Just one out of many potential participants in a {@link StoreUpgrader migration}.
 *
 * @see StoreUpgrader
 */
public class CountsMigrator extends AbstractStoreMigrationParticipant
{
    private static final Iterable<StoreFile> COUNTS_STORE_FILES = Iterables
            .iterable( StoreFile.COUNTS_STORE_LEFT, StoreFile.COUNTS_STORE_RIGHT );

    private final Config config;
    private final FileSystemAbstraction fileSystem;
    private final PageCache pageCache;
    private boolean migrated;

    public CountsMigrator( FileSystemAbstraction fileSystem, PageCache pageCache, Config config )
    {
        super( "Counts store" );
        this.fileSystem = fileSystem;
        this.pageCache = pageCache;
        this.config = config;
    }

    @Override
    public void migrate( File storeDir, File migrationDir, ProgressReporter progressMonitor,
            String versionToMigrateFrom, String versionToMigrateTo ) throws IOException
    {
        if ( countStoreRebuildRequired( versionToMigrateFrom ) )
        {
            // create counters from scratch
            StoreFile.fileOperation( DELETE, fileSystem, migrationDir, migrationDir, COUNTS_STORE_FILES, true, null,
                    StoreFileType.STORE );
            File neoStore = new File( storeDir, DEFAULT_NAME );
            long lastTxId = MetaDataStore.getRecord( pageCache, neoStore, Position.LAST_TRANSACTION_ID );
            try
            {
                rebuildCountsFromScratch( migrationDir, migrationDir, lastTxId, progressMonitor, versionToMigrateTo,
                        pageCache, NullLogProvider.getInstance() );
            }
            catch ( StoreFailureException e )
            {
                //This means that we did not perform a full migration, as the formats had the same capabilities. Thus
                // we should use the store directory for information when rebuilding the count store. Note that we
                // still put the new count store in the migration directory.
                rebuildCountsFromScratch( storeDir, migrationDir, lastTxId, progressMonitor, versionToMigrateFrom,
                        pageCache, NullLogProvider.getInstance() );
            }
            migrated = true;
        }
    }

    @Override
    public void moveMigratedFiles( File migrationDir, File storeDir, String versionToUpgradeFrom,
            String versionToUpgradeTo ) throws IOException
    {

        if ( migrated )
        {
            // Delete any current count files in the store directory.
            StoreFile.fileOperation( DELETE, fileSystem, storeDir, null, COUNTS_STORE_FILES, true, null,
                    StoreFileType.values() );
            // Move the migrated ones into the store directory
            StoreFile.fileOperation( MOVE, fileSystem, migrationDir, storeDir, COUNTS_STORE_FILES, true,
                    // allow to skip non existent source files
                    ExistingTargetStrategy.OVERWRITE, // allow to overwrite target files
                    StoreFileType.values() );
            // We do not need to move files with the page cache, as the count files always reside on the normal file system.
        }
    }

    @Override
    public void cleanup( File migrationDir ) throws IOException
    {
        fileSystem.deleteRecursively( migrationDir );
    }

    @Override
    public String toString()
    {
        return "Kernel Node Count Rebuilder";
    }

    boolean countStoreRebuildRequired( String versionToMigrateFrom )
    {
        return StandardV2_3.STORE_VERSION.equals( versionToMigrateFrom ) ||
               StandardV3_0.STORE_VERSION.equals( versionToMigrateFrom ) ||
               StoreVersion.HIGH_LIMIT_V3_0_0.versionString().equals( versionToMigrateFrom ) ||
               StoreVersion.HIGH_LIMIT_V3_0_6.versionString().equals( versionToMigrateFrom ) ||
               StoreVersion.HIGH_LIMIT_V3_1_0.versionString().equals( versionToMigrateFrom );
    }

    private void rebuildCountsFromScratch( File storeDirToReadFrom, File migrationDir, long lastTxId,
            ProgressReporter progressMonitor, String expectedStoreVersion, PageCache pageCache,
            LogProvider logProvider )
    {
        final File storeFileBase = new File( migrationDir,
                MetaDataStore.DEFAULT_NAME + StoreFactory.COUNTS_STORE );

        RecordFormats recordFormats = selectForVersion( expectedStoreVersion );
        IdGeneratorFactory idGeneratorFactory = new ReadOnlyIdGeneratorFactory( fileSystem );
        StoreFactory storeFactory = new StoreFactory( storeDirToReadFrom, config, idGeneratorFactory, pageCache,
                fileSystem, recordFormats, logProvider, EmptyVersionContextSupplier.EMPTY );
        try ( NeoStores neoStores = storeFactory
                .openNeoStores( StoreType.NODE, StoreType.RELATIONSHIP, StoreType.LABEL_TOKEN,
                        StoreType.RELATIONSHIP_TYPE_TOKEN ) )
        {
            neoStores.verifyStoreOk();
            NodeStore nodeStore = neoStores.getNodeStore();
            RelationshipStore relationshipStore = neoStores.getRelationshipStore();
            try ( Lifespan life = new Lifespan() )
            {
                int highLabelId = (int) neoStores.getLabelTokenStore().getHighId();
                int highRelationshipTypeId = (int) neoStores.getRelationshipTypeTokenStore().getHighId();
                CountsComputer initializer = new CountsComputer( lastTxId, nodeStore, relationshipStore, highLabelId,
                        highRelationshipTypeId, NumberArrayFactory.auto( pageCache, migrationDir, true, NumberArrayFactory.NO_MONITOR ),
                        progressMonitor );
                life.add( new CountsTracker( logProvider, fileSystem, pageCache, config,
                        storeFileBase, EmptyVersionContextSupplier.EMPTY ).setInitializer( initializer ) );
            }
        }
    }
}
