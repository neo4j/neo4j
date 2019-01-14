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
import java.nio.file.NoSuchFileException;
import java.util.Optional;

import org.neo4j.function.Predicates;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.FileHandle;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.scan.FullLabelStream;
import org.neo4j.kernel.impl.index.labelscan.NativeLabelScanStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.ReadOnlyIdGeneratorFactory;
import org.neo4j.kernel.impl.transaction.state.storeview.NeoStoreIndexStoreView;
import org.neo4j.kernel.impl.util.monitoring.ProgressReporter;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;

import static org.neo4j.kernel.impl.locking.LockService.NO_LOCK_SERVICE;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.selectForVersion;

public class NativeLabelScanStoreMigrator extends AbstractStoreMigrationParticipant
{
    private final FileSystemAbstraction fileSystem;
    private final PageCache pageCache;
    private final Config config;
    private boolean nativeLabelScanStoreMigrated;

    public NativeLabelScanStoreMigrator( FileSystemAbstraction fileSystem, PageCache pageCache, Config config )
    {
        super( "Native label scan index" );
        this.fileSystem = fileSystem;
        this.pageCache = pageCache;
        this.config = config;
    }

    @Override
    public void migrate( File storeDir, File migrationDir, ProgressReporter progressReporter,
            String versionToMigrateFrom, String versionToMigrateTo ) throws IOException
    {
        if ( isNativeLabelScanStoreMigrationRequired( storeDir ) )
        {
            StoreFactory storeFactory = getStoreFactory( storeDir, versionToMigrateFrom );
            try ( NeoStores neoStores = storeFactory.openAllNeoStores();
                    Lifespan lifespan = new Lifespan() )
            {
                neoStores.verifyStoreOk();
                // Remove any existing file to ensure we always do migration
                deleteNativeIndexFile( migrationDir );

                progressReporter.start( neoStores.getNodeStore().getNumberOfIdsInUse() );
                NativeLabelScanStore nativeLabelScanStore = getNativeLabelScanStore( migrationDir, progressReporter, neoStores );
                lifespan.add( nativeLabelScanStore );
            }
            nativeLabelScanStoreMigrated = true;
        }
    }

    @Override
    public void moveMigratedFiles( File migrationDir, File storeDir, String versionToUpgradeFrom,
            String versionToMigrateTo ) throws IOException
    {
        if ( nativeLabelScanStoreMigrated )
        {
            File nativeLabelIndex = new File( migrationDir, NativeLabelScanStore.FILE_NAME );
            moveNativeIndexFile( storeDir, nativeLabelIndex );
            deleteLuceneLabelIndex( getLuceneStoreDirectory( storeDir ) );
        }
    }

    private void deleteNativeIndexFile( File storeDir ) throws IOException
    {
        Optional<FileHandle> indexFile = pageCache.getCachedFileSystem()
                .streamFilesRecursive( NativeLabelScanStore.getLabelScanStoreFile( storeDir ) ).findFirst();

        if ( indexFile.isPresent() )
        {
            try
            {
                indexFile.get().delete();
            }
            catch ( NoSuchFileException e )
            {
                // Already deleted, ignore
            }
        }
    }

    private void moveNativeIndexFile( File storeDir, File nativeLabelIndex ) throws IOException
    {
        Optional<FileHandle> nativeIndexFileHandle =
                pageCache.getCachedFileSystem().streamFilesRecursive( nativeLabelIndex ).findFirst();
        if ( nativeIndexFileHandle.isPresent() )
        {
            nativeIndexFileHandle.get().rename( new File( storeDir, NativeLabelScanStore.FILE_NAME ) );
        }
    }

    private NativeLabelScanStore getNativeLabelScanStore( File migrationDir,
            ProgressReporter progressReporter, NeoStores neoStores )
    {
        NeoStoreIndexStoreView neoStoreIndexStoreView = new NeoStoreIndexStoreView( NO_LOCK_SERVICE, neoStores );
        return new NativeLabelScanStore( pageCache, fileSystem, migrationDir,
                new MonitoredFullLabelStream( neoStoreIndexStoreView, progressReporter ), false, new Monitors(),
                RecoveryCleanupWorkCollector.immediate() );
    }

    private StoreFactory getStoreFactory( File storeDir, String versionToMigrateFrom )
    {
        NullLogProvider logProvider = NullLogProvider.getInstance();
        RecordFormats recordFormats = selectForVersion( versionToMigrateFrom );
        IdGeneratorFactory idGeneratorFactory = new ReadOnlyIdGeneratorFactory( fileSystem );
        return new StoreFactory( storeDir, config, idGeneratorFactory, pageCache, fileSystem,
                recordFormats, logProvider, EmptyVersionContextSupplier.EMPTY );
    }

    private boolean isNativeLabelScanStoreMigrationRequired( File storeDir ) throws IOException
    {
        return pageCache.getCachedFileSystem()
                .streamFilesRecursive( new File( storeDir, NativeLabelScanStore.FILE_NAME ) )
                .noneMatch( Predicates.alwaysTrue() );
    }

    private void deleteLuceneLabelIndex( File indexRootDirectory ) throws IOException
    {
        fileSystem.deleteRecursively( indexRootDirectory );
    }

    private static File getLuceneStoreDirectory( File storeRootDir )
    {
        return new File( new File( new File( storeRootDir, "schema" ), "label" ), "lucene" );
    }

    private static class MonitoredFullLabelStream extends FullLabelStream
    {

        private final ProgressReporter progressReporter;

        MonitoredFullLabelStream( IndexStoreView indexStoreView, ProgressReporter progressReporter )
        {
            super( indexStoreView );
            this.progressReporter = progressReporter;
        }

        @Override
        public boolean visit( NodeLabelUpdate update ) throws IOException
        {
            boolean visit = super.visit( update );
            progressReporter.progress( 1 );
            return visit;
        }
    }
}
