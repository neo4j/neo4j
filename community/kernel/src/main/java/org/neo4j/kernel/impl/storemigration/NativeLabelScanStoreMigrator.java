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
package org.neo4j.kernel.impl.storemigration;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Optional;
import java.util.function.Supplier;

import org.neo4j.common.ProgressReporter;
import org.neo4j.function.Predicates;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.FileHandle;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.scan.FullLabelStream;
import org.neo4j.kernel.impl.index.labelscan.NativeLabelScanStore;
import org.neo4j.kernel.impl.transaction.state.storeview.NeoStoreIndexStoreView;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.lifecycle.LifecycleException;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.storageengine.api.NodeLabelUpdate;
import org.neo4j.storageengine.api.ReadableStorageEngine;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.migration.AbstractStoreMigrationParticipant;

import static org.neo4j.kernel.impl.locking.LockService.NO_LOCK_SERVICE;

class NativeLabelScanStoreMigrator extends AbstractStoreMigrationParticipant
{
    private final FileSystemAbstraction fileSystem;
    private final PageCache pageCache;
    private final Config config;
    private final StorageEngineFactory storageEngineFactory;
    private boolean nativeLabelScanStoreMigrated;

    NativeLabelScanStoreMigrator( FileSystemAbstraction fileSystem, PageCache pageCache, Config config, StorageEngineFactory storageEngineFactory )
    {
        super( "Native label scan index" );
        this.fileSystem = fileSystem;
        this.pageCache = pageCache;
        this.config = config;
        this.storageEngineFactory = storageEngineFactory;
    }

    @Override
    public void migrate( DatabaseLayout directoryLayout, DatabaseLayout migrationLayout, ProgressReporter progressReporter,
            String versionToMigrateFrom, String versionToMigrateTo ) throws IOException
    {
        if ( isNativeLabelScanStoreMigrationRequired( directoryLayout ) )
        {
            try ( Lifespan lifespan = new Lifespan() )
            {
                Dependencies dependencies = new Dependencies();
                // Use Config.defaults() because we don't want to pass in the config which specifies the store format,
                // now that we're referencing the older store.
                dependencies.satisfyDependencies( directoryLayout, Config.defaults(), pageCache, fileSystem, NullLogService.getInstance(),
                        EmptyVersionContextSupplier.EMPTY );
                ReadableStorageEngine storageEngine = lifespan.add( storageEngineFactory.instantiateReadable( dependencies ) );
                try ( StorageReader reader = storageEngine.newReader() )
                {
                    deleteNativeIndexFile( migrationLayout );
                    progressReporter.start( reader.nodesGetCount() );
                    // The label scan store will be rebuilt as part of init/start
                    lifespan.add( getNativeLabelScanStore( migrationLayout, progressReporter, storageEngine::newReader ) );
                }
            }
            catch ( LifecycleException e )
            {
                Throwable cause = e.getCause();
                if ( cause instanceof RuntimeException )
                {
                    throw (RuntimeException) cause;
                }
                if ( cause instanceof IOException )
                {
                    throw (IOException) cause;
                }
                throw new RuntimeException( cause );
            }
            nativeLabelScanStoreMigrated = true;
        }
    }

    @Override
    public void moveMigratedFiles( DatabaseLayout migrationLayout, DatabaseLayout directoryLayout,
            String versionToUpgradeFrom, String versionToMigrateTo ) throws IOException
    {
        if ( nativeLabelScanStoreMigrated )
        {
            File nativeLabelIndex = migrationLayout.labelScanStore();
            moveNativeIndexFile( directoryLayout, nativeLabelIndex );
            deleteLuceneLabelIndex( getLuceneStoreDirectory( directoryLayout ) );
        }
    }

    @Override
    public void cleanup( DatabaseLayout migrationLayout )
    {
        // nop
    }

    private void deleteNativeIndexFile( DatabaseLayout directoryStructure ) throws IOException
    {
        Optional<FileHandle> indexFile = fileSystem.streamFilesRecursive( NativeLabelScanStore.getLabelScanStoreFile( directoryStructure ) ).findFirst();

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

    private void moveNativeIndexFile( DatabaseLayout storeStructure, File nativeLabelIndex ) throws IOException
    {
        Optional<FileHandle> nativeIndexFileHandle = fileSystem.streamFilesRecursive( nativeLabelIndex ).findFirst();
        if ( nativeIndexFileHandle.isPresent() )
        {
            nativeIndexFileHandle.get().rename( storeStructure.labelScanStore() );
        }
    }

    private NativeLabelScanStore getNativeLabelScanStore( DatabaseLayout migrationDirectoryStructure, ProgressReporter progressReporter,
            Supplier<StorageReader> reader )
    {
        NeoStoreIndexStoreView neoStoreIndexStoreView = new NeoStoreIndexStoreView( NO_LOCK_SERVICE, reader );
        return new NativeLabelScanStore( pageCache, migrationDirectoryStructure, fileSystem,
                new MonitoredFullLabelStream( neoStoreIndexStoreView, progressReporter ), false, new Monitors(),
                RecoveryCleanupWorkCollector.immediate() );
    }

    private boolean isNativeLabelScanStoreMigrationRequired( DatabaseLayout directoryStructure ) throws IOException
    {
        return fileSystem.streamFilesRecursive( directoryStructure.labelScanStore() )
                .noneMatch( Predicates.alwaysTrue() );
    }

    private void deleteLuceneLabelIndex( File indexRootDirectory ) throws IOException
    {
        fileSystem.deleteRecursively( indexRootDirectory );
    }

    private static File getLuceneStoreDirectory( DatabaseLayout directoryStructure )
    {
        return new File( new File( new File( directoryStructure.databaseDirectory(), "schema" ), "label" ), "lucene" );
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
