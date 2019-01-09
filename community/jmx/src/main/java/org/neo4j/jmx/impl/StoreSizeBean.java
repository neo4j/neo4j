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
package org.neo4j.jmx.impl;

import org.apache.commons.lang3.mutable.MutableLong;

import java.io.File;
import java.io.IOException;
import java.time.Clock;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.Service;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.jmx.StoreSize;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.api.ExplicitIndexProviderLookup;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.store.StoreFile;
import org.neo4j.kernel.impl.storemigration.StoreFileType;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogVersionVisitor;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.spi.explicitindex.IndexImplementation;
import org.neo4j.util.VisibleForTesting;

import static java.util.Objects.requireNonNull;
import static org.neo4j.jmx.impl.ThrottlingBeanSnapshotProxy.newThrottlingBeanSnapshotProxy;
import static org.neo4j.kernel.impl.store.StoreFile.COUNTS_STORE_LEFT;
import static org.neo4j.kernel.impl.store.StoreFile.COUNTS_STORE_RIGHT;
import static org.neo4j.kernel.impl.store.StoreFile.LABEL_TOKEN_NAMES_STORE;
import static org.neo4j.kernel.impl.store.StoreFile.LABEL_TOKEN_STORE;
import static org.neo4j.kernel.impl.store.StoreFile.NODE_LABEL_STORE;
import static org.neo4j.kernel.impl.store.StoreFile.NODE_STORE;
import static org.neo4j.kernel.impl.store.StoreFile.PROPERTY_ARRAY_STORE;
import static org.neo4j.kernel.impl.store.StoreFile.PROPERTY_KEY_TOKEN_NAMES_STORE;
import static org.neo4j.kernel.impl.store.StoreFile.PROPERTY_KEY_TOKEN_STORE;
import static org.neo4j.kernel.impl.store.StoreFile.PROPERTY_STORE;
import static org.neo4j.kernel.impl.store.StoreFile.PROPERTY_STRING_STORE;
import static org.neo4j.kernel.impl.store.StoreFile.RELATIONSHIP_GROUP_STORE;
import static org.neo4j.kernel.impl.store.StoreFile.RELATIONSHIP_STORE;
import static org.neo4j.kernel.impl.store.StoreFile.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE;
import static org.neo4j.kernel.impl.store.StoreFile.RELATIONSHIP_TYPE_TOKEN_STORE;
import static org.neo4j.kernel.impl.store.StoreFile.SCHEMA_STORE;

@Service.Implementation( ManagementBeanProvider.class )
public final class StoreSizeBean extends ManagementBeanProvider
{
    private static final long UPDATE_INTERVAL = 60000;
    private static final StoreSize NO_STORE_SIZE = new StoreSize()
    {
        @Override
        public long getTransactionLogsSize()
        {
            return 0;
        }

        @Override
        public long getNodeStoreSize()
        {
            return 0;
        }

        @Override
        public long getRelationshipStoreSize()
        {
            return 0;
        }

        @Override
        public long getPropertyStoreSize()
        {
            return 0;
        }

        @Override
        public long getStringStoreSize()
        {
            return 0;
        }

        @Override
        public long getArrayStoreSize()
        {
            return 0;
        }

        @Override
        public long getLabelStoreSize()
        {
            return 0;
        }

        @Override
        public long getCountStoreSize()
        {
            return 0;
        }

        @Override
        public long getSchemaStoreSize()
        {
            return 0;
        }

        @Override
        public long getIndexStoreSize()
        {
            return 0;
        }

        @Override
        public long getTotalStoreSize()
        {
            return 0;
        }
    };

    @SuppressWarnings( "WeakerAccess" ) // Bean needs public constructor
    public StoreSizeBean()
    {
        super( StoreSize.class );
    }

    @Override
    protected Neo4jMBean createMBean( ManagementData management )
    {
        return createBean( management, false, UPDATE_INTERVAL, Clock.systemUTC() );
    }

    @Override
    protected Neo4jMBean createMXBean( ManagementData management )
    {
        return createBean( management, true, UPDATE_INTERVAL, Clock.systemUTC() );
    }

    @VisibleForTesting
    static StoreSizeMBean createBean( ManagementData management, boolean isMxBean, long updateInterval, Clock clock )
    {
        final StoreSizeMBean bean = new StoreSizeMBean( management, isMxBean, updateInterval, clock );
        final DataSourceManager dataSourceManager = management.resolveDependency( DataSourceManager.class );
        dataSourceManager.addListener( bean );
        return bean;
    }

    static class StoreSizeMBean extends Neo4jMBean implements StoreSize, DataSourceManager.Listener
    {
        private final FileSystemAbstraction fs;
        private final File storePath;
        private final long updateInterval;
        private final Clock clock;

        private volatile StoreSize delegate = NO_STORE_SIZE;

        StoreSizeMBean( ManagementData management, boolean isMXBean, long updateInterval, Clock clock )
        {
            super( management, isMXBean );
            this.fs = management.getKernelData().getFilesystemAbstraction();
            this.storePath = resolveStorePath( management );
            this.updateInterval = updateInterval;
            this.clock = clock;
        }

        @Override
        public void registered( NeoStoreDataSource ds )
        {
            final StoreSizeProvider dataProvider = new StoreSizeProvider( fs, storePath, ds.getDependencyResolver() );
            this.delegate = newThrottlingBeanSnapshotProxy( StoreSize.class, dataProvider, updateInterval, clock );
        }

        @Override
        public void unregistered( NeoStoreDataSource ds )
        {
            this.delegate = NO_STORE_SIZE;
        }

        @Override
        public long getTransactionLogsSize()
        {
            return delegate.getTransactionLogsSize();
        }

        @Override
        public long getNodeStoreSize()
        {
            return delegate.getNodeStoreSize();
        }

        @Override
        public long getRelationshipStoreSize()
        {
            return delegate.getRelationshipStoreSize();
        }

        @Override
        public long getPropertyStoreSize()
        {
            return delegate.getPropertyStoreSize();
        }

        @Override
        public long getStringStoreSize()
        {
            return delegate.getStringStoreSize();
        }

        @Override
        public long getArrayStoreSize()
        {
            return delegate.getArrayStoreSize();
        }

        @Override
        public long getLabelStoreSize()
        {
            return delegate.getLabelStoreSize();
        }

        @Override
        public long getCountStoreSize()
        {
            return delegate.getCountStoreSize();
        }

        @Override
        public long getSchemaStoreSize()
        {
            return delegate.getSchemaStoreSize();
        }

        @Override
        public long getIndexStoreSize()
        {
            return delegate.getIndexStoreSize();
        }

        @Override
        public long getTotalStoreSize()
        {
            return delegate.getTotalStoreSize();
        }
    }

    static File resolveStorePath( ManagementData management )
    {
        File storeDir = management.getKernelData().getStoreDir();
        try
        {
            return storeDir.getCanonicalFile().getAbsoluteFile();
        }
        catch ( IOException e )
        {
            return storeDir.getAbsoluteFile();
        }
    }

    private static class StoreSizeProvider implements StoreSize
    {
        private final FileSystemAbstraction fs;
        private final File storePath;
        private final LogFiles logFiles;
        private final ExplicitIndexProviderLookup explicitIndexProviderLookup;
        private final IndexProviderMap indexProviderMap;
        private final LabelScanStore labelScanStore;

        private StoreSizeProvider( FileSystemAbstraction fs, File storePath, DependencyResolver deps )
        {
            this.fs = requireNonNull( fs );
            this.storePath = requireNonNull( storePath );
            this.logFiles = deps.resolveDependency( LogFiles.class );
            this.explicitIndexProviderLookup = deps.resolveDependency( ExplicitIndexProviderLookup.class );
            this.indexProviderMap = deps.resolveDependency( IndexProviderMap.class );
            this.labelScanStore = deps.resolveDependency( LabelScanStore.class );
        }

        @Override
        public long getTransactionLogsSize()
        {
            final TotalSizeVersionVisitor logVersionVisitor = new TotalSizeVersionVisitor();
            logFiles.accept( logVersionVisitor );
            return logVersionVisitor.getTotalSize();
        }

        @Override
        public long getNodeStoreSize()
        {
            return sizeOfStoreFiles( NODE_STORE, NODE_LABEL_STORE );
        }

        @Override
        public long getRelationshipStoreSize()
        {
            return sizeOfStoreFiles( RELATIONSHIP_STORE, RELATIONSHIP_GROUP_STORE, RELATIONSHIP_TYPE_TOKEN_STORE,
                    RELATIONSHIP_TYPE_TOKEN_NAMES_STORE );
        }

        @Override
        public long getPropertyStoreSize()
        {
            return sizeOfStoreFiles( PROPERTY_STORE, PROPERTY_KEY_TOKEN_STORE, PROPERTY_KEY_TOKEN_NAMES_STORE );
        }

        @Override
        public long getStringStoreSize()
        {
            return sizeOfStoreFiles( PROPERTY_STRING_STORE );
        }

        @Override
        public long getArrayStoreSize()
        {
            return sizeOfStoreFiles( PROPERTY_ARRAY_STORE );
        }

        @Override
        public long getLabelStoreSize()
        {
            return sizeOfStoreFiles( LABEL_TOKEN_STORE, LABEL_TOKEN_NAMES_STORE );
        }

        @Override
        public long getCountStoreSize()
        {
            return sizeOfStoreFiles( COUNTS_STORE_LEFT, COUNTS_STORE_RIGHT );
        }

        @Override
        public long getSchemaStoreSize()
        {
            return sizeOfStoreFiles( SCHEMA_STORE );
        }

        @Override
        public long getIndexStoreSize()
        {
            long size = 0L;

            // Add explicit indices
            for ( IndexImplementation index : explicitIndexProviderLookup.all() )
            {
                size += FileUtils.size( fs, index.getIndexImplementationDirectory( storePath ) );
            }

            // Add schema index
            MutableLong schemaSize = new MutableLong();
            indexProviderMap.accept( provider ->
            {
                File rootDirectory = provider.directoryStructure().rootDirectory();
                if ( rootDirectory != null )
                {
                    schemaSize.add( FileUtils.size( fs, rootDirectory ) );
                }
                // else this provider didn't have any persistent storage
            } );
            size += schemaSize.longValue();

            // Add label index
            size += FileUtils.size( fs, labelScanStore.getLabelScanStoreFile() );

            return size;
        }

        @Override
        public long getTotalStoreSize()
        {
            return storePath == null ? 0L : FileUtils.size( fs, storePath );
        }

        private long sizeOf( String name )
        {
            return storePath == null ? 0L : FileUtils.size( fs, new File( storePath, name ) );
        }

        private class TotalSizeVersionVisitor implements LogVersionVisitor
        {
            private long totalSize;

            long getTotalSize()
            {
                return totalSize;
            }

            @Override
            public void visit( File file, long logVersion )
            {
                totalSize += FileUtils.size( fs, file );
            }
        }

        /**
         * Count the total file size, including id files, of {@link StoreFile}s.
         * Missing files will be counted as 0 bytes.
         *
         * @param files the file types to count
         * @return the total size in bytes of the files
         */
        private long sizeOfStoreFiles( StoreFile... files )
        {
            long size = 0L;
            for ( StoreFile file : files )
            {
                // Get size of both store and id file
                size += sizeOf( file.fileName( StoreFileType.STORE ) );
                if ( file.isRecordStore() )
                {
                    size += sizeOf( file.fileName( StoreFileType.ID ) );
                }
            }
            return size;
        }
    }
}
