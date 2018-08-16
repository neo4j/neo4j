/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.neo4j.helpers.Service;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.jmx.StoreSize;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.api.ExplicitIndexProvider;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogVersionVisitor;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.spi.explicitindex.IndexImplementation;

import static org.neo4j.io.layout.DatabaseFile.COUNTS_STORE_A;
import static org.neo4j.io.layout.DatabaseFile.COUNTS_STORE_B;
import static org.neo4j.io.layout.DatabaseFile.LABEL_TOKEN_NAMES_STORE;
import static org.neo4j.io.layout.DatabaseFile.LABEL_TOKEN_STORE;
import static org.neo4j.io.layout.DatabaseFile.NODE_LABEL_STORE;
import static org.neo4j.io.layout.DatabaseFile.NODE_STORE;
import static org.neo4j.io.layout.DatabaseFile.PROPERTY_ARRAY_STORE;
import static org.neo4j.io.layout.DatabaseFile.PROPERTY_KEY_TOKEN_NAMES_STORE;
import static org.neo4j.io.layout.DatabaseFile.PROPERTY_KEY_TOKEN_STORE;
import static org.neo4j.io.layout.DatabaseFile.PROPERTY_STORE;
import static org.neo4j.io.layout.DatabaseFile.PROPERTY_STRING_STORE;
import static org.neo4j.io.layout.DatabaseFile.RELATIONSHIP_GROUP_STORE;
import static org.neo4j.io.layout.DatabaseFile.RELATIONSHIP_STORE;
import static org.neo4j.io.layout.DatabaseFile.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE;
import static org.neo4j.io.layout.DatabaseFile.RELATIONSHIP_TYPE_TOKEN_STORE;
import static org.neo4j.io.layout.DatabaseFile.SCHEMA_STORE;

@Service.Implementation( ManagementBeanProvider.class )
public final class StoreSizeBean extends ManagementBeanProvider
{
    @SuppressWarnings( "WeakerAccess" ) // Bean needs public constructor
    public StoreSizeBean()
    {
        super( StoreSize.class );
    }

    @Override
    protected Neo4jMBean createMBean( ManagementData management )
    {
        return new StoreSizeImpl( management, false );
    }

    @Override
    protected Neo4jMBean createMXBean( ManagementData management )
    {
        return new StoreSizeImpl( management, true );
    }

    static class StoreSizeImpl extends Neo4jMBean implements StoreSize
    {
        private final FileSystemAbstraction fs;

        private LogFiles logFiles;
        private ExplicitIndexProvider explicitIndexProviderLookup;
        private IndexProviderMap indexProviderMap;
        private LabelScanStore labelScanStore;
        private DatabaseLayout databaseLayout;

        StoreSizeImpl( ManagementData management, boolean isMXBean )
        {
            super( management, isMXBean );

            fs = management.getKernelData().getFilesystemAbstraction();

            DataSourceManager dataSourceManager = management.getKernelData().getDataSourceManager();
            dataSourceManager.addListener( new DataSourceManager.Listener()
            {

                @Override
                public void registered( NeoStoreDataSource ds )
                {
                    logFiles = resolveDependency( ds, LogFiles.class );
                    explicitIndexProviderLookup = resolveDependency( ds, ExplicitIndexProvider.class );
                    indexProviderMap = resolveDependency( ds, IndexProviderMap.class );
                    labelScanStore = resolveDependency( ds, LabelScanStore.class );
                    databaseLayout = ds.getDatabaseLayout();
                }

                private <T> T resolveDependency( NeoStoreDataSource ds, Class<T> clazz )
                {
                    return ds.getDependencyResolver().resolveDependency( clazz );
                }

                @Override
                public void unregistered( NeoStoreDataSource ds )
                {
                    logFiles = null;
                    explicitIndexProviderLookup = null;
                    indexProviderMap = null;
                    labelScanStore = null;
                }
            } );
        }

        @Override
        public long getTransactionLogsSize()
        {
            TotalSizeVersionVisitor logVersionVisitor = new TotalSizeVersionVisitor( fs );

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
            return sizeOfStoreFiles( RELATIONSHIP_STORE, RELATIONSHIP_GROUP_STORE,
                    RELATIONSHIP_TYPE_TOKEN_STORE,
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
            return sizeOfStoreFiles( COUNTS_STORE_A, COUNTS_STORE_B );
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
            for ( IndexImplementation index : explicitIndexProviderLookup.allIndexProviders() )
            {
                size += FileUtils.size( fs, index.getIndexImplementationDirectory( databaseLayout ) );
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
            return databaseLayout == null ? 0L : FileUtils.size( fs, databaseLayout.databaseDirectory() );
        }

        private long sizeOf( File file )
        {
            return FileUtils.size( fs, file );
        }

        private static class TotalSizeVersionVisitor implements LogVersionVisitor
        {
            private final FileSystemAbstraction fs;
            private long totalSize;

            TotalSizeVersionVisitor( FileSystemAbstraction fs )
            {
                this.fs = fs;
            }

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
         * Count the total file size, including id files, of {@link DatabaseFile}s.
         * Missing files will be counted as 0 bytes.
         *
         * @param databaseFiles the store types to count
         * @return the total size in bytes of the files
         */
        private long sizeOfStoreFiles( DatabaseFile... databaseFiles )
        {
            long size = 0L;
            for ( DatabaseFile store : databaseFiles )
            {
                size += databaseLayout.file( store ).mapToLong( this::sizeOf ).sum();
                size += databaseLayout.idFile( store ).map( this::sizeOf ).orElse( 0L );
            }
            return size;
        }
    }
}
