/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.jmx.impl;

import org.apache.commons.lang3.mutable.MutableLong;

import java.io.File;
import java.io.IOException;
import javax.management.NotCompliantMBeanException;

import org.neo4j.helpers.Service;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.jmx.StoreSize;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.api.ExplicitIndexProviderLookup;
import org.neo4j.kernel.impl.api.index.SchemaIndexProviderMap;
import org.neo4j.kernel.impl.store.StoreFile;
import org.neo4j.kernel.impl.storemigration.StoreFileType;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.spi.explicitindex.IndexImplementation;

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
    @SuppressWarnings( "WeakerAccess" ) // Bean needs public constructor
    public StoreSizeBean()
    {
        super( StoreSize.class );
    }

    @Override
    protected Neo4jMBean createMBean( ManagementData management ) throws NotCompliantMBeanException
    {
        return new StoreSizeImpl( management, false );
    }

    @Override
    protected Neo4jMBean createMXBean( ManagementData management ) throws NotCompliantMBeanException
    {
        return new StoreSizeImpl( management, true );
    }

    static class StoreSizeImpl extends Neo4jMBean implements StoreSize
    {
        private final FileSystemAbstraction fs;
        private final File storePath;

        private PhysicalLogFiles physicalLogFiles;
        private ExplicitIndexProviderLookup explicitIndexProviderLookup;
        private SchemaIndexProviderMap schemaIndexProviderMap;
        private LabelScanStore labelScanStore;

        StoreSizeImpl( ManagementData management, boolean isMXBean ) throws NotCompliantMBeanException
        {
            super( management, isMXBean );

            fs = management.getKernelData().getFilesystemAbstraction();
            storePath = resolveStorePath( management );

            DataSourceManager dataSourceManager = management.resolveDependency( DataSourceManager.class );
            dataSourceManager.addListener( new DataSourceManager.Listener()
            {
                @Override
                public void registered( NeoStoreDataSource ds )
                {
                    physicalLogFiles = resolveDependency( ds, PhysicalLogFiles.class );
                    explicitIndexProviderLookup = resolveDependency( ds, ExplicitIndexProviderLookup.class );
                    schemaIndexProviderMap = resolveDependency( ds, SchemaIndexProviderMap.class );
                    labelScanStore = resolveDependency( ds, LabelScanStore.class );
                }

                private <T> T resolveDependency( NeoStoreDataSource ds, Class<T> clazz )
                {
                    return ds.getDependencyResolver().resolveDependency( clazz );
                }

                @Override
                public void unregistered( NeoStoreDataSource ds )
                {
                    physicalLogFiles = null;
                    explicitIndexProviderLookup = null;
                    schemaIndexProviderMap = null;
                    labelScanStore = null;
                }
            } );
        }

        @Override
        public long getTransactionLogsSize()
        {
            TotalSizeVersionVisitor logVersionVisitor = new TotalSizeVersionVisitor( fs );

            physicalLogFiles.accept( logVersionVisitor );

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
            schemaIndexProviderMap.accept( provider ->
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

        private static class TotalSizeVersionVisitor implements PhysicalLogFiles.LogVersionVisitor
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

        private File resolveStorePath( ManagementData management )
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
    }
}
