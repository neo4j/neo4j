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

import java.io.File;
import java.io.IOException;
import javax.management.NotCompliantMBeanException;

import org.neo4j.helpers.Service;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.jmx.StoreFile;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.api.LegacyIndexProviderLookup;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.transaction.log.LogFile;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.spi.legacyindex.IndexImplementation;

import static org.neo4j.kernel.impl.store.StoreFactory.NODE_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.PROPERTY_ARRAYS_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.PROPERTY_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.PROPERTY_STRINGS_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.RELATIONSHIP_STORE_NAME;

@Service.Implementation( ManagementBeanProvider.class )
public final class StoreFileBean extends ManagementBeanProvider
{
    StoreFileBean()
    {
        super( StoreFile.class );
    }

    @Override
    protected Neo4jMBean createMBean( ManagementData management ) throws NotCompliantMBeanException
    {
        return new StoreFileImpl( management );
    }

    static class StoreFileImpl extends Neo4jMBean implements StoreFile
    {
        private static final String NODE_STORE = MetaDataStore.DEFAULT_NAME + NODE_STORE_NAME;
        private static final String RELATIONSHIP_STORE = MetaDataStore.DEFAULT_NAME +  RELATIONSHIP_STORE_NAME;
        private static final String PROPERTY_STORE = MetaDataStore.DEFAULT_NAME + PROPERTY_STORE_NAME;
        private static final String ARRAY_STORE = MetaDataStore.DEFAULT_NAME + PROPERTY_ARRAYS_STORE_NAME;
        private static final String STRING_STORE = MetaDataStore.DEFAULT_NAME + PROPERTY_STRINGS_STORE_NAME;

        private File storePath;
        private LogFile logFile;
        private PhysicalLogFiles physicalLogFiles;
        private FileSystemAbstraction fs;
        private LegacyIndexProviderLookup legacyIndexProviderLookup;
        private SchemaIndexProvider schemaIndexProvider;
        private LabelScanStore labelScanStore;

        StoreFileImpl( ManagementData management ) throws NotCompliantMBeanException
        {
            super( management );

            fs = management.getKernelData().getFilesystemAbstraction();

            DataSourceManager dataSourceManager = management.resolveDependency( DataSourceManager.class );
            dataSourceManager.addListener( new DataSourceManager.Listener()
            {
                @Override
                public void registered( NeoStoreDataSource ds )
                {
                    logFile = resolveDependency( ds, LogFile.class );
                    physicalLogFiles = resolveDependency( ds, PhysicalLogFiles.class );
                    legacyIndexProviderLookup = resolveDependency( ds, LegacyIndexProviderLookup.class );
                    schemaIndexProvider = resolveDependency( ds, SchemaIndexProvider.class );
                    labelScanStore = resolveDependency( ds, LabelScanStore.class );

                    storePath = resolvePath( ds );
                }

                private <T> T resolveDependency( NeoStoreDataSource ds, Class<T> clazz )
                {
                    return ds.getDependencyResolver().resolveDependency( clazz );
                }

                @Override
                public void unregistered( NeoStoreDataSource ds )
                {
                    logFile = null;
                    physicalLogFiles = null;
                    storePath = null;
                }

                private File resolvePath( NeoStoreDataSource ds )
                {
                    try
                    {
                        return ds.getStoreDir().getCanonicalFile().getAbsoluteFile();
                    }
                    catch ( IOException e )
                    {
                        return ds.getStoreDir().getAbsoluteFile();
                    }
                }
            } );
        }

        @Override
        public long getTotalStoreSize()
        {
            return storePath == null ? 0 : FileUtils.size( fs, storePath );
        }

        @Override
        public long getLogicalLogSize()
        {
            return logFile == null ? 0 : FileUtils.size( fs, logFile.currentLogFile() );
        }

        private long sizeOf( String name )
        {
            return storePath == null ? 0 : FileUtils.size( fs, new File( storePath, name ) );
        }

        @Override
        public long getArrayStoreSize()
        {
            return sizeOf( ARRAY_STORE );
        }

        @Override
        public long getAllLogicalLogsSize()
        {
            TotalSizeVersionVisitor logVersionVisitor = new TotalSizeVersionVisitor( fs );

            physicalLogFiles.accept( logVersionVisitor );

            return logVersionVisitor.getTotalSize();
        }

        @Override
        public long getIndexStoreSize()
        {
            long size = 0L;

            // Add legacy indices
            for ( IndexImplementation index : legacyIndexProviderLookup.all() )
            {
                size += FileUtils.size( fs, index.getIndexImplementationDirectory( storePath ) );
            }

            // Add schema index
            size += FileUtils.size( fs, schemaIndexProvider.getSchemaIndexStoreDirectory( storePath ) );

            // Add label index
            size += FileUtils.size( fs, labelScanStore.getLabelScanStoreFile() );

            return size;
        }

        @Override
        public long getNodeStoreSize()
        {
            return sizeOf( NODE_STORE );
        }

        @Override
        public long getPropertyStoreSize()
        {
            return sizeOf( PROPERTY_STORE );
        }

        @Override
        public long getRelationshipStoreSize()
        {
            return sizeOf( RELATIONSHIP_STORE );
        }

        @Override
        public long getStringStoreSize()
        {
            return sizeOf( STRING_STORE );
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
    }
}
