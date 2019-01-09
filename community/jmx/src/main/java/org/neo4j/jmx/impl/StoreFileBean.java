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

import java.io.File;
import java.time.Clock;

import org.neo4j.helpers.Service;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.jmx.StoreFile;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;

import static org.neo4j.jmx.impl.StoreSizeBean.resolveStorePath;
import static org.neo4j.jmx.impl.ThrottlingBeanSnapshotProxy.newThrottlingBeanSnapshotProxy;
import static org.neo4j.kernel.impl.store.StoreFactory.NODE_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.PROPERTY_ARRAYS_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.PROPERTY_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.PROPERTY_STRINGS_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.RELATIONSHIP_STORE_NAME;

@Service.Implementation( ManagementBeanProvider.class )
public final class StoreFileBean extends ManagementBeanProvider
{
    private static final long UPDATE_INTERVAL = 60000;
    private static final StoreFile NO_STORE_FILE = new StoreFile()
    {
        @Override
        public long getLogicalLogSize()
        {
            return 0;
        }

        @Override
        public long getTotalStoreSize()
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
    };

    public StoreFileBean()
    {
        super( StoreFile.class );
    }

    @Override
    protected Neo4jMBean createMBean( ManagementData management )
    {
        final StoreFileMBean bean = new StoreFileMBean( management );
        final DataSourceManager dataSourceManager = management.resolveDependency( DataSourceManager.class );
        dataSourceManager.addListener( bean );
        return bean;
    }

    static class StoreFileMBean extends Neo4jMBean implements StoreFile, DataSourceManager.Listener
    {
        private final FileSystemAbstraction fs;
        private final File storePath;
        private volatile StoreFile delegate = NO_STORE_FILE;

        StoreFileMBean( ManagementData management )
        {
            super( management, false );
            this.fs = management.getKernelData().getFilesystemAbstraction();
            this.storePath = resolveStorePath( management );
        }

        @Override
        public void registered( NeoStoreDataSource ds )
        {
            final LogFiles logFiles = ds.getDependencyResolver().resolveDependency( LogFiles.class );
            final StoreFileImpl dataProvider = new StoreFileImpl( fs, storePath, logFiles );
            this.delegate = newThrottlingBeanSnapshotProxy( StoreFile.class, dataProvider, UPDATE_INTERVAL, Clock.systemUTC() );
        }

        @Override
        public void unregistered( NeoStoreDataSource ds )
        {
            this.delegate = NO_STORE_FILE;
        }

        @Override
        public long getLogicalLogSize()
        {
            return delegate.getLogicalLogSize();
        }

        @Override
        public long getTotalStoreSize()
        {
            return delegate.getTotalStoreSize();
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
    }

    static class StoreFileImpl implements StoreFile
    {
        private static final String NODE_STORE = MetaDataStore.DEFAULT_NAME + NODE_STORE_NAME;
        private static final String RELATIONSHIP_STORE = MetaDataStore.DEFAULT_NAME +  RELATIONSHIP_STORE_NAME;
        private static final String PROPERTY_STORE = MetaDataStore.DEFAULT_NAME + PROPERTY_STORE_NAME;
        private static final String ARRAY_STORE = MetaDataStore.DEFAULT_NAME + PROPERTY_ARRAYS_STORE_NAME;
        private static final String STRING_STORE = MetaDataStore.DEFAULT_NAME + PROPERTY_STRINGS_STORE_NAME;

        private final File storePath;
        private final LogFiles logFiles;
        private final FileSystemAbstraction fs;

        StoreFileImpl( FileSystemAbstraction fs, File storePath, LogFiles logFiles )
        {
            this.fs = fs;
            this.storePath = storePath;
            this.logFiles = logFiles;
        }

        @Override
        public long getTotalStoreSize()
        {
            return storePath == null ? 0 : FileUtils.size( fs, storePath );
        }

        @Override
        public long getLogicalLogSize()
        {
            return logFiles == null ? 0 : FileUtils.size( fs, logFiles.getHighestLogFile() );
        }

        @Override
        public long getArrayStoreSize()
        {
            return sizeOf( ARRAY_STORE );
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

        private long sizeOf( String name )
        {
            return storePath == null ? 0 : FileUtils.size( fs, new File( storePath, name ) );
        }
    }
}
