/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import javax.management.NotCompliantMBeanException;

import org.neo4j.helpers.Service;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.jmx.StoreFile;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;

@Service.Implementation( ManagementBeanProvider.class )
@Deprecated
public final class StoreFileBean extends ManagementBeanProvider
{
    @SuppressWarnings( "WeakerAccess" ) // Bean needs public constructor
    public StoreFileBean()
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
        private File databaseDirectory;
        private LogFiles logFiles;
        private FileSystemAbstraction fs;
        private DatabaseLayout databaseLayout;

        StoreFileImpl( ManagementData management ) throws NotCompliantMBeanException
        {
            super( management );

            fs = management.getKernelData().getFilesystemAbstraction();

            DataSourceManager dataSourceManager = management.getKernelData().getDataSourceManager();
            dataSourceManager.addListener( new DataSourceManager.Listener()
            {
                @Override
                public void registered( NeoStoreDataSource ds )
                {
                    logFiles = resolveDependency( ds, LogFiles.class );
                    databaseLayout = ds.getDatabaseLayout();
                    databaseDirectory = resolveDatabaseDirectory();
                }

                private <T> T resolveDependency( NeoStoreDataSource ds, Class<T> clazz )
                {
                    return ds.getDependencyResolver().resolveDependency( clazz );
                }

                @Override
                public void unregistered( NeoStoreDataSource ds )
                {
                    logFiles = null;
                    databaseDirectory = null;
                    databaseLayout = null;
                }

                private File resolveDatabaseDirectory()
                {
                    return databaseLayout.databaseDirectory();
                }
            } );
        }

        @Override
        public long getTotalStoreSize()
        {
            return databaseDirectory == null ? 0 : FileUtils.size( fs, databaseDirectory );
        }

        @Override
        public long getLogicalLogSize()
        {
            return logFiles == null ? 0 : FileUtils.size( fs, logFiles.getHighestLogFile() );
        }

        @Override
        public long getArrayStoreSize()
        {
            return sizeOf( databaseLayout.propertyArrayStore() );
        }

        @Override
        public long getNodeStoreSize()
        {
            return sizeOf( databaseLayout.nodeStore() );
        }

        @Override
        public long getPropertyStoreSize()
        {
            return sizeOf( databaseLayout.propertyStore() );
        }

        @Override
        public long getRelationshipStoreSize()
        {
            return sizeOf( databaseLayout.relationshipStore() );
        }

        @Override
        public long getStringStoreSize()
        {
            return sizeOf( databaseLayout.propertyStringStore() );
        }

        private long sizeOf( File file )
        {
            return databaseDirectory == null ? 0 : FileUtils.size( fs, file );
        }
    }
}
