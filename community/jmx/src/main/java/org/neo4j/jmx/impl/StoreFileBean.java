/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import org.neo4j.jmx.StoreFile;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.transaction.log.LogFile;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;

@Service.Implementation(ManagementBeanProvider.class)
public final class StoreFileBean extends ManagementBeanProvider
{
    public StoreFileBean()
    {
        super( StoreFile.class );
    }

    @Override
    protected Neo4jMBean createMBean( ManagementData management ) throws NotCompliantMBeanException
    {
        return new StoreFileImpl( management );
    }

    private static class StoreFileImpl extends Neo4jMBean implements StoreFile
    {
        private static final String NODE_STORE = "neostore.nodestore.db";
        private static final String RELATIONSHIP_STORE = "neostore.relationshipstore.db";
        private static final String PROPERTY_STORE = "neostore.propertystore.db";
        private static final String ARRAY_STORE = "neostore.propertystore.db.arrays";
        private static final String STRING_STORE = "neostore.propertystore.db.strings";
        private File storePath;
        private LogFile logFile;

        StoreFileImpl( ManagementData management ) throws NotCompliantMBeanException
        {
            super( management );

            DataSourceManager dataSourceManager = management.resolveDependency( DataSourceManager.class );
            dataSourceManager.addListener( new DataSourceManager.Listener()
            {
                @Override
                public void registered( NeoStoreDataSource ds )
                {
                    logFile = ds.getDependencyResolver().resolveDependency( LogFile.class );
                    storePath = resolvePath( ds );
                }

                @Override
                public void unregistered( NeoStoreDataSource ds )
                {
                    logFile = null;
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
            return storePath == null ? 0 : sizeOf( storePath );
        }

        @Override
        public long getLogicalLogSize()
        {
            return logFile == null ? 0 : sizeOf( logFile.currentLogFile() );
        }

        private static long sizeOf( File file )
        {
            if ( file.isFile() )
            {
                return file.length();
            }
            else if ( file.isDirectory() )
            {
                long size = 0;
                File[] files = file.listFiles();
                if ( files == null )
                {
                    return 0;
                }
                for ( File child : files )
                {
                    size += sizeOf( child );
                }
                return size;
            }
            return 0;
        }

        private long sizeOf( String name )
        {
            return sizeOf( new File( storePath, name ) );
        }

        @Override
        public long getArrayStoreSize()
        {
            if ( storePath == null )
            {
                return 0;
            }

            return sizeOf( ARRAY_STORE );
        }

        @Override
        public long getNodeStoreSize()
        {
            if ( storePath == null )
            {
                return 0;
            }

            return sizeOf( NODE_STORE );
        }

        @Override
        public long getPropertyStoreSize()
        {
            if ( storePath == null )
            {
                return 0;
            }

            return sizeOf( PROPERTY_STORE );
        }

        @Override
        public long getRelationshipStoreSize()
        {
            if ( storePath == null )
            {
                return 0;
            }

            return sizeOf( RELATIONSHIP_STORE );
        }

        @Override
        public long getStringStoreSize()
        {
            if ( storePath == null )
            {
                return 0;
            }

            return sizeOf( STRING_STORE );
        }
    }
}
