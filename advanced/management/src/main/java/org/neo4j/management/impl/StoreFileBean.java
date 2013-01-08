/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.management.impl;

import java.io.File;
import java.io.IOException;
import javax.management.NotCompliantMBeanException;
import org.neo4j.helpers.Service;
import org.neo4j.jmx.impl.ManagementBeanProvider;
import org.neo4j.jmx.impl.ManagementData;
import org.neo4j.jmx.impl.Neo4jMBean;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.management.StoreFile;

@Service.Implementation( ManagementBeanProvider.class )
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
        private static final String LOGICAL_LOG1 = "nioneo_logical.log.1";
        private static final String LOGICAL_LOG2 = "nioneo_logical.log.2";
        private final File storePath;

        StoreFileImpl( ManagementData management ) throws NotCompliantMBeanException
        {
            super( management );
            NeoStoreXaDataSource nioneodb = management.getKernelData().graphDatabase().getXaDataSourceManager().getNeoStoreDataSource();
            File path;
            try
            {
                path = new File( nioneodb.getStoreDir() ).getCanonicalFile().getAbsoluteFile();
            }
            catch ( IOException e )
            {
                path = new File( nioneodb.getStoreDir() ).getAbsoluteFile();
            }
            this.storePath = path;
        }

        public long getTotalStoreSize()
        {
            return sizeOf( storePath );
        }

        public long getLogicalLogSize()
        {
            File logicalLog = new File( storePath, LOGICAL_LOG1 );
            if ( !logicalLog.isFile() )
            {
                logicalLog = new File( storePath, LOGICAL_LOG2 );
            }
            return sizeOf( logicalLog );
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
                for ( File child : file.listFiles() )
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

        public long getArrayStoreSize()
        {
            return sizeOf( ARRAY_STORE );
        }

        public long getNodeStoreSize()
        {
            return sizeOf( NODE_STORE );
        }

        public long getPropertyStoreSize()
        {
            return sizeOf( PROPERTY_STORE );
        }

        public long getRelationshipStoreSize()
        {
            return sizeOf( RELATIONSHIP_STORE );
        }

        public long getStringStoreSize()
        {
            return sizeOf( STRING_STORE );
        }
    }
}
