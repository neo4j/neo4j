/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import javax.management.NotCompliantMBeanException;
import org.neo4j.helpers.Service;
import org.neo4j.jmx.impl.ManagementBeanProvider;
import org.neo4j.jmx.impl.ManagementData;
import org.neo4j.jmx.impl.Neo4jMBean;
import org.neo4j.kernel.HighlyAvailableGraphDatabase.BranchedDataPolicy;
import org.neo4j.kernel.ha.AbstractHAGraphDatabase;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.management.BranchedStore;
import org.neo4j.management.BranchedStoreInfo;

@Service.Implementation( ManagementBeanProvider.class )
public final class BranchedStoreBean extends ManagementBeanProvider
{
    public BranchedStoreBean()
    {
        super( BranchedStore.class );
    }

    @Override
    protected Neo4jMBean createMXBean( ManagementData management )
            throws NotCompliantMBeanException
    {
        if ( !isHA( management ) ) return null;
        return new BranchedStoreImpl( management, true );
    }

    @Override
    protected Neo4jMBean createMBean( ManagementData management )
            throws NotCompliantMBeanException
    {
        if ( !isHA( management ) ) return null;
        return new BranchedStoreImpl( management );
    }

    private static boolean isHA( ManagementData management )
    {
        return management.getKernelData().graphDatabase() instanceof AbstractHAGraphDatabase;
    }

    private static class BranchedStoreImpl extends Neo4jMBean implements
            BranchedStore
    {
        private final File storePath;
        private static final FilenameFilter branchedDataDirectoryFilenameFilter = new BranchedDataDirectoryFilenameFilter();

        protected BranchedStoreImpl( ManagementData management )
                                                                throws NotCompliantMBeanException
        {
            super( management );
            this.storePath = extractStorePath( management );
        }

        protected BranchedStoreImpl( ManagementData management, boolean isMXBean )
        {
            super( management, isMXBean );
            this.storePath = extractStorePath( management );
        }

        private File extractStorePath( ManagementData management )
        {
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
            return path;
        }

        @Override
        public BranchedStoreInfo[] getBranchedStores()
        {
            List<BranchedStoreInfo> toReturn = new LinkedList<BranchedStoreInfo>();
            for ( String filename : storePath.list( branchedDataDirectoryFilenameFilter ) )
            {
                toReturn.add( parseBranchedStore( filename ) );
            }
            return toReturn.toArray( new BranchedStoreInfo[] {} );
        }

        private BranchedStoreInfo parseBranchedStore(
                String branchedStoreDirName )
        {
            File theDir = new File( storePath, branchedStoreDirName );
            File theNeostoreFile = new File( theDir, NeoStore.DEFAULT_NAME );

            String timestampFromFilename = branchedStoreDirName.substring( BranchedDataPolicy.BRANCH_PREFIX.length() );
            long timestamp = Long.parseLong( timestampFromFilename );

            long txId = NeoStore.getTxId( CommonFactories.defaultFileSystemAbstraction(), theNeostoreFile.getAbsolutePath() );
            return new BranchedStoreInfo( branchedStoreDirName, txId, timestamp );
        }

        private static final class BranchedDataDirectoryFilenameFilter
                implements FilenameFilter
        {
            @Override
            public boolean accept( File dir, String name )
            {
                return new File( dir, name ).isDirectory()
                       && name.startsWith( BranchedDataPolicy.BRANCH_PREFIX );
            }
        }
    }
}
