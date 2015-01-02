/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.ha.management;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import javax.management.NotCompliantMBeanException;

import org.neo4j.helpers.Service;
import org.neo4j.jmx.impl.ManagementBeanProvider;
import org.neo4j.jmx.impl.ManagementData;
import org.neo4j.jmx.impl.Neo4jMBean;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.ha.BranchedDataPolicy;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.DataSourceRegistrationListener;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.management.BranchedStore;
import org.neo4j.management.BranchedStoreInfo;

@Service.Implementation(ManagementBeanProvider.class)
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
        if ( !isHA( management ) )
        {
            return null;
        }
        return new BranchedStoreImpl( management, true );
    }

    @Override
    protected Neo4jMBean createMBean( ManagementData management )
            throws NotCompliantMBeanException
    {
        if ( !isHA( management ) )
        {
            return null;
        }
        return new BranchedStoreImpl( management );
    }

    private static boolean isHA( ManagementData management )
    {
        return management.getKernelData().graphDatabase() instanceof HighlyAvailableGraphDatabase;
    }

    private static class BranchedStoreImpl extends Neo4jMBean implements
            BranchedStore
    {
        private File storePath;

        protected BranchedStoreImpl( final ManagementData management )
                throws NotCompliantMBeanException
        {
            super( management );

            XaDataSourceManager xadsm = management.getKernelData().graphDatabase().getDependencyResolver()
                    .resolveDependency( XaDataSourceManager.class );
            xadsm.addDataSourceRegistrationListener( new DataSourceRegistrationListener()
            {
                @Override
                public void registeredDataSource( XaDataSource ds )
                {
                    if ( ds instanceof NeoStoreXaDataSource )
                    {
                        storePath = extractStorePath( management );
                    }
                }

                @Override
                public void unregisteredDataSource( XaDataSource ds )
                {
                    if ( ds instanceof NeoStoreXaDataSource )
                    {
                        storePath = null;
                    }
                }
            } );
        }

        protected BranchedStoreImpl( final ManagementData management, boolean isMXBean )
        {
            super( management, isMXBean );

            XaDataSourceManager xadsm = management.getKernelData().graphDatabase().getDependencyResolver()
                    .resolveDependency( XaDataSourceManager.class );
            xadsm.addDataSourceRegistrationListener( new DataSourceRegistrationListener()
            {
                @Override
                public void registeredDataSource( XaDataSource ds )
                {
                    if ( ds instanceof NeoStoreXaDataSource )
                    {
                        storePath = extractStorePath( management );
                    }
                }

                @Override
                public void unregisteredDataSource( XaDataSource ds )
                {
                    if ( ds instanceof NeoStoreXaDataSource )
                    {
                        storePath = null;
                    }
                }
            } );
        }

        private File extractStorePath( ManagementData management )
        {
            NeoStoreXaDataSource nioneodb = management.getKernelData().graphDatabase().getDependencyResolver()
                    .resolveDependency( XaDataSourceManager.class ).getNeoStoreDataSource();
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
            if ( storePath == null )
            {
                return new BranchedStoreInfo[0];
            }

            List<BranchedStoreInfo> toReturn = new LinkedList<BranchedStoreInfo>();
            for ( File branchDirectory : BranchedDataPolicy.getBranchedDataRootDirectory( storePath ).listFiles() )
            {
                if ( !branchDirectory.isDirectory() )
                {
                    continue;
                }
                toReturn.add( parseBranchedStore( branchDirectory ) );
            }
            return toReturn.toArray( new BranchedStoreInfo[]{} );
        }

        private BranchedStoreInfo parseBranchedStore( File branchDirectory )
        {
            File neostoreFile = new File( branchDirectory, NeoStore.DEFAULT_NAME );
            long txId = NeoStore.getTxId( new DefaultFileSystemAbstraction(), neostoreFile );
            long timestamp = Long.parseLong( branchDirectory.getName() );
            return new BranchedStoreInfo( branchDirectory.getName(), txId, timestamp );
        }
    }
}
