/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel.ha.management;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import javax.management.NotCompliantMBeanException;

import org.neo4j.helpers.Service;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.jmx.impl.ManagementBeanProvider;
import org.neo4j.jmx.impl.ManagementData;
import org.neo4j.jmx.impl.Neo4jMBean;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.MetaDataStore.Position;
import org.neo4j.management.BranchedStore;
import org.neo4j.management.BranchedStoreInfo;

import static org.neo4j.com.storecopy.StoreUtil.getBranchedDataRootDirectory;

@Service.Implementation( ManagementBeanProvider.class )
public final class BranchedStoreBean extends ManagementBeanProvider
{
    @SuppressWarnings( "WeakerAccess" ) // Bean needs public constructor
    public BranchedStoreBean()
    {
        super( BranchedStore.class );
    }

    @Override
    protected Neo4jMBean createMXBean( ManagementData management )
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

    private static class BranchedStoreImpl extends Neo4jMBean implements BranchedStore
    {
        private final FileSystemAbstraction fileSystem;
        private final File storePath;
        private final PageCache pageCache;

        BranchedStoreImpl( final ManagementData management ) throws NotCompliantMBeanException
        {
            super( management );
            fileSystem = getFilesystem( management );
            storePath = getStorePath( management );
            pageCache = getPageCache( management );
        }

        BranchedStoreImpl( final ManagementData management, boolean isMXBean )
        {
            super( management, isMXBean );
            fileSystem = getFilesystem( management );
            storePath = getStorePath( management );
            pageCache = getPageCache( management );
        }

        @Override
        public BranchedStoreInfo[] getBranchedStores()
        {
            if ( storePath == null )
            {
                return new BranchedStoreInfo[0];
            }

            List<BranchedStoreInfo> toReturn = new LinkedList<>();
            for ( File branchDirectory : fileSystem.listFiles( getBranchedDataRootDirectory( storePath ) ) )
            {
                if ( !branchDirectory.isDirectory() )
                {
                    continue;
                }
                toReturn.add( parseBranchedStore( branchDirectory ) );
            }
            return toReturn.toArray( new BranchedStoreInfo[toReturn.size()] );
        }

        private BranchedStoreInfo parseBranchedStore( File branchDirectory )
        {
            try
            {
                final File neoStoreFile = new File( branchDirectory, MetaDataStore.DEFAULT_NAME );
                final long txId = MetaDataStore.getRecord( pageCache, neoStoreFile, Position.LAST_TRANSACTION_ID );
                final long timestamp = Long.parseLong( branchDirectory.getName() );
                final long branchedStoreSize = FileUtils.size( fileSystem, branchDirectory );

                return new BranchedStoreInfo( branchDirectory.getName(), txId, timestamp, branchedStoreSize );
            }
            catch ( IOException e )
            {
                throw new IllegalStateException( "Cannot read branched neostore", e );
            }
        }

        private PageCache getPageCache( ManagementData management )
        {
            return management.getKernelData().getPageCache();
        }

        private FileSystemAbstraction getFilesystem( ManagementData management )
        {
            return management.getKernelData().getFilesystemAbstraction();
        }

        private File getStorePath( ManagementData management )
        {
            return management.getKernelData().getStoreDir();
        }
    }
}
