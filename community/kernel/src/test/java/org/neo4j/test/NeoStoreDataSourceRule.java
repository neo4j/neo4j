/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.test;


import java.io.File;
import java.util.Map;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.TransactionEventHandlers;
import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.scan.InMemoryLabelScanStore;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider;
import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.StartupStatisticsProvider;
import org.neo4j.kernel.impl.factory.CommunityEditionModule;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.TransactionMonitor;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;

import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class NeoStoreDataSourceRule extends ExternalResource
{
    private NeoStoreDataSource theDs;

    public NeoStoreDataSource getDataSource( File storeDir, FileSystemAbstraction fs,
                                             PageCache pageCache, Map<String, String> additionalConfig, KernelHealth kernelHealth )
    {
        if ( theDs != null )
        {
            theDs.stop();
            theDs.shutdown();
        }
        final Config config = new Config( stringMap( additionalConfig ),
                GraphDatabaseSettings.class );

        StoreFactory sf = new StoreFactory( storeDir, config, new DefaultIdGeneratorFactory(), pageCache, fs,
                NullLogProvider.getInstance(), new Monitors() );

        Locks locks = mock( Locks.class );
        when( locks.newClient() ).thenReturn( mock( Locks.Client.class ) );

        theDs = new NeoStoreDataSource( storeDir, config, sf, NullLogProvider.getInstance(),
                mock( JobScheduler.class, RETURNS_MOCKS ), mock( TokenNameLookup.class ),
                dependencyResolverForNoIndexProvider(), mock( PropertyKeyTokenHolder.class ),
                mock( LabelTokenHolder.class ), mock( RelationshipTypeTokenHolder.class ), locks,
                mock( SchemaWriteGuard.class ), mock( TransactionEventHandlers.class ), IndexingService.NO_MONITOR,
                fs, mock( StoreUpgrader.class ), mock( TransactionMonitor.class ), kernelHealth,
                mock( PhysicalLogFile.Monitor.class ), TransactionHeaderInformationFactory.DEFAULT,
                new StartupStatisticsProvider(), mock( NodeManager.class ), null, null,
                CommunityEditionModule.createCommitProcessFactory(), mock( PageCache.class ),
                mock( Monitors.class ), new Tracers( "null", NullLog.getInstance() ) );

        return theDs;
    }

    public NeoStoreDataSource getDataSource( File storeDir, FileSystemAbstraction fs,
                                             PageCache pageCache, Map<String, String> additionalConfig )
    {
        KernelHealth kernelHealth = new KernelHealth( mock( KernelPanicEventGenerator.class ),
                NullLogProvider.getInstance().getLog( KernelHealth.class ) );
        return getDataSource( storeDir, fs, pageCache, additionalConfig, kernelHealth );
    }

    private DependencyResolver dependencyResolverForNoIndexProvider()
    {
        return new DependencyResolver.Adapter()
        {
            private final LabelScanStoreProvider labelScanStoreProvider =
                    new LabelScanStoreProvider( new InMemoryLabelScanStore(), 10 );

            @Override
            public <T> T resolveDependency( Class<T> type, SelectionStrategy selector ) throws IllegalArgumentException
            {
                if ( SchemaIndexProvider.class.isAssignableFrom( type ) )
                {
                    return type.cast( SchemaIndexProvider.NO_INDEX_PROVIDER );
                }
                else if ( LabelScanStoreProvider.class.isAssignableFrom( type ) )
                {
                    return type.cast( labelScanStoreProvider );
                }
                throw new IllegalArgumentException( type.toString() );
            }
        };
    }
}
