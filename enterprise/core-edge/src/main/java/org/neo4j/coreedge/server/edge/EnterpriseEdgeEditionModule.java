/*
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
package org.neo4j.coreedge.server.edge;

import java.io.File;
import java.util.function.Supplier;

import org.neo4j.coreedge.catchup.storecopy.LocalDatabase;
import org.neo4j.coreedge.catchup.storecopy.StoreFiles;
import org.neo4j.coreedge.catchup.storecopy.edge.CopiedStoreRecovery;
import org.neo4j.coreedge.catchup.storecopy.edge.EdgeToCoreClient;
import org.neo4j.coreedge.catchup.storecopy.edge.StoreCopyClient;
import org.neo4j.coreedge.catchup.storecopy.edge.StoreFetcher;
import org.neo4j.coreedge.catchup.tx.edge.ApplyPulledTransactions;
import org.neo4j.coreedge.catchup.tx.edge.TransactionApplier;
import org.neo4j.coreedge.catchup.tx.edge.TransactionLogCatchUpFactory;
import org.neo4j.coreedge.catchup.tx.edge.TxPollingClient;
import org.neo4j.coreedge.catchup.tx.edge.TxPullClient;
import org.neo4j.coreedge.discovery.DiscoveryServiceFactory;
import org.neo4j.coreedge.discovery.EdgeDiscoveryService;
import org.neo4j.coreedge.server.Expiration;
import org.neo4j.coreedge.server.ExpiryScheduler;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.DatabaseAvailability;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.KernelData;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.Version;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.ReadOnlyTransactionCommitProcess;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.core.DelegatingLabelTokenHolder;
import org.neo4j.kernel.impl.core.DelegatingPropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.DelegatingRelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.ReadOnlyTokenCreator;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.storageengine.StorageEngine;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleListener;
import org.neo4j.kernel.lifecycle.LifecycleStatus;
import org.neo4j.logging.LogProvider;
import org.neo4j.udc.UsageData;
import org.neo4j.udc.UsageDataKeys;

import static org.neo4j.helpers.Clock.SYSTEM_CLOCK;
import static org.neo4j.kernel.impl.factory.CommunityEditionModule.createLockManager;

/**
 * This implementation of {@link org.neo4j.kernel.impl.factory.EditionModule} creates the implementations of services
 * that are specific to the Enterprise Edge edition that provides an edge cluster.
 */
public class EnterpriseEdgeEditionModule extends EditionModule
{
    public EnterpriseEdgeEditionModule( final PlatformModule platformModule,
                                        DiscoveryServiceFactory discoveryServiceFactory )
    {
        org.neo4j.kernel.impl.util.Dependencies dependencies = platformModule.dependencies;
        Config config = platformModule.config;
        LogService logging = platformModule.logging;
        FileSystemAbstraction fileSystem = platformModule.fileSystem;
        PageCache pageCache = platformModule.pageCache;
        File storeDir = platformModule.storeDir;
        LifeSupport life = platformModule.life;

        GraphDatabaseFacade graphDatabaseFacade = platformModule.graphDatabaseFacade;

        lockManager = dependencies.satisfyDependency( createLockManager( config, logging ) );

        idGeneratorFactory = dependencies.satisfyDependency( new DefaultIdGeneratorFactory( fileSystem ) );

        propertyKeyTokenHolder = life.add( dependencies.satisfyDependency(
                new DelegatingPropertyKeyTokenHolder( new ReadOnlyTokenCreator() ) ) );
        labelTokenHolder = life.add( dependencies.satisfyDependency(
                new DelegatingLabelTokenHolder( new ReadOnlyTokenCreator() ) ) );
        relationshipTypeTokenHolder = life.add( dependencies.satisfyDependency(
                new DelegatingRelationshipTypeTokenHolder( new ReadOnlyTokenCreator() ) ) );

        life.add( dependencies.satisfyDependency(
                new DefaultKernelData( fileSystem, pageCache, storeDir, config, graphDatabaseFacade ) ) );

        headerInformationFactory = TransactionHeaderInformationFactory.DEFAULT;

        schemaWriteGuard = new SchemaWriteGuard()
        {
            @Override
            public void assertSchemaWritesAllowed() throws InvalidTransactionTypeKernelException
            {
            }
        };

        transactionStartTimeout = config.get( GraphDatabaseSettings.transaction_start_timeout );

        constraintSemantics = new StandardConstraintSemantics();

        registerRecovery( config.get( GraphDatabaseFacadeFactory.Configuration.editionName ), life, dependencies );

        publishEditionInfo( dependencies.resolveDependency( UsageData.class ) );
        commitProcessFactory = readOnly();

        LogProvider logProvider = platformModule.logging.getInternalLogProvider();

        EdgeDiscoveryService discoveryService = discoveryServiceFactory.edgeDiscoveryService( config );
        life.add(dependencies.satisfyDependency( discoveryService ));

        Supplier<TransactionApplier> transactionApplierSupplier =
                () -> new TransactionApplier( platformModule.dependencies );

        ExpiryScheduler expiryScheduler = new ExpiryScheduler( platformModule.jobScheduler );
        Expiration expiration = new Expiration( SYSTEM_CLOCK );

        EdgeToCoreClient.ChannelInitializer channelInitializer = new EdgeToCoreClient.ChannelInitializer( logProvider );
        EdgeToCoreClient edgeToCoreClient = life.add( new EdgeToCoreClient( logProvider, expiryScheduler, expiration,
                channelInitializer ) );
        channelInitializer.setOwner( edgeToCoreClient );

        Supplier<TransactionIdStore> transactionIdStoreSupplier =
                () -> platformModule.dependencies.resolveDependency( TransactionIdStore.class ) ;

        ApplyPulledTransactions applyPulledTransactions =
                new ApplyPulledTransactions( logProvider, transactionApplierSupplier, transactionIdStoreSupplier );

        TxPollingClient txPollingClient = life.add(
                new TxPollingClient( platformModule.jobScheduler, config.get( HaSettings.pull_interval ),
                        platformModule.dependencies.provideDependency( TransactionIdStore.class ), edgeToCoreClient,
                        applyPulledTransactions, discoveryService ) );

        StoreFetcher storeFetcher = new StoreFetcher( platformModule.logging.getInternalLogProvider(),
                new DefaultFileSystemAbstraction(), platformModule.pageCache,
                new StoreCopyClient( edgeToCoreClient ), new TxPullClient( edgeToCoreClient ),
                new TransactionLogCatchUpFactory() );

        life.add( new EdgeServerStartupProcess( storeFetcher,
                new LocalDatabase( platformModule.storeDir,
                        new CopiedStoreRecovery( config, platformModule.kernelExtensions.listFactories(),
                                platformModule.pageCache ),
                        new StoreFiles( new DefaultFileSystemAbstraction() ),
                        dependencies.provideDependency( NeoStoreDataSource.class ), platformModule.dependencies
                        .provideDependency( TransactionIdStore.class ) ),
                txPollingClient, discoveryService, platformModule.dataSourceManager ) );
    }

    private void publishEditionInfo( UsageData sysInfo )
    {
        sysInfo.set( UsageDataKeys.edition, UsageDataKeys.Edition.enterprise );
        sysInfo.set( UsageDataKeys.operationalMode, UsageDataKeys.OperationalMode.edge );
    }

    protected void registerRecovery( final String editionName, LifeSupport life,
                                     final DependencyResolver dependencyResolver )
    {
        life.addLifecycleListener( new LifecycleListener()
        {
            @Override
            public void notifyStatusChanged( Object instance, LifecycleStatus from, LifecycleStatus to )
            {
                if ( instance instanceof DatabaseAvailability && to.equals( LifecycleStatus.STARTED ) )
                {
                    doAfterRecoveryAndStartup( editionName, dependencyResolver );
                }
            }
        } );
    }

    private CommitProcessFactory readOnly()
    {
        return new CommitProcessFactory()
        {
            @Override
            public TransactionCommitProcess create( TransactionAppender appender, StorageEngine storageEngine,
                    Config config )
            {
                return new ReadOnlyTransactionCommitProcess();
            }
        };
    }

    protected final class DefaultKernelData extends KernelData implements Lifecycle
    {
        private final GraphDatabaseAPI graphDb;

        public DefaultKernelData( FileSystemAbstraction fileSystem, PageCache pageCache, File storeDir,
                                  Config config, GraphDatabaseAPI graphDb )
        {
            super( fileSystem, pageCache, storeDir, config );
            this.graphDb = graphDb;
        }

        @Override
        public Version version()
        {
            return Version.getKernel();
        }

        @Override
        public GraphDatabaseAPI graphDatabase()
        {
            return graphDb;
        }

        @Override
        public void init() throws Throwable
        {
        }

        @Override
        public void start() throws Throwable
        {
        }

        @Override
        public void stop() throws Throwable
        {
        }
    }

}
