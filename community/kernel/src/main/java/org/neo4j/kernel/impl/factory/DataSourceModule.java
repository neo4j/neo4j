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
package org.neo4j.kernel.impl.factory;

import java.io.File;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.function.Supplier;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.RelationshipAutoIndexer;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.DatabaseAvailability;
import org.neo4j.kernel.KernelEventHandlers;
import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.TransactionEventHandlers;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.guard.Guard;
import org.neo4j.kernel.impl.api.NonTransactionalTokenNameLookup;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.cache.MonitorGc;
import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.RelationshipProxy;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.StartupStatisticsProvider;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.core.TokenNotFoundException;
import org.neo4j.kernel.impl.coreapi.IndexManagerImpl;
import org.neo4j.kernel.impl.coreapi.IndexProvider;
import org.neo4j.kernel.impl.coreapi.IndexProviderImpl;
import org.neo4j.kernel.impl.coreapi.LegacyIndexProxy;
import org.neo4j.kernel.impl.coreapi.NodeAutoIndexerImpl;
import org.neo4j.kernel.impl.coreapi.RelationshipAutoIndexerImpl;
import org.neo4j.kernel.impl.coreapi.schema.SchemaImpl;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.storemigration.StoreMigrator;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.storemigration.monitoring.VisibleMigrationProgressMonitor;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.info.DiagnosticsManager;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory.Configuration.execution_guard_enabled;

/**
 * Datasource module for {@link GraphDatabaseFacadeFactory}. This implements all the
 * remaining services not yet created by either the {@link PlatformModule} or {@link EditionModule}.
 * <p/>
 * When creating new services, this would be the default place to put them, unless they need to go into the other
 * modules for any reason.
 */
public class DataSourceModule
{
    public final ThreadToStatementContextBridge threadToTransactionBridge;

    public final NodeManager nodeManager;

    public final NeoStoreDataSource neoStoreDataSource;

    public final IndexManager indexManager;

    public final Schema schema;

    public final Supplier<KernelAPI> kernelAPI;

    public final Supplier<QueryExecutionEngine> queryExecutor;

    public final KernelEventHandlers kernelEventHandlers;

    public final TransactionEventHandlers transactionEventHandlers;

    public final Supplier<StoreId> storeId;

    public DataSourceModule( final GraphDatabaseFacadeFactory.Dependencies dependencies,
            final PlatformModule platformModule, EditionModule editionModule )
    {
        final org.neo4j.kernel.impl.util.Dependencies deps = platformModule.dependencies;
        Config config = platformModule.config;
        LogService logging = platformModule.logging;
        FileSystemAbstraction fileSystem = platformModule.fileSystem;
        DataSourceManager dataSourceManager = platformModule.dataSourceManager;
        LifeSupport life = platformModule.life;
        final GraphDatabaseFacade graphDatabaseFacade = platformModule.graphDatabaseFacade;
        RelationshipTypeTokenHolder relationshipTypeTokenHolder = editionModule.relationshipTypeTokenHolder;
        File storeDir = platformModule.storeDir;
        DiagnosticsManager diagnosticsManager = platformModule.diagnosticsManager;

        threadToTransactionBridge = deps.satisfyDependency( life.add( new ThreadToStatementContextBridge() ) );

        nodeManager = deps.satisfyDependency( new NodeManager( graphDatabaseFacade,
                threadToTransactionBridge, relationshipTypeTokenHolder ) );

        NodeProxy.NodeActions nodeActions = deps.satisfyDependency( createNodeActions( graphDatabaseFacade,
                threadToTransactionBridge, nodeManager ) );
        RelationshipProxy.RelationshipActions relationshipActions = deps.satisfyDependency(
                createRelationshipActions( graphDatabaseFacade, threadToTransactionBridge, nodeManager,
                        relationshipTypeTokenHolder ) );

        transactionEventHandlers = new TransactionEventHandlers( nodeActions, relationshipActions,
                threadToTransactionBridge );

        IndexConfigStore indexStore =
                life.add( deps.satisfyDependency( new IndexConfigStore( storeDir, fileSystem ) ) );

        diagnosticsManager.prependProvider( config );

        life.add( platformModule.kernelExtensions );

        schema = new SchemaImpl( threadToTransactionBridge );

        final LegacyIndexProxy.Lookup indexLookup = new LegacyIndexProxy.Lookup()
        {
            @Override
            public GraphDatabaseService getGraphDatabaseService()
            {
                return graphDatabaseFacade;
            }
        };

        final IndexProvider indexProvider = new IndexProviderImpl( indexLookup, threadToTransactionBridge );
        NodeAutoIndexerImpl nodeAutoIndexer = life.add( new NodeAutoIndexerImpl( config, indexProvider, nodeManager ) );
        RelationshipAutoIndexer relAutoIndexer = life.add( new RelationshipAutoIndexerImpl( config, indexProvider,
                nodeManager ) );
        indexManager = new IndexManagerImpl(
                threadToTransactionBridge, indexProvider, nodeAutoIndexer, relAutoIndexer );

        // Factories for things that needs to be created later
        PageCache pageCache = platformModule.pageCache;
        StoreFactory storeFactory = new StoreFactory( storeDir, config, editionModule.idGeneratorFactory,
                pageCache, fileSystem, logging.getInternalLogProvider() );

        StartupStatisticsProvider startupStatistics = deps.satisfyDependency( new StartupStatisticsProvider() );

        SchemaWriteGuard schemaWriteGuard = deps.satisfyDependency( editionModule.schemaWriteGuard );

        StoreUpgrader storeMigrationProcess = new StoreUpgrader( editionModule.upgradeConfiguration, fileSystem,
                platformModule.monitors.newMonitor( StoreUpgrader.Monitor.class ), logging.getInternalLogProvider() );

        VisibleMigrationProgressMonitor progressMonitor =
                new VisibleMigrationProgressMonitor( logging.getInternalLog( StoreMigrator.class ) );
        storeMigrationProcess.addParticipant(
                new StoreMigrator( progressMonitor, fileSystem, pageCache, config, logging ) );

        Guard guard = config.get( execution_guard_enabled ) ?
                      deps.satisfyDependency( new Guard( logging.getInternalLog( Guard.class ) ) ) :
                      null;

        kernelEventHandlers = new KernelEventHandlers( logging.getInternalLog( KernelEventHandlers.class ) );

        KernelPanicEventGenerator kernelPanicEventGenerator = deps.satisfyDependency(
                new KernelPanicEventGenerator( kernelEventHandlers ) );

        KernelHealth kernelHealth = deps.satisfyDependency( new KernelHealth( kernelPanicEventGenerator,
                logging.getInternalLog( KernelHealth.class ) ) );

        neoStoreDataSource = deps.satisfyDependency( new NeoStoreDataSource( storeDir, config,
                storeFactory, logging.getInternalLogProvider(), platformModule.jobScheduler,
                new NonTransactionalTokenNameLookup( editionModule.labelTokenHolder,
                        editionModule.relationshipTypeTokenHolder, editionModule.propertyKeyTokenHolder ),
                deps, editionModule.propertyKeyTokenHolder, editionModule.labelTokenHolder, relationshipTypeTokenHolder,
                editionModule.statementLocksFactory, schemaWriteGuard, transactionEventHandlers,
                platformModule.monitors.newMonitor( IndexingService.Monitor.class ), fileSystem,
                storeMigrationProcess, platformModule.transactionMonitor, kernelHealth,
                platformModule.monitors.newMonitor( PhysicalLogFile.Monitor.class ),
                editionModule.headerInformationFactory, startupStatistics, nodeManager, guard, indexStore,
                editionModule.commitProcessFactory, pageCache, editionModule.constraintSemantics,
                platformModule.monitors, platformModule.tracers, editionModule.idGeneratorFactory,
                editionModule.eligibleForIdReuse, editionModule.idTypeConfigurationProvider ) );
        dataSourceManager.register( neoStoreDataSource );

        life.add( new MonitorGc( config, logging.getInternalLog( MonitorGc.class ) ) );

        life.add( nodeManager );

        life.add( new DatabaseAvailability( platformModule.availabilityGuard, platformModule.transactionMonitor ) );

        life.add( new StartupWaiter( platformModule.availabilityGuard, editionModule.transactionStartTimeout ) );

        // Kernel event handlers should be the very last, i.e. very first to receive shutdown events
        life.add( kernelEventHandlers );

        final AtomicReference<QueryExecutionEngine> queryExecutor = new AtomicReference<>( QueryEngineProvider
                .noEngine() );

        dataSourceManager.addListener( new DataSourceManager.Listener()
        {
            private QueryExecutionEngine engine;

            @Override
            public void registered( NeoStoreDataSource dataSource )
            {
                if ( engine == null )
                {
                    engine = QueryEngineProvider.initialize( platformModule.graphDatabaseFacade,
                            dependencies.executionEngines() );

                    deps.satisfyDependency( engine );
                }

                queryExecutor.set( engine );
            }

            @Override
            public void unregistered( NeoStoreDataSource dataSource )
            {
                queryExecutor.set( QueryEngineProvider.noEngine() );
            }
        } );

        storeId = new Supplier<StoreId>()
        {
            @Override
            public StoreId get()
            {
                return neoStoreDataSource.getStoreId();
            }
        };

        kernelAPI = new Supplier<KernelAPI>()
        {
            @Override
            public KernelAPI get()
            {
                return neoStoreDataSource.getKernel();
            }
        };

        this.queryExecutor = new Supplier<QueryExecutionEngine>()
        {
            @Override
            public QueryExecutionEngine get()
            {
                return queryExecutor.get();
            }
        };
    }

    protected RelationshipProxy.RelationshipActions createRelationshipActions(
            final GraphDatabaseService graphDatabaseService,
            final ThreadToStatementContextBridge threadToStatementContextBridge,
            final NodeManager nodeManager,
            final RelationshipTypeTokenHolder relationshipTypeTokenHolder )
    {
        return new RelationshipProxy.RelationshipActions()
        {
            @Override
            public GraphDatabaseService getGraphDatabaseService()
            {
                return graphDatabaseService;
            }

            @Override
            public void failTransaction()
            {
                threadToStatementContextBridge.getKernelTransactionBoundToThisThread( true ).failure();
            }

            @Override
            public void assertInUnterminatedTransaction()
            {
                threadToStatementContextBridge.assertInUnterminatedTransaction();
            }

            @Override
            public Statement statement()
            {
                return threadToStatementContextBridge.get();
            }

            @Override
            public Node newNodeProxy( long nodeId )
            {
                // only used by relationship already checked as valid in cache
                return nodeManager.newNodeProxyById( nodeId );
            }

            @Override
            public RelationshipType getRelationshipTypeById( int type )
            {
                try
                {
                    return relationshipTypeTokenHolder.getTokenById( type );
                }
                catch ( TokenNotFoundException e )
                {
                    throw new NotFoundException( e );
                }
            }
        };
    }

    protected NodeProxy.NodeActions createNodeActions( final GraphDatabaseService graphDatabaseService,
            final ThreadToStatementContextBridge threadToStatementContextBridge,
            final NodeManager nodeManager )
    {
        return new NodeProxy.NodeActions()
        {
            @Override
            public Statement statement()
            {
                return threadToStatementContextBridge.get();
            }

            @Override
            public GraphDatabaseService getGraphDatabase()
            {
                // TODO This should be wrapped as well
                return graphDatabaseService;
            }

            @Override
            public void assertInUnterminatedTransaction()
            {
                threadToStatementContextBridge.assertInUnterminatedTransaction();
            }

            @Override
            public void failTransaction()
            {
                threadToStatementContextBridge.getKernelTransactionBoundToThisThread( true ).failure();
            }

            @Override
            public Relationship lazyRelationshipProxy( long id )
            {
                return nodeManager.newRelationshipProxyById( id );
            }

            @Override
            public Relationship newRelationshipProxy( long id )
            {
                return nodeManager.newRelationshipProxy( id );
            }

            @Override
            public Relationship newRelationshipProxy( long id, long startNodeId, int typeId, long endNodeId )
            {
                return nodeManager.newRelationshipProxy( id, startNodeId, typeId, endNodeId );
            }
        };
    }

    /**
     * At end of startup, wait for instance to become available for transactions.
     * <p/>
     * This helps users who expect to be able to access the instance after
     * the constructor is run.
     */
    private static class StartupWaiter extends LifecycleAdapter
    {
        private final AvailabilityGuard availabilityGuard;
        private final long timeout;

        public StartupWaiter( AvailabilityGuard availabilityGuard, long timeout )
        {
            this.availabilityGuard = availabilityGuard;
            this.timeout = timeout;
        }

        @Override
        public void start() throws Throwable
        {
            availabilityGuard.isAvailable( timeout );
        }
    }
}
