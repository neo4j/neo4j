/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import java.time.Clock;
import java.util.function.Supplier;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.spatial.Geometry;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.DatabaseAvailability;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.explicitindex.AutoIndexing;
import org.neo4j.kernel.api.security.SecurityContext;
import org.neo4j.kernel.builtinprocs.SpecialBuiltInProcedures;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.guard.Guard;
import org.neo4j.kernel.guard.TerminationGuard;
import org.neo4j.kernel.impl.api.NonTransactionalTokenNameLookup;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.api.dbms.NonTransactionalDbmsOperations;
import org.neo4j.kernel.impl.api.explicitindex.InternalAutoIndexing;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.cache.MonitorGc;
import org.neo4j.kernel.impl.core.DatabasePanicEventGenerator;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.RelationshipProxy;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.StartupStatisticsProvider;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.core.TokenNotFoundException;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.proc.ProcedureConfig;
import org.neo4j.kernel.impl.proc.ProcedureGDSFactory;
import org.neo4j.kernel.impl.proc.ProcedureTransactionProvider;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.proc.TerminationGuardProvider;
import org.neo4j.kernel.impl.proc.TypeMappers.SimpleConverter;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.info.DiagnosticsManager;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.internal.KernelEventHandlers;
import org.neo4j.kernel.internal.TransactionEventHandlers;
import org.neo4j.kernel.internal.Version;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.procedure.ProcedureTransaction;

import static org.neo4j.kernel.api.proc.Context.KERNEL_TRANSACTION;
import static org.neo4j.kernel.api.proc.Context.SECURITY_CONTEXT;
import static org.neo4j.kernel.api.proc.Neo4jTypes.NTGeometry;
import static org.neo4j.kernel.api.proc.Neo4jTypes.NTNode;
import static org.neo4j.kernel.api.proc.Neo4jTypes.NTPath;
import static org.neo4j.kernel.api.proc.Neo4jTypes.NTPoint;
import static org.neo4j.kernel.api.proc.Neo4jTypes.NTRelationship;

/**
 * Datasource module for {@link GraphDatabaseFacadeFactory}. This implements all the
 * remaining services not yet created by either the {@link PlatformModule} or {@link EditionModule}.
 * <p>
 * When creating new services, this would be the default place to put them, unless they need to go into the other
 * modules for any reason.
 */
public class DataSourceModule
{
    public final ThreadToStatementContextBridge threadToTransactionBridge;

    public final NodeManager nodeManager;

    public final NeoStoreDataSource neoStoreDataSource;

    public final Supplier<KernelAPI> kernelAPI;

    public final Supplier<QueryExecutionEngine> queryExecutor;

    public final KernelEventHandlers kernelEventHandlers;

    public final TransactionEventHandlers transactionEventHandlers;

    public final Supplier<StoreId> storeId;

    public final AutoIndexing autoIndexing;

    public final Guard guard;

    public DataSourceModule( final PlatformModule platformModule, EditionModule editionModule,
            Supplier<QueryExecutionEngine> queryExecutionEngineSupplier )
    {
        final Dependencies deps = platformModule.dependencies;
        Config config = platformModule.config;
        LogService logging = platformModule.logging;
        FileSystemAbstraction fileSystem = platformModule.fileSystem;
        DataSourceManager dataSourceManager = platformModule.dataSourceManager;
        LifeSupport life = platformModule.life;
        final GraphDatabaseFacade graphDatabaseFacade = platformModule.graphDatabaseFacade;
        RelationshipTypeTokenHolder relationshipTypeTokenHolder = editionModule.relationshipTypeTokenHolder;
        File storeDir = platformModule.storeDir;
        DiagnosticsManager diagnosticsManager = platformModule.diagnosticsManager;
        this.queryExecutor = queryExecutionEngineSupplier;

        threadToTransactionBridge = deps.satisfyDependency( life.add( new ThreadToStatementContextBridge() ) );

        nodeManager = deps.satisfyDependency( new NodeManager( graphDatabaseFacade,
                threadToTransactionBridge, relationshipTypeTokenHolder ) );

        NodeProxy.NodeActions nodeActions = deps.satisfyDependency( createNodeActions( graphDatabaseFacade,
                threadToTransactionBridge, nodeManager ) );
        RelationshipProxy.RelationshipActions relationshipActions = deps.satisfyDependency(
                createRelationshipActions( graphDatabaseFacade, threadToTransactionBridge, nodeManager,
                        relationshipTypeTokenHolder ) );

        transactionEventHandlers = new TransactionEventHandlers( nodeActions, relationshipActions );

        diagnosticsManager.prependProvider( config );

        life.add( platformModule.kernelExtensions );

        // Factories for things that needs to be created later
        PageCache pageCache = platformModule.pageCache;

        StartupStatisticsProvider startupStatistics = deps.satisfyDependency( new StartupStatisticsProvider() );

        SchemaWriteGuard schemaWriteGuard = deps.satisfyDependency( editionModule.schemaWriteGuard );

        guard = createGuard( deps, platformModule.clock, logging );

        kernelEventHandlers = new KernelEventHandlers( logging.getInternalLog( KernelEventHandlers.class ) );

        DatabasePanicEventGenerator databasePanicEventGenerator = deps.satisfyDependency(
                new DatabasePanicEventGenerator( kernelEventHandlers ) );

        DatabaseHealth databaseHealth = deps.satisfyDependency( new DatabaseHealth( databasePanicEventGenerator,
                logging.getInternalLog( DatabaseHealth.class ) ) );

        autoIndexing = new InternalAutoIndexing( platformModule.config, editionModule.propertyKeyTokenHolder );

        Procedures procedures = setupProcedures( platformModule, editionModule );

        deps.satisfyDependency( new NonTransactionalDbmsOperations( procedures ) );

        editionModule.setupSecurityModule( platformModule, procedures );

        NonTransactionalTokenNameLookup tokenNameLookup = new NonTransactionalTokenNameLookup(
                editionModule.labelTokenHolder,
                editionModule.relationshipTypeTokenHolder,
                editionModule.propertyKeyTokenHolder );

        neoStoreDataSource = deps.satisfyDependency( new NeoStoreDataSource(
                storeDir,
                config,
                editionModule.idGeneratorFactory,
                logging,
                platformModule.jobScheduler,
                tokenNameLookup,
                deps,
                editionModule.propertyKeyTokenHolder,
                editionModule.labelTokenHolder,
                relationshipTypeTokenHolder,
                editionModule.statementLocksFactory,
                schemaWriteGuard,
                transactionEventHandlers,
                platformModule.monitors.newMonitor( IndexingService.Monitor.class ),
                fileSystem,
                platformModule.transactionMonitor,
                databaseHealth,
                platformModule.monitors.newMonitor( PhysicalLogFile.Monitor.class ),
                editionModule.headerInformationFactory,
                startupStatistics,
                guard,
                editionModule.commitProcessFactory,
                autoIndexing,
                pageCache,
                editionModule.constraintSemantics,
                platformModule.monitors,
                platformModule.tracers,
                procedures,
                editionModule.ioLimiter,
                platformModule.availabilityGuard,
                platformModule.clock, editionModule.accessCapability,
                platformModule.storeCopyCheckPointMutex,
                platformModule.recoveryCleanupWorkCollector,
                editionModule.idController,
                platformModule.databaseInfo.operationalMode ) );

        dataSourceManager.register( neoStoreDataSource );

        life.add( new MonitorGc( config, logging.getInternalLog( MonitorGc.class ) ) );

        life.add( nodeManager );

        life.add( new DatabaseAvailability( platformModule.availabilityGuard, platformModule.transactionMonitor,
                config.get( GraphDatabaseSettings.shutdown_transaction_end_timeout ).toMillis() ) );

        life.add( new StartupWaiter( platformModule.availabilityGuard, editionModule.transactionStartTimeout ) );

        // Kernel event handlers should be the very last, i.e. very first to receive shutdown events
        life.add( kernelEventHandlers );

        this.storeId = neoStoreDataSource::getStoreId;
        this.kernelAPI = neoStoreDataSource::getKernel;

        ProcedureGDSFactory gdsFactory = new ProcedureGDSFactory( platformModule, this, deps,
                editionModule.coreAPIAvailabilityGuard );
        procedures.registerComponent( GraphDatabaseService.class, gdsFactory::apply, true );
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
            public Relationship newRelationshipProxy( long id, long startNodeId, int typeId, long endNodeId )
            {
                return nodeManager.newRelationshipProxy( id, startNodeId, typeId, endNodeId );
            }
        };
    }

    private Guard createGuard( Dependencies deps, Clock clock, LogService logging )
    {
        TerminationGuard guard = createGuard();
        deps.satisfyDependency( guard );
        return guard;
    }

    protected TerminationGuard createGuard()
    {
        return new TerminationGuard();
    }

    private Procedures setupProcedures( PlatformModule platform, EditionModule editionModule )
    {
        File pluginDir = platform.config.get( GraphDatabaseSettings.plugin_dir );
        Log internalLog = platform.logging.getInternalLog( Procedures.class );

        Procedures procedures = new Procedures(
                new SpecialBuiltInProcedures( Version.getNeo4jVersion(),
                        platform.databaseInfo.edition.toString() ),
                pluginDir, internalLog, new ProcedureConfig( platform.config ) );
        platform.life.add( procedures );
        platform.dependencies.satisfyDependency( procedures );

        procedures.registerType( Node.class, new SimpleConverter( NTNode, Node.class ) );
        procedures.registerType( Relationship.class, new SimpleConverter( NTRelationship, Relationship.class ) );
        procedures.registerType( Path.class, new SimpleConverter( NTPath, Path.class ) );
        procedures.registerType( Geometry.class, new SimpleConverter( NTGeometry, Geometry.class ) );
        procedures.registerType( Point.class, new SimpleConverter( NTPoint, Point.class ) );

        // Register injected public API components
        Log proceduresLog = platform.logging.getUserLog( Procedures.class );
        procedures.registerComponent( Log.class, ctx -> proceduresLog, true );

        Guard guard = platform.dependencies.resolveDependency( Guard.class );
        procedures.registerComponent( ProcedureTransaction.class, new ProcedureTransactionProvider(), true );
        procedures.registerComponent( org.neo4j.procedure.TerminationGuard.class, new TerminationGuardProvider( guard ), true );

        // Below components are not public API, but are made available for internal
        // procedures to call, and to provide temporary workarounds for the following
        // patterns:
        //  - Batch-transaction imports (GDAPI, needs to be real and passed to background processing threads)
        //  - Group-transaction writes (same pattern as above, but rather than splitting large transactions,
        //                              combine lots of small ones)
        //  - Bleeding-edge performance (KernelTransaction, to bypass overhead of working with Core API)
        procedures.registerComponent( DependencyResolver.class, ctx -> platform.dependencies, false );
        procedures.registerComponent( KernelTransaction.class, ctx -> ctx.get( KERNEL_TRANSACTION ), false );
        procedures.registerComponent( GraphDatabaseAPI.class, ctx -> platform.graphDatabaseFacade, false );

        // Security procedures
        procedures.registerComponent( SecurityContext.class, ctx -> ctx.get( SECURITY_CONTEXT ), true );

        // Edition procedures
        try
        {
            editionModule.registerProcedures( procedures );
        }
        catch ( KernelException e )
        {
            internalLog.error( "Failed to register built-in edition procedures at start up: " + e.getMessage() );
        }

        return procedures;
    }

    /**
     * At end of startup, wait for instance to become available for transactions.
     * <p>
     * This helps users who expect to be able to access the instance after
     * the constructor is run.
     */
    private static class StartupWaiter extends LifecycleAdapter
    {
        private final AvailabilityGuard availabilityGuard;
        private final long timeout;

        StartupWaiter( AvailabilityGuard availabilityGuard, long timeout )
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
