/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.graphdb.factory.module;

import java.io.File;
import java.util.function.Supplier;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.InwardKernel;
import org.neo4j.kernel.api.explicitindex.AutoIndexing;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.NonTransactionalTokenNameLookup;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.api.explicitindex.InternalAutoIndexing;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.DatabasePanicEventGenerator;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.kernel.impl.transaction.log.files.LogFileCreationMonitor;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.info.DiagnosticsManager;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.internal.TransactionEventHandlers;
import org.neo4j.kernel.lifecycle.LifeSupport;

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

    public final NeoStoreDataSource neoStoreDataSource;

    public final Supplier<InwardKernel> kernelAPI;

    public final Supplier<QueryExecutionEngine> queryExecutor;

    public final StoreCopyCheckPointMutex storeCopyCheckPointMutex;

    public final TransactionEventHandlers transactionEventHandlers;

    public final Supplier<StoreId> storeId;

    public final AutoIndexing autoIndexing;

    public DataSourceModule( final PlatformModule platformModule, EditionModule editionModule,
            Supplier<QueryExecutionEngine> queryExecutionEngineSupplier, Procedures procedures )
    {
        final Dependencies deps = platformModule.dependencies;
        Config config = platformModule.config;
        LogService logging = platformModule.logging;
        FileSystemAbstraction fileSystem = platformModule.fileSystem;
        DataSourceManager dataSourceManager = platformModule.dataSourceManager;
        LifeSupport life = platformModule.life;
        final GraphDatabaseFacade graphDatabaseFacade = platformModule.graphDatabaseFacade;
        File storeDir = platformModule.storeDir;
        DiagnosticsManager diagnosticsManager = platformModule.diagnosticsManager;
        this.queryExecutor = queryExecutionEngineSupplier;

        threadToTransactionBridge = deps.satisfyDependency( new ThreadToStatementContextBridge( platformModule.availabilityGuard ) );

        deps.satisfyDependency( graphDatabaseFacade );
        transactionEventHandlers = new TransactionEventHandlers( graphDatabaseFacade );

        diagnosticsManager.prependProvider( config );

        life.add( platformModule.kernelExtensions );

        // Factories for things that needs to be created later
        PageCache pageCache = platformModule.pageCache;

        SchemaWriteGuard schemaWriteGuard = deps.satisfyDependency( editionModule.schemaWriteGuard );

        DatabasePanicEventGenerator databasePanicEventGenerator = deps.satisfyDependency(
                new DatabasePanicEventGenerator( platformModule.eventHandlers ) );

        DatabaseHealth databaseHealth = deps.satisfyDependency( new DatabaseHealth( databasePanicEventGenerator,
                logging.getInternalLog( DatabaseHealth.class ) ) );

        autoIndexing = new InternalAutoIndexing( platformModule.config, editionModule.tokenHolders.propertyKeyTokens() );

        NonTransactionalTokenNameLookup tokenNameLookup = new NonTransactionalTokenNameLookup( editionModule.tokenHolders );

        storeCopyCheckPointMutex = new StoreCopyCheckPointMutex();
        deps.satisfyDependency( storeCopyCheckPointMutex );

        neoStoreDataSource = deps.satisfyDependency( new NeoStoreDataSource(
                storeDir,
                config,
                editionModule.idGeneratorFactory,
                logging,
                platformModule.jobScheduler,
                tokenNameLookup,
                deps,
                editionModule.tokenHolders,
                editionModule.statementLocksFactory,
                schemaWriteGuard,
                transactionEventHandlers,
                platformModule.monitors.newMonitor( IndexingService.Monitor.class ),
                fileSystem,
                platformModule.transactionMonitor,
                databaseHealth,
                platformModule.monitors.newMonitor( LogFileCreationMonitor.class ),
                editionModule.headerInformationFactory,
                editionModule.commitProcessFactory,
                autoIndexing,
                pageCache,
                editionModule.constraintSemantics,
                platformModule.monitors,
                platformModule.tracers,
                procedures,
                editionModule.ioLimiter,
                platformModule.availabilityGuard,
                platformModule.clock,
                editionModule.accessCapability,
                storeCopyCheckPointMutex,
                platformModule.recoveryCleanupWorkCollector,
                editionModule.idController,
                platformModule.databaseInfo.operationalMode,
                platformModule.versionContextSupplier,
                platformModule.collectionsFactorySupplier ) );

        dataSourceManager.register( neoStoreDataSource );

        this.storeId = neoStoreDataSource::getStoreId;
        this.kernelAPI = neoStoreDataSource::getKernel;

        ProcedureGDSFactory gdsFactory = new ProcedureGDSFactory( platformModule, this, deps,
                editionModule.coreAPIAvailabilityGuard, editionModule.tokenHolders );
        procedures.registerComponent( GraphDatabaseService.class, gdsFactory::apply, true );
    }
}
