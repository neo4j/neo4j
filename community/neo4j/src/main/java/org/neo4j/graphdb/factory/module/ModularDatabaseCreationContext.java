/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import java.util.function.Function;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.module.edition.context.DatabaseEditionContext;
import org.neo4j.graphdb.factory.module.id.DatabaseIdContext;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.DatabaseCreationContext;
import org.neo4j.kernel.api.explicitindex.AutoIndexing;
import org.neo4j.kernel.availability.DatabaseAvailability;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.DefaultExplicitIndexProvider;
import org.neo4j.kernel.impl.api.ExplicitIndexProvider;
import org.neo4j.kernel.impl.api.NonTransactionalTokenNameLookup;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.api.explicitindex.InternalAutoIndexing;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.kernel.impl.coreapi.CoreAPIAvailabilityGuard;
import org.neo4j.kernel.impl.factory.AccessCapability;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.id.IdController;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.TransactionMonitor;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.kernel.impl.transaction.log.files.LogFileCreationMonitor;
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats;
import org.neo4j.kernel.impl.util.collection.CollectionsFactorySupplier;
import org.neo4j.kernel.impl.util.watcher.FileSystemWatcherService;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.internal.TransactionEventHandlers;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.SystemNanoClock;

public class ModularDatabaseCreationContext implements DatabaseCreationContext
{
    private final String databaseName;
    private final Config config;
    private final IdGeneratorFactory idGeneratorFactory;
    private final LogService logService;
    private final JobScheduler scheduler;
    private final TokenNameLookup tokenNameLookup;
    private final DependencyResolver globalDependencies;
    private final TokenHolders tokenHolders;
    private final Locks locks;
    private final StatementLocksFactory statementLocksFactory;
    private final SchemaWriteGuard schemaWriteGuard;
    private final TransactionEventHandlers transactionEventHandlers;
    private final IndexingService.Monitor indexingServiceMonitor;
    private final FileSystemAbstraction fs;
    private final DatabaseTransactionStats transactionStats;
    private final DatabaseHealth databaseHealth;
    private final LogFileCreationMonitor physicalLogMonitor;
    private final TransactionHeaderInformationFactory transactionHeaderInformationFactory;
    private final CommitProcessFactory commitProcessFactory;
    private final AutoIndexing autoIndexing;
    private final IndexConfigStore indexConfigStore;
    private final ExplicitIndexProvider explicitIndexProvider;
    private final PageCache pageCache;
    private final ConstraintSemantics constraintSemantics;
    private final Monitors monitors;
    private final Tracers tracers;
    private final Procedures procedures;
    private final IOLimiter ioLimiter;
    private final DatabaseAvailabilityGuard databaseAvailabilityGuard;
    private final CoreAPIAvailabilityGuard coreAPIAvailabilityGuard;
    private final SystemNanoClock clock;
    private final AccessCapability accessCapability;
    private final StoreCopyCheckPointMutex storeCopyCheckPointMutex;
    private final RecoveryCleanupWorkCollector recoveryCleanupWorkCollector;
    private final IdController idController;
    private final DatabaseInfo databaseInfo;
    private final VersionContextSupplier versionContextSupplier;
    private final CollectionsFactorySupplier collectionsFactorySupplier;
    private final Iterable<KernelExtensionFactory<?>> kernelExtensionFactories;
    private final Function<File,FileSystemWatcherService> watcherServiceFactory;
    private final GraphDatabaseFacade facade;
    private final Iterable<QueryEngineProvider> engineProviders;
    private final DatabaseLayout databaseLayout;
    private final DatabaseAvailability databaseAvailability;

    ModularDatabaseCreationContext( String databaseName, PlatformModule platformModule, DatabaseEditionContext editionContext,
            Procedures procedures, GraphDatabaseFacade facade )
    {
        this.databaseName = databaseName;
        this.config = platformModule.config;
        DatabaseIdContext idContext = editionContext.getIdContext();
        this.idGeneratorFactory = idContext.getIdGeneratorFactory();
        this.idController = idContext.getIdController();
        this.databaseLayout = platformModule.storeLayout.databaseLayout( databaseName );
        this.logService = platformModule.logging;
        this.scheduler = platformModule.jobScheduler;
        this.globalDependencies =  platformModule.dependencies;
        this.tokenHolders = editionContext.createTokenHolders();
        this.tokenNameLookup = new NonTransactionalTokenNameLookup( tokenHolders );
        this.locks = editionContext.createLocks();
        this.statementLocksFactory = editionContext.createStatementLocksFactory();
        this.schemaWriteGuard = editionContext.getSchemaWriteGuard();
        this.transactionEventHandlers = new TransactionEventHandlers( facade );
        this.monitors = new Monitors( platformModule.monitors );
        this.indexingServiceMonitor = monitors.newMonitor( IndexingService.Monitor.class );
        this.physicalLogMonitor = monitors.newMonitor( LogFileCreationMonitor.class );
        this.fs = platformModule.fileSystem;
        this.transactionStats = editionContext.createTransactionMonitor();
        this.databaseHealth = new DatabaseHealth( platformModule.panicEventGenerator, logService.getInternalLog( DatabaseHealth.class ) );
        this.transactionHeaderInformationFactory = editionContext.getHeaderInformationFactory();
        this.commitProcessFactory = editionContext.getCommitProcessFactory();
        this.autoIndexing = new InternalAutoIndexing( platformModule.config, tokenHolders.propertyKeyTokens() );
        this.indexConfigStore = new IndexConfigStore( databaseLayout, fs );
        this.explicitIndexProvider = new DefaultExplicitIndexProvider();
        this.pageCache = platformModule.pageCache;
        this.constraintSemantics = editionContext.getConstraintSemantics();
        this.tracers = platformModule.tracers;
        this.procedures = procedures;
        this.ioLimiter = editionContext.getIoLimiter();
        this.clock = platformModule.clock;
        this.databaseAvailabilityGuard = editionContext.createDatabaseAvailabilityGuard( clock, logService, config );
        this.databaseAvailability =
                new DatabaseAvailability( databaseAvailabilityGuard, transactionStats, platformModule.clock, getAwaitActiveTransactionDeadlineMillis() );
        this.coreAPIAvailabilityGuard = new CoreAPIAvailabilityGuard( databaseAvailabilityGuard, editionContext.getTransactionStartTimeout() );
        this.accessCapability = editionContext.getAccessCapability();
        this.storeCopyCheckPointMutex = new StoreCopyCheckPointMutex();
        this.recoveryCleanupWorkCollector = platformModule.recoveryCleanupWorkCollector;
        this.databaseInfo = platformModule.databaseInfo;
        this.versionContextSupplier = platformModule.versionContextSupplier;
        this.collectionsFactorySupplier = platformModule.collectionsFactorySupplier;
        this.kernelExtensionFactories = platformModule.kernelExtensionFactories;
        this.watcherServiceFactory = editionContext.getWatcherServiceFactory();
        this.facade = facade;
        this.engineProviders = platformModule.engineProviders;
    }

    @Override
    public String getDatabaseName()
    {
        return databaseName;
    }

    @Override
    public DatabaseLayout getDatabaseLayout()
    {
        return databaseLayout;
    }

    @Override
    public Config getConfig()
    {
        return config;
    }

    @Override
    public IdGeneratorFactory getIdGeneratorFactory()
    {
        return idGeneratorFactory;
    }

    @Override
    public LogService getLogService()
    {
        return logService;
    }

    @Override
    public JobScheduler getScheduler()
    {
        return scheduler;
    }

    @Override
    public TokenNameLookup getTokenNameLookup()
    {
        return tokenNameLookup;
    }

    @Override
    public DependencyResolver getGlobalDependencies()
    {
        return globalDependencies;
    }

    @Override
    public TokenHolders getTokenHolders()
    {
        return tokenHolders;
    }

    @Override
    public Locks getLocks()
    {
        return locks;
    }

    @Override
    public StatementLocksFactory getStatementLocksFactory()
    {
        return statementLocksFactory;
    }

    @Override
    public SchemaWriteGuard getSchemaWriteGuard()
    {
        return schemaWriteGuard;
    }

    @Override
    public TransactionEventHandlers getTransactionEventHandlers()
    {
        return transactionEventHandlers;
    }

    @Override
    public IndexingService.Monitor getIndexingServiceMonitor()
    {
        return indexingServiceMonitor;
    }

    @Override
    public FileSystemAbstraction getFs()
    {
        return fs;
    }

    @Override
    public TransactionMonitor getTransactionMonitor()
    {
        return transactionStats;
    }

    @Override
    public DatabaseHealth getDatabaseHealth()
    {
        return databaseHealth;
    }

    @Override
    public LogFileCreationMonitor getPhysicalLogMonitor()
    {
        return physicalLogMonitor;
    }

    @Override
    public TransactionHeaderInformationFactory getTransactionHeaderInformationFactory()
    {
        return transactionHeaderInformationFactory;
    }

    @Override
    public CommitProcessFactory getCommitProcessFactory()
    {
        return commitProcessFactory;
    }

    @Override
    public AutoIndexing getAutoIndexing()
    {
        return autoIndexing;
    }

    @Override
    public IndexConfigStore getIndexConfigStore()
    {
        return indexConfigStore;
    }

    @Override
    public ExplicitIndexProvider getExplicitIndexProvider()
    {
        return explicitIndexProvider;
    }

    @Override
    public PageCache getPageCache()
    {
        return pageCache;
    }

    @Override
    public ConstraintSemantics getConstraintSemantics()
    {
        return constraintSemantics;
    }

    @Override
    public Monitors getMonitors()
    {
        return monitors;
    }

    @Override
    public Tracers getTracers()
    {
        return tracers;
    }

    @Override
    public Procedures getProcedures()
    {
        return procedures;
    }

    @Override
    public IOLimiter getIoLimiter()
    {
        return ioLimiter;
    }

    @Override
    public DatabaseAvailabilityGuard getDatabaseAvailabilityGuard()
    {
        return databaseAvailabilityGuard;
    }

    @Override
    public CoreAPIAvailabilityGuard getCoreAPIAvailabilityGuard()
    {
        return coreAPIAvailabilityGuard;
    }

    @Override
    public SystemNanoClock getClock()
    {
        return clock;
    }

    @Override
    public AccessCapability getAccessCapability()
    {
        return accessCapability;
    }

    @Override
    public StoreCopyCheckPointMutex getStoreCopyCheckPointMutex()
    {
        return storeCopyCheckPointMutex;
    }

    @Override
    public RecoveryCleanupWorkCollector getRecoveryCleanupWorkCollector()
    {
        return recoveryCleanupWorkCollector;
    }

    @Override
    public IdController getIdController()
    {
        return idController;
    }

    @Override
    public DatabaseInfo getDatabaseInfo()
    {
        return databaseInfo;
    }

    @Override
    public VersionContextSupplier getVersionContextSupplier()
    {
        return versionContextSupplier;
    }

    @Override
    public CollectionsFactorySupplier getCollectionsFactorySupplier()
    {
        return collectionsFactorySupplier;
    }

    @Override
    public Iterable<KernelExtensionFactory<?>> getKernelExtensionFactories()
    {
        return kernelExtensionFactories;
    }

    @Override
    public Function<File,FileSystemWatcherService> getWatcherServiceFactory()
    {
        return watcherServiceFactory;
    }

    @Override
    public GraphDatabaseFacade getFacade()
    {
        return facade;
    }

    @Override
    public Iterable<QueryEngineProvider> getEngineProviders()
    {
        return engineProviders;
    }

    @Override
    public DatabaseAvailability getDatabaseAvailability()
    {
        return databaseAvailability;
    }

    private long getAwaitActiveTransactionDeadlineMillis()
    {
        return config.get( GraphDatabaseSettings.shutdown_transaction_end_timeout ).toMillis();
    }
}
