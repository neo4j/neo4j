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
import java.util.function.Function;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.DatabaseCreationContext;
import org.neo4j.kernel.api.explicitindex.AutoIndexing;
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
import org.neo4j.kernel.impl.factory.AccessCapability;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.id.IdController;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.TransactionMonitor;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.kernel.impl.transaction.log.files.LogFileCreationMonitor;
import org.neo4j.kernel.impl.util.collection.CollectionsFactorySupplier;
import org.neo4j.kernel.impl.util.watcher.FileSystemWatcherService;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.internal.TransactionEventHandlers;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.SystemNanoClock;

public class ModularDatabaseCreationContext implements DatabaseCreationContext
{
    private final String databaseName;
    private final File databaseDirectory;
    private final Config config;
    private final IdGeneratorFactory idGeneratorFactory;
    private final LogService logService;
    private final JobScheduler scheduler;
    private final TokenNameLookup tokenNameLookup;
    private final DependencyResolver globalDependencies;
    private final TokenHolders tokenHolders;
    private final StatementLocksFactory statementLocksFactory;
    private final SchemaWriteGuard schemaWriteGuard;
    private final TransactionEventHandlers transactionEventHandlers;
    private final IndexingService.Monitor indexingServiceMonitor;
    private final FileSystemAbstraction fs;
    private final TransactionMonitor transactionMonitor;
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
    private final AvailabilityGuard availabilityGuard;
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

    ModularDatabaseCreationContext( String databaseName, File databaseDirectory, PlatformModule platformModule, EditionModule editionModule,
            Procedures procedures, GraphDatabaseFacade facade, TokenHolders tokenHolders )
    {
        this.databaseName = databaseName;
        this.databaseDirectory = databaseDirectory;
        this.config = platformModule.config;
        this.idGeneratorFactory = editionModule.idGeneratorFactory;
        this.logService = platformModule.logging;
        this.scheduler = platformModule.jobScheduler;
        this.globalDependencies =  platformModule.dependencies;
        this.tokenHolders = tokenHolders;
        this.tokenNameLookup = new NonTransactionalTokenNameLookup( tokenHolders );
        this.statementLocksFactory = editionModule.statementLocksFactory;
        this.schemaWriteGuard = editionModule.schemaWriteGuard;
        this.transactionEventHandlers = new TransactionEventHandlers( facade );
        this.monitors = new Monitors( platformModule.monitors );
        this.indexingServiceMonitor = monitors.newMonitor( IndexingService.Monitor.class );
        this.physicalLogMonitor = monitors.newMonitor( LogFileCreationMonitor.class );
        this.fs = platformModule.fileSystem;
        this.transactionMonitor = editionModule.createTransactionMonitor();
        this.databaseHealth = new DatabaseHealth( platformModule.panicEventGenerator, logService.getInternalLog( DatabaseHealth.class ) );
        this.transactionHeaderInformationFactory = editionModule.headerInformationFactory;
        this.commitProcessFactory = editionModule.commitProcessFactory;
        this.autoIndexing = new InternalAutoIndexing( platformModule.config, tokenHolders.propertyKeyTokens() );
        this.indexConfigStore = new IndexConfigStore( databaseDirectory, fs );
        this.explicitIndexProvider = new DefaultExplicitIndexProvider();
        this.pageCache = platformModule.pageCache;
        this.constraintSemantics = editionModule.constraintSemantics;
        this.tracers = platformModule.tracers;
        this.procedures = procedures;
        this.ioLimiter = editionModule.ioLimiter;
        this.availabilityGuard = platformModule.availabilityGuard;
        this.clock = platformModule.clock;
        this.accessCapability = editionModule.accessCapability;
        this.storeCopyCheckPointMutex = new StoreCopyCheckPointMutex();
        this.recoveryCleanupWorkCollector = platformModule.recoveryCleanupWorkCollector;
        this.idController = editionModule.idController;
        this.databaseInfo = platformModule.databaseInfo;
        this.versionContextSupplier = platformModule.versionContextSupplier;
        this.collectionsFactorySupplier = platformModule.collectionsFactorySupplier;
        this.kernelExtensionFactories = platformModule.kernelExtensionFactories;
        this.watcherServiceFactory = editionModule.watcherServiceFactory;
        this.facade = facade;
        this.engineProviders = platformModule.engineProviders;
    }

    public String getDatabaseName()
    {
        return databaseName;
    }

    public File getDatabaseDirectory()
    {
        return databaseDirectory;
    }

    public Config getConfig()
    {
        return config;
    }

    public IdGeneratorFactory getIdGeneratorFactory()
    {
        return idGeneratorFactory;
    }

    public LogService getLogService()
    {
        return logService;
    }

    public JobScheduler getScheduler()
    {
        return scheduler;
    }

    public TokenNameLookup getTokenNameLookup()
    {
        return tokenNameLookup;
    }

    public DependencyResolver getGlobalDependencies()
    {
        return globalDependencies;
    }

    public TokenHolders getTokenHolders()
    {
        return tokenHolders;
    }

    public StatementLocksFactory getStatementLocksFactory()
    {
        return statementLocksFactory;
    }

    public SchemaWriteGuard getSchemaWriteGuard()
    {
        return schemaWriteGuard;
    }

    public TransactionEventHandlers getTransactionEventHandlers()
    {
        return transactionEventHandlers;
    }

    public IndexingService.Monitor getIndexingServiceMonitor()
    {
        return indexingServiceMonitor;
    }

    public FileSystemAbstraction getFs()
    {
        return fs;
    }

    public TransactionMonitor getTransactionMonitor()
    {
        return transactionMonitor;
    }

    public DatabaseHealth getDatabaseHealth()
    {
        return databaseHealth;
    }

    public LogFileCreationMonitor getPhysicalLogMonitor()
    {
        return physicalLogMonitor;
    }

    public TransactionHeaderInformationFactory getTransactionHeaderInformationFactory()
    {
        return transactionHeaderInformationFactory;
    }

    public CommitProcessFactory getCommitProcessFactory()
    {
        return commitProcessFactory;
    }

    public AutoIndexing getAutoIndexing()
    {
        return autoIndexing;
    }

    public IndexConfigStore getIndexConfigStore()
    {
        return indexConfigStore;
    }

    public ExplicitIndexProvider getExplicitIndexProvider()
    {
        return explicitIndexProvider;
    }

    public PageCache getPageCache()
    {
        return pageCache;
    }

    public ConstraintSemantics getConstraintSemantics()
    {
        return constraintSemantics;
    }

    public Monitors getMonitors()
    {
        return monitors;
    }

    public Tracers getTracers()
    {
        return tracers;
    }

    public Procedures getProcedures()
    {
        return procedures;
    }

    public IOLimiter getIoLimiter()
    {
        return ioLimiter;
    }

    public AvailabilityGuard getAvailabilityGuard()
    {
        return availabilityGuard;
    }

    public SystemNanoClock getClock()
    {
        return clock;
    }

    public AccessCapability getAccessCapability()
    {
        return accessCapability;
    }

    public StoreCopyCheckPointMutex getStoreCopyCheckPointMutex()
    {
        return storeCopyCheckPointMutex;
    }

    public RecoveryCleanupWorkCollector getRecoveryCleanupWorkCollector()
    {
        return recoveryCleanupWorkCollector;
    }

    public IdController getIdController()
    {
        return idController;
    }

    public DatabaseInfo getDatabaseInfo()
    {
        return databaseInfo;
    }

    public VersionContextSupplier getVersionContextSupplier()
    {
        return versionContextSupplier;
    }

    public CollectionsFactorySupplier getCollectionsFactorySupplier()
    {
        return collectionsFactorySupplier;
    }

    public Iterable<KernelExtensionFactory<?>> getKernelExtensionFactories()
    {
        return kernelExtensionFactories;
    }

    public Function<File,FileSystemWatcherService> getWatcherServiceFactory()
    {
        return watcherServiceFactory;
    }

    public GraphDatabaseFacade getFacade()
    {
        return facade;
    }

    public Iterable<QueryEngineProvider> getEngineProviders()
    {
        return engineProviders;
    }
}
