/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import java.util.function.Function;

import org.neo4j.common.DependencyResolver;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabaseConfig;
import org.neo4j.function.Factory;
import org.neo4j.graphdb.factory.module.edition.context.EditionDatabaseComponents;
import org.neo4j.graphdb.factory.module.id.DatabaseIdContext;
import org.neo4j.internal.id.IdController;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.watcher.DatabaseLayoutWatcher;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.database.DatabaseCreationContext;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.NonTransactionalTokenNameLookup;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.factory.AccessCapability;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.impl.storemigration.DatabaseMigratorFactory;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.events.GlobalTransactionEventListeners;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats;
import org.neo4j.kernel.impl.util.collection.CollectionsFactorySupplier;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.DatabaseEventListeners;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.DatabasePanicEventGenerator;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.token.TokenHolders;

public class ModularDatabaseCreationContext implements DatabaseCreationContext
{
    private final DatabaseId databaseId;
    private final Config globalConfig;
    private final DatabaseConfig databaseConfig;
    private final IdGeneratorFactory idGeneratorFactory;
    private final LogService logService;
    private final JobScheduler scheduler;
    private final TokenNameLookup tokenNameLookup;
    private final DependencyResolver globalDependencies;
    private final TokenHolders tokenHolders;
    private final Locks locks;
    private final StatementLocksFactory statementLocksFactory;
    private final FileSystemAbstraction fs;
    private final DatabaseTransactionStats transactionStats;
    private final Factory<DatabaseHealth> databaseHealthFactory;
    private final TransactionHeaderInformationFactory transactionHeaderInformationFactory;
    private final CommitProcessFactory commitProcessFactory;
    private final PageCache pageCache;
    private final ConstraintSemantics constraintSemantics;
    private final Monitors globalMonitors;
    private final Tracers tracers;
    private final GlobalProcedures globalProcedures;
    private final IOLimiter ioLimiter;
    private final Factory<DatabaseAvailabilityGuard> databaseAvailabilityGuardFactory;
    private final SystemNanoClock clock;
    private final AccessCapability accessCapability;
    private final StoreCopyCheckPointMutex storeCopyCheckPointMutex;
    private final IdController idController;
    private final DatabaseInfo databaseInfo;
    private final VersionContextSupplier versionContextSupplier;
    private final CollectionsFactorySupplier collectionsFactorySupplier;
    private final Iterable<ExtensionFactory<?>> extensionFactories;
    private final Function<DatabaseLayout,DatabaseLayoutWatcher> watcherServiceFactory;
    private final GraphDatabaseFacade facade;
    private final Iterable<QueryEngineProvider> engineProviders;
    private final DatabaseLayout databaseLayout;
    private final DatabaseEventListeners eventListeners;
    private final GlobalTransactionEventListeners transactionEventListeners;
    private final DatabaseMigratorFactory databaseMigratorFactory;
    private final StorageEngineFactory storageEngineFactory;

    public ModularDatabaseCreationContext( DatabaseId databaseId, GlobalModule globalModule, EditionDatabaseComponents perEditionComponents,
            GlobalProcedures globalProcedures, GraphDatabaseFacade facade )
    {
        this.databaseId = databaseId;
        this.globalConfig = globalModule.getGlobalConfig();
        this.databaseConfig = DatabaseConfig.from( globalConfig, databaseId );
        DatabaseIdContext idContext = perEditionComponents.getIdContext();
        this.idGeneratorFactory = idContext.getIdGeneratorFactory();
        this.idController = idContext.getIdController();
        this.databaseLayout = globalModule.getStoreLayout().databaseLayout( databaseId.name() );
        this.logService = globalModule.getLogService();
        this.scheduler = globalModule.getJobScheduler();
        this.globalDependencies =  globalModule.getGlobalDependencies();
        this.tokenHolders = perEditionComponents.getTokenHolders();
        this.tokenNameLookup = new NonTransactionalTokenNameLookup( tokenHolders );
        this.locks = perEditionComponents.getLocks();
        this.statementLocksFactory = perEditionComponents.getStatementLocksFactory();
        this.transactionEventListeners = globalModule.getTransactionEventListeners();
        this.globalMonitors = globalModule.getGlobalMonitors();
        this.fs = globalModule.getFileSystem();
        this.transactionStats = perEditionComponents.getTransactionMonitor();
        this.eventListeners = globalModule.getDatabaseEventListeners();
        this.databaseHealthFactory = () -> globalModule.getGlobalHealthService()
                .createDatabaseHealth( new DatabasePanicEventGenerator( eventListeners, databaseId.name() ),
                        logService.getInternalLog( DatabaseHealth.class ) );
        this.transactionHeaderInformationFactory = perEditionComponents.getHeaderInformationFactory();
        this.commitProcessFactory = perEditionComponents.getCommitProcessFactory();
        this.pageCache = globalModule.getPageCache();
        this.constraintSemantics = perEditionComponents.getConstraintSemantics();
        this.tracers = globalModule.getTracers();
        this.globalProcedures = globalProcedures;
        this.ioLimiter = perEditionComponents.getIoLimiter();
        this.clock = globalModule.getGlobalClock();
        this.accessCapability = perEditionComponents.getAccessCapability();
        this.storeCopyCheckPointMutex = new StoreCopyCheckPointMutex();
        this.databaseInfo = globalModule.getDatabaseInfo();
        this.versionContextSupplier = globalModule.getVersionContextSupplier();
        this.collectionsFactorySupplier = globalModule.getCollectionsFactorySupplier();
        this.extensionFactories = globalModule.getExtensionFactories();
        this.watcherServiceFactory = perEditionComponents.getWatcherServiceFactory();
        this.facade = facade;
        this.engineProviders = globalModule.getQueryEngineProviders();
        this.databaseAvailabilityGuardFactory = () -> globalModule.getGlobalAvailabilityGuard().createDatabaseAvailabilityGuard( databaseId );
        this.databaseMigratorFactory = new DatabaseMigratorFactory( fs, globalConfig, logService, pageCache, scheduler );
        this.storageEngineFactory = globalModule.getStorageEngineFactory();
    }

    @Override
    public DatabaseId getDatabaseId()
    {
        return databaseId;
    }

    @Override
    public DatabaseLayout getDatabaseLayout()
    {
        return databaseLayout;
    }

    @Override
    public Config getGlobalConfig()
    {
        return globalConfig;
    }

    @Override
    public DatabaseConfig getDatabaseConfig()
    {
        return databaseConfig;
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
    public GlobalTransactionEventListeners getTransactionEventListeners()
    {
        return transactionEventListeners;
    }

    @Override
    public FileSystemAbstraction getFs()
    {
        return fs;
    }

    @Override
    public DatabaseTransactionStats getTransactionStats()
    {
        return transactionStats;
    }

    @Override
    public Factory<DatabaseHealth> getDatabaseHealthFactory()
    {
        return databaseHealthFactory;
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
        return globalMonitors;
    }

    @Override
    public Tracers getTracers()
    {
        return tracers;
    }

    @Override
    public GlobalProcedures getGlobalProcedures()
    {
        return globalProcedures;
    }

    @Override
    public IOLimiter getIoLimiter()
    {
        return ioLimiter;
    }

    @Override
    public Factory<DatabaseAvailabilityGuard> getDatabaseAvailabilityGuardFactory()
    {
        return databaseAvailabilityGuardFactory;
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
    public Iterable<ExtensionFactory<?>> getExtensionFactories()
    {
        return extensionFactories;
    }

    @Override
    public Function<DatabaseLayout,DatabaseLayoutWatcher> getWatcherServiceFactory()
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
    public DatabaseEventListeners getDatabaseEventListeners()
    {
        return eventListeners;
    }

    @Override
    public DatabaseMigratorFactory getDatabaseMigratorFactory()
    {
        return databaseMigratorFactory;
    }

    @Override
    public StorageEngineFactory getStorageEngineFactory()
    {
        return storageEngineFactory;
    }
}
