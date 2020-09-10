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

import java.util.function.Function;
import java.util.function.LongFunction;

import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
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
import org.neo4j.kernel.database.DatabaseStartupController;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.LeaseService;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.factory.AccessCapabilityFactory;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats;
import org.neo4j.kernel.impl.util.collection.CollectionsFactorySupplier;
import org.neo4j.kernel.internal.event.GlobalTransactionEventListeners;
import org.neo4j.kernel.internal.locker.FileLockerService;
import org.neo4j.kernel.monitoring.DatabaseEventListeners;
import org.neo4j.kernel.monitoring.DatabasePanicEventGenerator;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.DatabaseLogService;
import org.neo4j.memory.GlobalMemoryGroupTracker;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.token.TokenHolders;

public class ModularDatabaseCreationContext implements DatabaseCreationContext
{
    private final NamedDatabaseId namedDatabaseId;
    private final Config globalConfig;
    private final DatabaseConfig databaseConfig;
    private final QueryEngineProvider queryEngineProvider;
    private final IdGeneratorFactory idGeneratorFactory;
    private final DatabaseLogService databaseLogService;
    private final JobScheduler scheduler;
    private final DependencyResolver globalDependencies;
    private final TokenHolders tokenHolders;
    private final Locks locks;
    private final StatementLocksFactory statementLocksFactory;
    private final FileSystemAbstraction fs;
    private final DatabaseTransactionStats transactionStats;
    private final Factory<DatabaseHealth> databaseHealthFactory;
    private final CommitProcessFactory commitProcessFactory;
    private final PageCache pageCache;
    private final ConstraintSemantics constraintSemantics;
    private final Monitors parentMonitors;
    private final Tracers tracers;
    private final GlobalProcedures globalProcedures;
    private final IOLimiter ioLimiter;
    private final LongFunction<DatabaseAvailabilityGuard> databaseAvailabilityGuardFactory;
    private final SystemNanoClock clock;
    private final StoreCopyCheckPointMutex storeCopyCheckPointMutex;
    private final IdController idController;
    private final DbmsInfo dbmsInfo;
    private final VersionContextSupplier versionContextSupplier;
    private final CollectionsFactorySupplier collectionsFactorySupplier;
    private final Iterable<ExtensionFactory<?>> extensionFactories;
    private final Function<DatabaseLayout,DatabaseLayoutWatcher> watcherServiceFactory;
    private final DatabaseLayout databaseLayout;
    private final DatabaseEventListeners eventListeners;
    private final GlobalTransactionEventListeners transactionEventListeners;
    private final StorageEngineFactory storageEngineFactory;
    private final FileLockerService fileLockerService;
    private final AccessCapabilityFactory accessCapabilityFactory;
    private final LeaseService leaseService;
    private final DatabaseStartupController startupController;
    private final GlobalMemoryGroupTracker transactionsMemoryPool;
    private final GlobalMemoryGroupTracker otherMemoryPool;

    public ModularDatabaseCreationContext( NamedDatabaseId namedDatabaseId, GlobalModule globalModule, Dependencies globalDependencies,
                                           Monitors parentMonitors, EditionDatabaseComponents editionComponents, GlobalProcedures globalProcedures,
                                           VersionContextSupplier versionContextSupplier, DatabaseConfig databaseConfig, LeaseService leaseService )
    {
        this.namedDatabaseId = namedDatabaseId;
        this.globalConfig = globalModule.getGlobalConfig();
        this.databaseConfig = databaseConfig;
        this.versionContextSupplier = versionContextSupplier;
        this.queryEngineProvider = editionComponents.getQueryEngineProvider();
        DatabaseIdContext idContext = editionComponents.getIdContext();
        this.idGeneratorFactory = idContext.getIdGeneratorFactory();
        this.idController = idContext.getIdController();
        this.transactionsMemoryPool = globalModule.getTransactionsMemoryPool();
        this.otherMemoryPool = globalModule.getOtherMemoryPool();
        this.databaseLayout = globalModule.getNeo4jLayout().databaseLayout( namedDatabaseId.name() );
        this.databaseLogService = new DatabaseLogService( namedDatabaseId, globalModule.getLogService() );
        this.scheduler = globalModule.getJobScheduler();
        this.globalDependencies = globalDependencies;
        this.tokenHolders = editionComponents.getTokenHolders();
        this.locks = editionComponents.getLocks();
        this.statementLocksFactory = editionComponents.getStatementLocksFactory();
        this.transactionEventListeners = globalModule.getTransactionEventListeners();
        this.parentMonitors = parentMonitors;
        this.fs = globalModule.getFileSystem();
        this.transactionStats = editionComponents.getTransactionMonitor();
        this.eventListeners = globalModule.getDatabaseEventListeners();
        this.databaseHealthFactory = () -> new DatabaseHealth( new DatabasePanicEventGenerator( eventListeners, namedDatabaseId ),
                        databaseLogService.getInternalLog( DatabaseHealth.class ) );
        this.commitProcessFactory = editionComponents.getCommitProcessFactory();
        this.pageCache = globalModule.getPageCache();
        this.constraintSemantics = editionComponents.getConstraintSemantics();
        this.tracers = globalModule.getTracers();
        this.globalProcedures = globalProcedures;
        this.ioLimiter = editionComponents.getIoLimiter();
        this.clock = globalModule.getGlobalClock();
        this.storeCopyCheckPointMutex = new StoreCopyCheckPointMutex();
        this.dbmsInfo = globalModule.getDbmsInfo();
        this.collectionsFactorySupplier = globalModule.getCollectionsFactorySupplier();
        this.extensionFactories = globalModule.getExtensionFactories();
        this.watcherServiceFactory = editionComponents.getWatcherServiceFactory();
        this.databaseAvailabilityGuardFactory =
                databaseTimeoutMillis -> databaseAvailabilityGuardFactory( namedDatabaseId, globalModule, databaseTimeoutMillis );
        this.storageEngineFactory = globalModule.getStorageEngineFactory();
        this.fileLockerService = globalModule.getFileLockerService();
        this.accessCapabilityFactory = editionComponents.getAccessCapabilityFactory();
        this.leaseService = leaseService;
        this.startupController = editionComponents.getStartupController();
    }

    @Override
    public NamedDatabaseId getNamedDatabaseId()
    {
        return namedDatabaseId;
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
    public DatabaseLogService getDatabaseLogService()
    {
        return databaseLogService;
    }

    @Override
    public JobScheduler getScheduler()
    {
        return scheduler;
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
        return parentMonitors;
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
    public LongFunction<DatabaseAvailabilityGuard> getDatabaseAvailabilityGuardFactory()
    {
        return databaseAvailabilityGuardFactory;
    }

    @Override
    public SystemNanoClock getClock()
    {
        return clock;
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
    public DbmsInfo getDbmsInfo()
    {
        return dbmsInfo;
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
    public QueryEngineProvider getEngineProvider()
    {
        return queryEngineProvider;
    }

    @Override
    public DatabaseEventListeners getDatabaseEventListeners()
    {
        return eventListeners;
    }

    @Override
    public StorageEngineFactory getStorageEngineFactory()
    {
        return storageEngineFactory;
    }

    @Override
    public FileLockerService getFileLockerService()
    {
        return fileLockerService;
    }

    @Override
    public AccessCapabilityFactory getAccessCapabilityFactory()
    {
        return accessCapabilityFactory;
    }

    @Override
    public LeaseService getLeaseService()
    {
        return leaseService;
    }

    @Override
    public DatabaseStartupController getStartupController()
    {
        return startupController;
    }

    @Override
    public GlobalMemoryGroupTracker getTransactionsMemoryPool()
    {
        return transactionsMemoryPool;
    }

    @Override
    public GlobalMemoryGroupTracker getOtherMemoryPool()
    {
        return otherMemoryPool;
    }

    private DatabaseAvailabilityGuard databaseAvailabilityGuardFactory( NamedDatabaseId namedDatabaseId, GlobalModule globalModule, long databaseTimeoutMillis )
    {
        Log guardLog = databaseLogService.getInternalLog( DatabaseAvailabilityGuard.class );
        return new DatabaseAvailabilityGuard( namedDatabaseId, clock, guardLog, databaseTimeoutMillis, globalModule.getGlobalAvailabilityGuard() );
    }
}
