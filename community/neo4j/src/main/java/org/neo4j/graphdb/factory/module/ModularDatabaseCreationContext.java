/*
 * Copyright (c) "Neo4j"
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
import java.util.function.Predicate;

import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.DatabaseConfig;
import org.neo4j.dbms.database.readonly.ReadOnlyDatabases;
import org.neo4j.function.Factory;
import org.neo4j.function.Predicates;
import org.neo4j.graphdb.factory.module.id.DatabaseIdContext;
import org.neo4j.internal.id.BufferingIdGeneratorFactory;
import org.neo4j.internal.id.IdController;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.watcher.DatabaseLayoutWatcher;
import org.neo4j.io.fs.watcher.FileWatcher;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.database.DatabaseCreationContext;
import org.neo4j.kernel.database.DatabaseStartupController;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.ExternalIdReuseConditionProvider;
import org.neo4j.kernel.impl.api.LeaseService;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.factory.AccessCapabilityFactory;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.pagecache.IOControllerService;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats;
import org.neo4j.kernel.impl.util.collection.CollectionsFactorySupplier;
import org.neo4j.kernel.impl.util.watcher.DefaultFileDeletionListenerFactory;
import org.neo4j.kernel.internal.event.GlobalTransactionEventListeners;
import org.neo4j.kernel.internal.locker.FileLockerService;
import org.neo4j.kernel.monitoring.DatabaseEventListeners;
import org.neo4j.kernel.monitoring.DatabasePanicEventGenerator;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.internal.DatabaseLogService;
import org.neo4j.logging.internal.LogService;
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
    private final ExternalIdReuseConditionProvider externalIdReuseConditionProvider;
    private final IdGeneratorFactory idGeneratorFactory;
    private final DatabaseLogService databaseLogService;
    private final JobScheduler scheduler;
    private final DependencyResolver globalDependencies;
    private final TokenHolders tokenHolders;
    private final Locks locks;
    private final FileSystemAbstraction fs;
    private final DatabaseTransactionStats transactionStats;
    private final Factory<DatabaseHealth> databaseHealthFactory;
    private final CommitProcessFactory commitProcessFactory;
    private final PageCache pageCache;
    private final ConstraintSemantics constraintSemantics;
    private final Monitors parentMonitors;
    private final Tracers tracers;
    private final GlobalProcedures globalProcedures;
    private final IOControllerService ioControllerService;
    private final LongFunction<DatabaseAvailabilityGuard> databaseAvailabilityGuardFactory;
    private final SystemNanoClock clock;
    private final StoreCopyCheckPointMutex storeCopyCheckPointMutex;
    private final IdController idController;
    private final DbmsInfo dbmsInfo;
    private final CursorContextFactory contextFactory;
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
    private final ReadOnlyDatabases readOnlyDatabases;

    public ModularDatabaseCreationContext( NamedDatabaseId namedDatabaseId,
                                           GlobalModule globalModule,
                                           Dependencies globalDependencies,
                                           DatabaseConfig databaseConfig,
                                           CursorContextFactory contextFactory,
                                           ConstraintSemantics constraintSemantics,
                                           QueryEngineProvider queryEngineProvider,
                                           DatabaseTransactionStats transactionStats,
                                           Predicate<String> databaseFileFilter,
                                           AccessCapabilityFactory accessCapabilityFactory,
                                           ExternalIdReuseConditionProvider externalIdReuseConditionProvider,
                                           Locks locks,
                                           DatabaseIdContext databaseIdContext,
                                           CommitProcessFactory commitProcessFactory,
                                           TokenHolders tokenHolders,
                                           DatabaseStartupController databaseStartupController,
                                           ReadOnlyDatabases readOnlyDatabases )
    {
        this( namedDatabaseId,
              globalModule,
              globalDependencies,
              contextFactory,
              databaseConfig,
              globalModule.getGlobalMonitors(),
              LeaseService.NO_LEASES,
              DatabaseCreationContext.selectStorageEngine( globalModule.getFileSystem(),
                                                           globalModule.getNeo4jLayout(),
                                                           globalModule.getPageCache(),
                                                           databaseConfig,
                                                           namedDatabaseId ),
              constraintSemantics,
              queryEngineProvider,
              transactionStats,
              databaseFileFilter,
              accessCapabilityFactory,
              externalIdReuseConditionProvider,
              locks,
              databaseIdContext,
              commitProcessFactory,
              tokenHolders,
              databaseStartupController,
              readOnlyDatabases );
    }

    public ModularDatabaseCreationContext( NamedDatabaseId namedDatabaseId,
                                           GlobalModule globalModule,
                                           Dependencies globalDependencies,
                                           CursorContextFactory contextFactory,
                                           DatabaseConfig databaseConfig,
                                           Monitors parentMonitors,
                                           LeaseService leaseService,
                                           StorageEngineFactory storageEngineFactory,
                                           ConstraintSemantics constraintSemantics,
                                           QueryEngineProvider queryEngineProvider,
                                           DatabaseTransactionStats transactionStats,
                                           Predicate<String> databaseFileFilter,
                                           AccessCapabilityFactory accessCapabilityFactory,
                                           ExternalIdReuseConditionProvider externalIdReuseConditionProvider,
                                           Locks locks,
                                           DatabaseIdContext databaseIdContext,
                                           CommitProcessFactory commitProcessFactory,
                                           TokenHolders tokenHolders,
                                           DatabaseStartupController databaseStartupController,
                                           ReadOnlyDatabases readOnlyDatabases )
    {
        this.namedDatabaseId = namedDatabaseId;
        this.globalConfig = globalModule.getGlobalConfig();
        this.databaseConfig = databaseConfig;
        this.contextFactory = contextFactory;
        this.queryEngineProvider = queryEngineProvider;
        this.externalIdReuseConditionProvider = externalIdReuseConditionProvider;
        this.idGeneratorFactory = databaseIdContext.getIdGeneratorFactory();
        this.idController = databaseIdContext.getIdController();
        this.transactionsMemoryPool = globalModule.getTransactionsMemoryPool();
        this.otherMemoryPool = globalModule.getOtherMemoryPool();
        this.databaseLogService = new DatabaseLogService( namedDatabaseId, globalModule.getLogService() );
        this.scheduler = globalModule.getJobScheduler();
        this.globalDependencies = globalDependencies;
        this.tokenHolders = tokenHolders;
        this.locks = locks;
        this.transactionEventListeners = globalModule.getTransactionEventListeners();
        this.parentMonitors = parentMonitors;
        this.fs = globalModule.getFileSystem();
        this.transactionStats = transactionStats;
        this.eventListeners = globalModule.getDatabaseEventListeners();
        this.databaseHealthFactory = () -> new DatabaseHealth( new DatabasePanicEventGenerator( eventListeners, namedDatabaseId ),
                                                               databaseLogService.getInternalLog( DatabaseHealth.class ) );
        this.commitProcessFactory = commitProcessFactory;
        this.pageCache = globalModule.getPageCache();
        this.constraintSemantics = constraintSemantics;
        this.tracers = globalModule.getTracers();
        this.globalProcedures = globalDependencies.resolveDependency( GlobalProcedures.class );
        this.ioControllerService = globalModule.getIoControllerService();
        this.clock = globalModule.getGlobalClock();
        this.storeCopyCheckPointMutex = new StoreCopyCheckPointMutex();
        this.dbmsInfo = globalModule.getDbmsInfo();
        this.collectionsFactorySupplier = globalModule.getCollectionsFactorySupplier();
        this.extensionFactories = globalModule.getExtensionFactories();
        this.watcherServiceFactory =
                databaseLayout -> createDatabaseFileSystemWatcher( globalModule.getFileWatcher(), databaseLayout, globalModule.getLogService(),
                                                                   databaseFileFilter );
        this.databaseAvailabilityGuardFactory =
                databaseTimeoutMillis -> databaseAvailabilityGuardFactory( namedDatabaseId, globalModule, databaseTimeoutMillis );
        Neo4jLayout neo4jLayout = globalModule.getNeo4jLayout();
        this.storageEngineFactory = storageEngineFactory;
        this.databaseLayout = storageEngineFactory.databaseLayout( neo4jLayout, namedDatabaseId.name() );
        this.fileLockerService = globalModule.getFileLockerService();
        this.accessCapabilityFactory = accessCapabilityFactory;
        this.leaseService = leaseService;
        this.startupController = databaseStartupController;
        this.readOnlyDatabases = readOnlyDatabases;
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
    public IOControllerService getIoControllerService()
    {
        return ioControllerService;
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

    @Override
    public ReadOnlyDatabases getDbmsReadOnlyChecker()
    {
        return readOnlyDatabases;
    }

    @Override
    public CursorContextFactory getContextFactory()
    {
        return contextFactory;
    }

    @Override
    public ExternalIdReuseConditionProvider externalIdReuseConditionProvider()
    {
        return externalIdReuseConditionProvider;
    }

    private DatabaseAvailabilityGuard databaseAvailabilityGuardFactory( NamedDatabaseId namedDatabaseId, GlobalModule globalModule, long databaseTimeoutMillis )
    {
        InternalLog guardLog = databaseLogService.getInternalLog( DatabaseAvailabilityGuard.class );
        return new DatabaseAvailabilityGuard( namedDatabaseId, clock, guardLog, databaseTimeoutMillis, globalModule.getGlobalAvailabilityGuard() );
    }

    private static DatabaseLayoutWatcher createDatabaseFileSystemWatcher( FileWatcher watcher, DatabaseLayout databaseLayout, LogService logging,
                                                                          Predicate<String> fileNameFilter )
    {
        var listenerFactory = new DefaultFileDeletionListenerFactory( databaseLayout, logging, fileNameFilter );
        return new DatabaseLayoutWatcher( watcher, databaseLayout, listenerFactory );
    }

    public static Predicate<String> defaultFileWatcherFilter()
    {
        return Predicates.any( TransactionLogFilesHelper.DEFAULT_FILENAME_PREDICATE, BufferingIdGeneratorFactory.PAGED_ID_BUFFER_FILE_NAME_FILTER );
    }
}
