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
package org.neo4j.kernel.database;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongFunction;
import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.DatabaseConfig;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.exceptions.KernelException;
import org.neo4j.function.Factory;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOController;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.impl.api.TransactionRegistry;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.store.StoreFileListing;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.DatabaseEventListeners;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.internal.DatabaseLogProvider;
import org.neo4j.logging.internal.DatabaseLogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreFileMetadata;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.token.TokenHolders;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.values.ElementIdMapper;

public abstract class AbstractDatabase extends LifecycleAdapter implements Lifecycle {

    private final Factory<DatabaseHealth> databaseHealthFactory;
    protected volatile boolean initialized;
    protected volatile boolean started;

    protected final DependencyResolver globalDependencies;
    protected final NamedDatabaseId namedDatabaseId;
    protected final DatabaseConfig databaseConfig;
    protected final DatabaseEventListeners eventListeners;
    protected final Monitors parentMonitors;
    protected final DatabaseLogService databaseLogService;
    protected final DatabaseLogProvider internalLogProvider;
    protected final SystemNanoClock clock;
    protected final DatabaseLogProvider userLogProvider;
    protected final InternalLog internalLog;
    protected final JobScheduler scheduler;
    protected final DatabaseAvailabilityGuard databaseAvailabilityGuard;

    protected DatabaseHealth databaseHealth;
    protected Dependencies databaseDependencies;
    protected LifeSupport life;
    protected Monitors databaseMonitors;

    protected AbstractDatabase(
            DependencyResolver globalDependencies,
            NamedDatabaseId namedDatabaseId,
            DatabaseConfig databaseConfig,
            DatabaseEventListeners eventListeners,
            Monitors monitors,
            DatabaseLogService databaseLogService,
            JobScheduler scheduler,
            LongFunction<DatabaseAvailabilityGuard> databaseAvailabilityGuardFactory,
            Factory<DatabaseHealth> databaseHealthFactory,
            SystemNanoClock clock) {
        this.globalDependencies = globalDependencies;
        this.namedDatabaseId = namedDatabaseId;
        this.databaseConfig = databaseConfig;
        this.eventListeners = eventListeners;
        this.parentMonitors = monitors;
        this.databaseLogService = databaseLogService;
        this.internalLogProvider = databaseLogService.getInternalLogProvider();
        this.clock = clock;
        this.internalLog = internalLogProvider.getLog(getClass());
        this.userLogProvider = databaseLogService.getUserLogProvider();
        this.scheduler = scheduler;
        long availabilityGuardTimeout = databaseConfig
                .get(GraphDatabaseInternalSettings.transaction_start_timeout)
                .toMillis();
        this.databaseAvailabilityGuard = databaseAvailabilityGuardFactory.apply(availabilityGuardTimeout);
        this.databaseHealthFactory = databaseHealthFactory;
        this.databaseHealthFactory.newInstance();
    }

    /**
     * Initialize the database, and bring it to a state where its version can be examined, and it can be
     * upgraded if necessary.
     */
    @Override
    public synchronized void init() {
        if (initialized) {
            return;
        }
        try {
            databaseDependencies = new Dependencies(globalDependencies);
            databaseMonitors = new Monitors(parentMonitors, internalLogProvider);
            life = new LifeSupport();
            databaseHealth = databaseHealthFactory.newInstance();

            databaseDependencies.satisfyDependency(this);
            databaseDependencies.satisfyDependency(databaseMonitors);
            databaseDependencies.satisfyDependency(databaseHealth);
            databaseDependencies.satisfyDependency(namedDatabaseId);
            databaseDependencies.satisfyDependency(databaseConfig);
            databaseDependencies.satisfyDependency(databaseLogService);
            databaseDependencies.satisfyDependency(databaseAvailabilityGuard);

            specificInit();

            eventListeners.databaseCreate(namedDatabaseId);
            initialized = true;

        } catch (Throwable e) {
            handleStartupFailure(e);
        }
    }

    /**
     * Start the database and make it ready for transaction processing.
     */
    @Override
    public synchronized void start() {
        if (started) {
            return;
        }
        init(); // Ensure we're initialized
        try {
            specificStart();

            life.start();
            eventListeners.databaseStart(namedDatabaseId);
            started = true;

            postStartupInit();
        } catch (Throwable e) {
            handleStartupFailure(e);
        }
    }

    @Override
    public synchronized void stop() {
        if (!started) {
            return;
        }
        specificStop();

        eventListeners.databaseShutdown(namedDatabaseId);
        life.stop();
        awaitAllClosingTransactions();
        life.shutdown();
        started = false;
        initialized = false;
    }

    @Override
    public synchronized void shutdown() throws Exception {
        specificShutdown();

        safeCleanup();
        started = false;
        initialized = false;
    }

    public synchronized void drop() {
        if (started) {
            prepareToDrop();
            stop();
        }
        deleteDatabaseFilesOnDrop();
        eventListeners.databaseDrop(namedDatabaseId);
    }

    protected void handleStartupFailure(Throwable e) {
        // Something unexpected happened during startup
        databaseAvailabilityGuard.startupFailure(e);
        internalLog.warn(
                "Exception occurred while starting the database. Trying to stop already started components.", e);
        try {
            shutdown();
        } catch (Exception closeException) {
            internalLog.error("Couldn't close database after startup failure", closeException);
        }
        throw new RuntimeException(e);
    }

    protected void awaitAllClosingTransactions() {
        internalLog.info("Waiting for closing transactions.");

        var transactionRegistry = transactionRegistry();
        transactionRegistry.terminateTransactions();

        var maxWaitTime = databaseConfig.get(GraphDatabaseSettings.shutdown_transaction_end_timeout);
        var endTime = clock.millis() + maxWaitTime.toMillis();
        boolean haveClosingTransactions;
        while ((haveClosingTransactions = transactionRegistry.haveClosingTransaction()) && clock.millis() < endTime) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
        }

        var numClosingTransactions = haveClosingTransactions
                ? transactionRegistry.executingTransactions().stream()
                        .filter(KernelTransactionHandle::isClosing)
                        .count()
                : 0;
        if (numClosingTransactions > 0) {
            internalLog.info(
                    "There are still %d transactions closing after the max wait time of %s elapsed. "
                            + "This may result in a need for recovery on next start",
                    numClosingTransactions, maxWaitTime);
        } else {
            internalLog.info("All transactions are closed.");
        }
    }

    public Config getConfig() {
        return databaseConfig;
    }

    public DatabaseLogService getLogService() {
        return databaseLogService;
    }

    public DatabaseLogProvider getInternalLogProvider() {
        return internalLogProvider;
    }

    public Monitors getMonitors() {
        return databaseMonitors;
    }

    public DependencyResolver getDependencyResolver() {
        return databaseDependencies;
    }

    public NamedDatabaseId getNamedDatabaseId() {
        return namedDatabaseId;
    }

    @VisibleForTesting
    public LifeSupport getLife() {
        return life;
    }

    public boolean isStarted() {
        return started;
    }

    public DatabaseAvailabilityGuard getDatabaseAvailabilityGuard() {
        return databaseAvailabilityGuard;
    }

    public DatabaseHealth getDatabaseHealth() {
        return databaseHealth;
    }

    public abstract StoreId getStoreId();

    public abstract DatabaseLayout getDatabaseLayout();

    public abstract QueryExecutionEngine getExecutionEngine();

    public abstract Kernel getKernel();

    public abstract ResourceIterator<StoreFileMetadata> listStoreFiles(boolean includeLogs) throws IOException;

    public abstract StoreFileListing getStoreFileListing();

    public abstract JobScheduler getScheduler();

    public abstract StoreCopyCheckPointMutex getStoreCopyCheckPointMutex();

    public abstract TokenHolders getTokenHolders();

    public abstract GraphDatabaseAPI getDatabaseAPI();

    public abstract DatabaseTracers getTracers();

    public abstract MemoryTracker getOtherDatabaseMemoryTracker();

    public abstract StorageEngineFactory getStorageEngineFactory();

    public abstract IOController getIoController();

    public abstract CursorContextFactory getCursorContextFactory();

    public abstract ElementIdMapper getElementIdMapper();

    public abstract boolean isSystem();

    public abstract void prepareToDrop();

    protected abstract void specificInit() throws Exception;

    protected abstract void specificStart() throws Exception;

    protected abstract void specificStop();

    protected abstract void specificShutdown() throws Exception;

    protected abstract TransactionRegistry transactionRegistry();

    protected abstract void postStartupInit() throws KernelException;

    protected abstract void safeCleanup() throws Exception;

    protected abstract void deleteDatabaseFilesOnDrop();
}
