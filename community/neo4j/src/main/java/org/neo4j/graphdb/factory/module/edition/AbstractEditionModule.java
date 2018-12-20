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
package org.neo4j.graphdb.factory.module.edition;

import java.time.Clock;
import java.util.function.Function;
import java.util.function.Predicate;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dmbs.database.DefaultDatabaseManager;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.graphdb.factory.module.edition.context.EditionDatabaseContext;
import org.neo4j.helpers.Service;
import org.neo4j.io.fs.watcher.DatabaseLayoutWatcher;
import org.neo4j.io.fs.watcher.FileWatcher;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.api.security.SecurityModule;
import org.neo4j.kernel.api.security.provider.SecurityProvider;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.factory.AccessCapability;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.proc.ProcedureConfig;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats;
import org.neo4j.kernel.impl.transaction.stats.TransactionCounters;
import org.neo4j.kernel.impl.util.watcher.DefaultFileDeletionListenerFactory;
import org.neo4j.logging.Log;
import org.neo4j.logging.Logger;
import org.neo4j.logging.internal.LogService;
import org.neo4j.udc.UsageData;
import org.neo4j.udc.UsageDataKeys;

import static org.neo4j.kernel.impl.proc.temporal.TemporalFunction.registerTemporalFunctions;

/**
 * Edition module for {@link GraphDatabaseFacadeFactory}. Implementations of this class
 * need to create all the services that would be specific for a particular edition of the database.
 */
public abstract class AbstractEditionModule
{
    private final DatabaseTransactionStats databaseStatistics = new DatabaseTransactionStats();
    protected NetworkConnectionTracker connectionTracker;
    protected ThreadToStatementContextBridge threadToTransactionBridge;
    protected long transactionStartTimeout;
    protected TransactionHeaderInformationFactory headerInformationFactory;
    protected SchemaWriteGuard schemaWriteGuard;
    protected ConstraintSemantics constraintSemantics;
    protected AccessCapability accessCapability;
    protected IOLimiter ioLimiter;
    protected Function<DatabaseLayout,DatabaseLayoutWatcher> watcherServiceFactory;
    protected AvailabilityGuard globalAvailabilityGuard;
    protected SecurityProvider securityProvider;

    public abstract EditionDatabaseContext createDatabaseContext( String databaseName );

    protected DatabaseLayoutWatcher createDatabaseFileSystemWatcher( FileWatcher watcher, DatabaseLayout databaseLayout, LogService logging,
            Predicate<String> fileNameFilter )
    {
        DefaultFileDeletionListenerFactory listenerFactory =
                new DefaultFileDeletionListenerFactory( databaseLayout.getDatabaseName(), logging, fileNameFilter );
        return new DatabaseLayoutWatcher( watcher, databaseLayout, listenerFactory );
    }

    public void registerProcedures( Procedures procedures, ProcedureConfig procedureConfig ) throws KernelException
    {
        procedures.registerProcedure( org.neo4j.kernel.builtinprocs.BuiltInProcedures.class );
        procedures.registerProcedure( org.neo4j.kernel.builtinprocs.TokenProcedures.class );
        procedures.registerProcedure( org.neo4j.kernel.builtinprocs.BuiltInDbmsProcedures.class );
        procedures.registerBuiltInFunctions( org.neo4j.kernel.builtinprocs.BuiltInFunctions.class );
        registerTemporalFunctions( procedures, procedureConfig );

        registerEditionSpecificProcedures( procedures );
    }

    protected abstract void registerEditionSpecificProcedures( Procedures procedures ) throws KernelException;

    protected void publishEditionInfo( UsageData sysInfo, DatabaseInfo databaseInfo, Config config )
    {
        sysInfo.set( UsageDataKeys.edition, databaseInfo.edition );
        sysInfo.set( UsageDataKeys.operationalMode, databaseInfo.operationalMode );
        config.augment( GraphDatabaseSettings.editionName, databaseInfo.edition.toString() );
    }

    public DatabaseManager createDatabaseManager( GraphDatabaseFacade graphDatabaseFacade, PlatformModule platform, AbstractEditionModule edition,
            Procedures procedures, Logger msgLog )
    {
        return new DefaultDatabaseManager( platform, edition, procedures, msgLog, graphDatabaseFacade );
    }

    /**
     * Returns {@code false} because {@link DatabaseManager}'s lifecycle is not managed by any component by default.
     * So {@link DatabaseManager} needs to be included in the global lifecycle.
     *
     * @return always {@code false}.
     */
    public boolean handlesDatabaseManagerLifecycle()
    {
        return false;
    }

    public abstract void createSecurityModule( PlatformModule platformModule, Procedures procedures );

    protected static SecurityModule setupSecurityModule( PlatformModule platformModule, AbstractEditionModule editionModule, Log log, Procedures procedures,
            String key )
    {
        SecurityModule.Dependencies securityModuleDependencies = new SecurityModuleDependencies( platformModule, editionModule, procedures );
        SecurityModule securityModule = Service.loadSilently( SecurityModule.class, key );
        if ( securityModule == null )
        {
            String errorMessage = "Failed to load security module with key '" + key + "'.";
            log.error( errorMessage );
            throw new IllegalArgumentException( errorMessage );
        }
        try
        {
            securityModule.setup( securityModuleDependencies );
            return securityModule;
        }
        catch ( Exception e )
        {
            String errorMessage = "Failed to load security module.";
            log.error( errorMessage );
            throw new RuntimeException( errorMessage, e );
        }
    }

    protected NetworkConnectionTracker createConnectionTracker()
    {
        return NetworkConnectionTracker.NO_OP;
    }

    public DatabaseTransactionStats createTransactionMonitor()
    {
        return databaseStatistics;
    }

    public TransactionCounters globalTransactionCounter()
    {
        return databaseStatistics;
    }

    public AvailabilityGuard getGlobalAvailabilityGuard( Clock clock, LogService logService, Config config )
    {
        if ( globalAvailabilityGuard == null )
        {
            globalAvailabilityGuard = new DatabaseAvailabilityGuard( config.get( GraphDatabaseSettings.active_database ), clock,
                    logService.getInternalLog( DatabaseAvailabilityGuard.class ) );
        }
        return globalAvailabilityGuard;
    }

    public DatabaseAvailabilityGuard createDatabaseAvailabilityGuard( String databaseName, Clock clock, LogService logService, Config config )
    {
        return (DatabaseAvailabilityGuard) getGlobalAvailabilityGuard( clock, logService, config );
    }

    public void createDatabases( DatabaseManager databaseManager, Config config )
    {
        databaseManager.createDatabase( config.get( GraphDatabaseSettings.active_database ) );
    }

    public long getTransactionStartTimeout()
    {
        return transactionStartTimeout;
    }

    public SchemaWriteGuard getSchemaWriteGuard()
    {
        return schemaWriteGuard;
    }

    public TransactionHeaderInformationFactory getHeaderInformationFactory()
    {
        return headerInformationFactory;
    }

    public ConstraintSemantics getConstraintSemantics()
    {
        return constraintSemantics;
    }

    public IOLimiter getIoLimiter()
    {
        return ioLimiter;
    }

    public AccessCapability getAccessCapability()
    {
        return accessCapability;
    }

    public Function<DatabaseLayout,DatabaseLayoutWatcher> getWatcherServiceFactory()
    {
        return watcherServiceFactory;
    }

    public ThreadToStatementContextBridge getThreadToTransactionBridge()
    {
        return threadToTransactionBridge;
    }

    public NetworkConnectionTracker getConnectionTracker()
    {
        return connectionTracker;
    }

    public SecurityProvider getSecurityProvider()
    {
        return securityProvider;
    }

    public void setSecurityProvider( SecurityProvider securityProvider )
    {
        this.securityProvider = securityProvider;
    }
}
