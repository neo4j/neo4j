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
package org.neo4j.graphdb.factory.module.edition;

import java.util.function.Function;
import java.util.function.Predicate;

import org.neo4j.annotations.api.IgnoreApiCheck;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.DbmsRuntimeRepository;
import org.neo4j.dbms.database.DbmsRuntimeSystemGraphComponent;
import org.neo4j.dbms.database.SystemGraphComponents;
import org.neo4j.dbms.database.SystemGraphInitializer;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.context.EditionDatabaseComponents;
import org.neo4j.internal.collector.DataCollectorProcedures;
import org.neo4j.io.fs.watcher.DatabaseLayoutWatcher;
import org.neo4j.io.fs.watcher.FileWatcher;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.api.net.DefaultNetworkConnectionTracker;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.provider.SecurityProvider;
import org.neo4j.kernel.database.DatabaseStartupController;
import org.neo4j.kernel.database.DefaultDatabaseResolver;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats;
import org.neo4j.kernel.impl.util.watcher.DefaultFileDeletionListenerFactory;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.procedure.builtin.BuiltInDbmsProcedures;
import org.neo4j.procedure.builtin.BuiltInProcedures;
import org.neo4j.procedure.builtin.FulltextProcedures;
import org.neo4j.procedure.builtin.TokenProcedures;
import org.neo4j.procedure.builtin.routing.BaseRoutingProcedureInstaller;
import org.neo4j.procedure.impl.ProcedureConfig;
import org.neo4j.time.SystemNanoClock;

import static org.neo4j.procedure.impl.temporal.TemporalFunction.registerTemporalFunctions;

/**
 * Edition module for {@link DatabaseManagementServiceFactory}. Implementations of this class
 * need to create all the services that would be specific for a particular edition of the database.
 */
@IgnoreApiCheck
public abstract class AbstractEditionModule
{
    protected NetworkConnectionTracker connectionTracker;
    protected ConstraintSemantics constraintSemantics;
    protected IOLimiter ioLimiter;
    protected Function<DatabaseLayout,DatabaseLayoutWatcher> watcherServiceFactory;
    protected SecurityProvider securityProvider;
    protected DefaultDatabaseResolver defaultDatabaseResolver;

    public abstract EditionDatabaseComponents createDatabaseComponents( NamedDatabaseId namedDatabaseId );

    protected DatabaseLayoutWatcher createDatabaseFileSystemWatcher( FileWatcher watcher, DatabaseLayout databaseLayout, LogService logging,
            Predicate<String> fileNameFilter )
    {
        DefaultFileDeletionListenerFactory listenerFactory =
                new DefaultFileDeletionListenerFactory( databaseLayout, logging, fileNameFilter );
        return new DatabaseLayoutWatcher( watcher, databaseLayout, listenerFactory );
    }

    public void registerProcedures( GlobalProcedures globalProcedures, ProcedureConfig procedureConfig, GlobalModule globalModule,
            DatabaseManager<?> databaseManager ) throws KernelException
    {
        globalProcedures.registerProcedure( BuiltInProcedures.class );
        globalProcedures.registerProcedure( TokenProcedures.class );
        globalProcedures.registerProcedure( BuiltInDbmsProcedures.class );
        globalProcedures.registerProcedure( FulltextProcedures.class );
        globalProcedures.registerProcedure( DataCollectorProcedures.class );
        registerTemporalFunctions( globalProcedures, procedureConfig );

        registerEditionSpecificProcedures( globalProcedures, databaseManager );
        BaseRoutingProcedureInstaller routingProcedureInstaller = createRoutingProcedureInstaller( globalModule, databaseManager );
        routingProcedureInstaller.install( globalProcedures );
    }

    protected abstract void registerEditionSpecificProcedures( GlobalProcedures globalProcedures, DatabaseManager<?> databaseManager )
            throws KernelException;

    protected abstract BaseRoutingProcedureInstaller createRoutingProcedureInstaller( GlobalModule globalModule, DatabaseManager<?> databaseManager );

    public abstract <DB extends DatabaseContext> DatabaseManager<DB> createDatabaseManager( GlobalModule globalModule );

    public abstract SystemGraphInitializer createSystemGraphInitializer( GlobalModule globalModule );

    public abstract void registerSystemGraphComponents( SystemGraphComponents systemGraphComponents, GlobalModule globalModule );

    public abstract void createSecurityModule( GlobalModule globalModule );

    protected NetworkConnectionTracker createConnectionTracker()
    {
        return new DefaultNetworkConnectionTracker();
    }

    public DatabaseTransactionStats createTransactionMonitor()
    {
        return new DatabaseTransactionStats();
    }

    public ConstraintSemantics getConstraintSemantics()
    {
        return constraintSemantics;
    }

    public IOLimiter getIoLimiter()
    {
        return ioLimiter;
    }

    public Function<DatabaseLayout,DatabaseLayoutWatcher> getWatcherServiceFactory()
    {
        return watcherServiceFactory;
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

    public abstract void createDefaultDatabaseResolver( GlobalModule globalModule );

    public void setDefaultDatabaseResolver( DefaultDatabaseResolver defaultDatabaseResolver )
    {
        this.defaultDatabaseResolver = defaultDatabaseResolver;
    }

    public DefaultDatabaseResolver getDefaultDatabaseResolver()
    {
        return defaultDatabaseResolver;
    }

    /**
     * @return the query engine provider for this edition.
     */
    public abstract QueryEngineProvider getQueryEngineProvider();

    public abstract void bootstrapFabricServices();

    public abstract BoltGraphDatabaseManagementServiceSPI createBoltDatabaseManagementServiceProvider( Dependencies dependencies,
            DatabaseManagementService managementService, Monitors monitors, SystemNanoClock clock, LogService logService );

    public AuthManager getBoltAuthManager( DependencyResolver dependencyResolver )
    {
        return dependencyResolver.resolveDependency( AuthManager.class );
    }

    public AuthManager getBoltInClusterAuthManager()
    {
        return securityProvider.inClusterAuthManager();
    }

    public abstract DatabaseStartupController getDatabaseStartupController();

    public abstract Lifecycle createWebServer( DatabaseManagementService managementService, Dependencies globalDependencies,
            Config config, LogProvider userLogProvider, DbmsInfo dbmsInfo );

    public abstract DbmsRuntimeRepository createAndRegisterDbmsRuntimeRepository( GlobalModule globalModule, DatabaseManager<?> databaseManager,
            Dependencies dependencies, DbmsRuntimeSystemGraphComponent dbmsRuntimeSystemGraphComponent );
}
