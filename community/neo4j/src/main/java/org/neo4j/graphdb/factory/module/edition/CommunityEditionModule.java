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
package org.neo4j.graphdb.factory.module.edition;

import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.dbapi.impl.BoltKernelDatabaseManagementServiceProvider;
import org.neo4j.bolt.txtracking.SimpleReconciledTransactionTracker;
import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.cypher.internal.javacompat.CommunityCypherEngineProvider;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.DefaultDatabaseManager;
import org.neo4j.dbms.database.DefaultSystemGraphInitializer;
import org.neo4j.dbms.database.SystemGraphInitializer;
import org.neo4j.dbms.procedures.StandaloneDatabaseStateProcedure;
import org.neo4j.exceptions.KernelException;
import org.neo4j.exceptions.UnsatisfiedDependencyException;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.id.IdContextFactory;
import org.neo4j.graphdb.factory.module.id.IdContextFactoryBuilder;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.security.SecurityModule;
import org.neo4j.kernel.api.security.provider.NoAuthSecurityProvider;
import org.neo4j.kernel.availability.CompositeDatabaseAvailabilityGuard;
import org.neo4j.kernel.database.DatabaseStartupController;
import org.neo4j.kernel.database.GlobalAvailabilityGuardController;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.core.DefaultLabelIdCreator;
import org.neo4j.kernel.impl.core.DefaultPropertyTokenCreator;
import org.neo4j.kernel.impl.core.DefaultRelationshipTypeCreator;
import org.neo4j.kernel.impl.factory.CommunityCommitProcessFactory;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.LocksFactory;
import org.neo4j.kernel.impl.locking.SimpleStatementLocksFactory;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.procedure.builtin.routing.BaseRoutingProcedureInstaller;
import org.neo4j.procedure.builtin.routing.SingleInstanceRoutingProcedureInstaller;
import org.neo4j.server.CommunityNeoWebServer;
import org.neo4j.server.security.auth.CommunitySecurityModule;
import org.neo4j.ssl.config.SslPolicyLoader;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.token.DelegatingTokenHolder;
import org.neo4j.token.ReadOnlyTokenCreator;
import org.neo4j.token.TokenCreator;
import org.neo4j.token.TokenHolders;

import static org.neo4j.graphdb.factory.EditionLocksFactories.createLockFactory;
import static org.neo4j.graphdb.factory.EditionLocksFactories.createLockManager;
import static org.neo4j.token.api.TokenHolder.TYPE_LABEL;
import static org.neo4j.token.api.TokenHolder.TYPE_PROPERTY_KEY;
import static org.neo4j.token.api.TokenHolder.TYPE_RELATIONSHIP_TYPE;

/**
 * This implementation of {@link AbstractEditionModule} creates the implementations of services
 * that are specific to the Community edition.
 */
public class CommunityEditionModule extends StandaloneEditionModule
{
    protected final SslPolicyLoader sslPolicyLoader;
    protected final GlobalModule globalModule;
    private final CompositeDatabaseAvailabilityGuard globalAvailabilityGuard;

    public CommunityEditionModule( GlobalModule globalModule )
    {
        Dependencies globalDependencies = globalModule.getGlobalDependencies();
        Config globalConfig = globalModule.getGlobalConfig();
        LogService logService = globalModule.getLogService();
        SystemNanoClock globalClock = globalModule.getGlobalClock();
        DependencyResolver externalDependencies = globalModule.getExternalDependencyResolver();
        this.globalModule = globalModule;

        watcherServiceFactory = databaseLayout -> createDatabaseFileSystemWatcher( globalModule.getFileWatcher(), databaseLayout,
                logService, fileWatcherFileNameFilter() );

        this.sslPolicyLoader = SslPolicyLoader.create( globalConfig, logService.getInternalLogProvider() );
        globalDependencies.satisfyDependency( sslPolicyLoader ); // for bolt and web server

        LocksFactory lockFactory = createLockFactory( globalConfig, logService );
        locksSupplier = () -> createLockManager( lockFactory, globalConfig, globalClock );
        statementLocksFactoryProvider = locks -> createStatementLocksFactory( locks, globalConfig, logService );

        idContextFactory = tryResolveOrCreate( IdContextFactory.class, externalDependencies, () -> createIdContextFactory( globalModule ) );

        tokenHoldersProvider = createTokenHolderProvider( globalModule );

        commitProcessFactory = new CommunityCommitProcessFactory();

        constraintSemantics = createSchemaRuleVerifier();

        ioLimiter = IOLimiter.UNLIMITED;

        connectionTracker = globalDependencies.satisfyDependency( createConnectionTracker() );
        globalAvailabilityGuard = globalModule.getGlobalAvailabilityGuard();
    }

    protected Function<NamedDatabaseId,TokenHolders> createTokenHolderProvider( GlobalModule platform )
    {
        Config globalConfig = platform.getGlobalConfig();
        return databaseId -> {
            DatabaseManager<?> databaseManager = platform.getGlobalDependencies().resolveDependency( DefaultDatabaseManager.class );
            Supplier<Kernel> kernelSupplier = () ->
            {
                DatabaseContext databaseContext = databaseManager.getDatabaseContext( databaseId )
                        .orElseThrow( () -> new IllegalStateException( "Default and system database kernels should always be accessible" ) );
                return databaseContext.dependencies().resolveDependency( Kernel.class );
            };
            return new TokenHolders(
                    new DelegatingTokenHolder( createPropertyKeyCreator( globalConfig, databaseId, kernelSupplier ), TYPE_PROPERTY_KEY ),
                    new DelegatingTokenHolder( createLabelIdCreator( globalConfig, databaseId, kernelSupplier ), TYPE_LABEL ),
                    new DelegatingTokenHolder( createRelationshipTypeCreator( globalConfig, databaseId, kernelSupplier ), TYPE_RELATIONSHIP_TYPE ) );
        };
    }

    private static IdContextFactory createIdContextFactory( GlobalModule globalModule )
    {
        return IdContextFactoryBuilder.of( globalModule.getFileSystem(), globalModule.getJobScheduler(), globalModule.getGlobalConfig() ).build();
    }

    protected Predicate<String> fileWatcherFileNameFilter()
    {
        return communityFileWatcherFileNameFilter();
    }

    static Predicate<String> communityFileWatcherFileNameFilter()
    {
        return fileName -> fileName.startsWith( TransactionLogFilesHelper.DEFAULT_NAME );
    }

    protected ConstraintSemantics createSchemaRuleVerifier()
    {
        return new StandardConstraintSemantics();
    }

    protected StatementLocksFactory createStatementLocksFactory( Locks locks, Config config, LogService logService )
    {
        return new SimpleStatementLocksFactory( locks );
    }

    protected static TokenCreator createRelationshipTypeCreator( Config config, NamedDatabaseId namedDatabaseId, Supplier<Kernel> kernelSupplier )
    {
        return createReadOnlyTokens( config, namedDatabaseId ) ? new ReadOnlyTokenCreator() : new DefaultRelationshipTypeCreator( kernelSupplier );
    }

    protected static TokenCreator createPropertyKeyCreator( Config config, NamedDatabaseId namedDatabaseId, Supplier<Kernel> kernelSupplier )
    {
        return createReadOnlyTokens( config, namedDatabaseId ) ? new ReadOnlyTokenCreator() : new DefaultPropertyTokenCreator( kernelSupplier );
    }

    protected static TokenCreator createLabelIdCreator( Config config, NamedDatabaseId namedDatabaseId, Supplier<Kernel> kernelSupplier )
    {
        return createReadOnlyTokens( config, namedDatabaseId ) ? new ReadOnlyTokenCreator() : new DefaultLabelIdCreator( kernelSupplier );
    }

    @Override
    public QueryEngineProvider getQueryEngineProvider()
    {
        return new CommunityCypherEngineProvider();
    }

    @Override
    public DatabaseStartupController getDatabaseStartupController()
    {
        return new GlobalAvailabilityGuardController( globalAvailabilityGuard );
    }

    @Override
    public Lifecycle createWebServer( DatabaseManagementService managementService, Dependencies globalDependencies, Config config,
            LogProvider userLogProvider, DatabaseInfo databaseInfo )
    {
        return new CommunityNeoWebServer( managementService, globalDependencies, config, userLogProvider, databaseInfo );
    }

    @Override
    public void registerEditionSpecificProcedures( GlobalProcedures globalProcedures, DatabaseManager<?> databaseManager ) throws KernelException
    {
        globalProcedures.register( new StandaloneDatabaseStateProcedure( databaseStateService,
                databaseManager.databaseIdRepository(), globalModule.getGlobalConfig() ) );
    }

    @Override
    protected BaseRoutingProcedureInstaller createRoutingProcedureInstaller( GlobalModule globalModule, DatabaseManager<?> databaseManager )
    {
        ConnectorPortRegister portRegister = globalModule.getConnectorPortRegister();
        Config config = globalModule.getGlobalConfig();
        LogProvider logProvider = globalModule.getLogService().getInternalLogProvider();
        return new SingleInstanceRoutingProcedureInstaller( databaseManager, portRegister, config, logProvider );
    }

    @Override
    public SystemGraphInitializer createSystemGraphInitializer( GlobalModule globalModule, DatabaseManager<?> databaseManager )
    {
        SystemGraphInitializer initializer = tryResolveOrCreate( SystemGraphInitializer.class, globalModule.getExternalDependencyResolver(),
                () -> new DefaultSystemGraphInitializer( databaseManager, globalModule.getGlobalConfig() ) );
        return globalModule.getGlobalDependencies().satisfyDependency( globalModule.getGlobalLife().add( initializer ) );
    }

    @Override
    public void createSecurityModule( GlobalModule globalModule )
    {
        LifeSupport globalLife = globalModule.getGlobalLife();
        if ( globalModule.getGlobalConfig().get( GraphDatabaseSettings.auth_enabled ) )
        {
            SecurityModule securityModule = new CommunitySecurityModule(
                    globalModule.getLogService(),
                    globalModule.getGlobalConfig(),
                    globalProcedures,
                    globalModule.getFileSystem(),
                    globalModule.getGlobalDependencies()
            );
            securityModule.setup();
            globalLife.add( securityModule );
            this.securityProvider = securityModule;
        }
        else
        {
            NoAuthSecurityProvider noAuthSecurityProvider = NoAuthSecurityProvider.INSTANCE;
            globalLife.add( noAuthSecurityProvider );
            this.securityProvider = noAuthSecurityProvider;
        }
    }

    public static <T> T tryResolveOrCreate( Class<T> clazz, DependencyResolver dependencies, Supplier<T> newInstanceMethod )
    {
        try
        {
            return dependencies.resolveDependency( clazz );
        }
        catch ( IllegalArgumentException | UnsatisfiedDependencyException e )
        {
            return newInstanceMethod.get();
        }
    }

    private static boolean createReadOnlyTokens( Config config, NamedDatabaseId namedDatabaseId )
    {
        return !namedDatabaseId.isSystemDatabase() && config.get( GraphDatabaseSettings.read_only );
    }

    @Override
    public BoltGraphDatabaseManagementServiceSPI createBoltDatabaseManagementServiceProvider( Dependencies dependencies,
            DatabaseManagementService managementService, Monitors monitors, SystemNanoClock clock, LogService logService )
    {
        var config = dependencies.resolveDependency( Config.class );
        var bookmarkAwaitDuration =  config.get( GraphDatabaseSettings.bookmark_ready_timeout );
        var reconciledTxTracker = new SimpleReconciledTransactionTracker( managementService, logService );
        return new BoltKernelDatabaseManagementServiceProvider( managementService, reconciledTxTracker, monitors, clock, bookmarkAwaitDuration );
    }
}
