/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.readreplica;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.CatchUpResponseHandler;
import org.neo4j.causalclustering.catchup.CatchupProtocolClientInstaller;
import org.neo4j.causalclustering.catchup.CatchupServerBuilder;
import org.neo4j.causalclustering.catchup.CheckPointerService;
import org.neo4j.causalclustering.catchup.RegularCatchupServerHandler;
import org.neo4j.causalclustering.catchup.storecopy.CopiedStoreRecovery;
import org.neo4j.causalclustering.catchup.storecopy.LocalDatabase;
import org.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyClient;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyProcess;
import org.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import org.neo4j.causalclustering.catchup.tx.BatchingTxApplier;
import org.neo4j.causalclustering.catchup.tx.CatchupPollingProcess;
import org.neo4j.causalclustering.catchup.tx.TransactionLogCatchUpFactory;
import org.neo4j.causalclustering.catchup.tx.TxPullClient;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.SupportedProtocolCreator;
import org.neo4j.causalclustering.core.TransactionBackupServiceProvider;
import org.neo4j.causalclustering.core.consensus.schedule.TimerService;
import org.neo4j.causalclustering.core.state.machines.id.CommandIndexTracker;
import org.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import org.neo4j.causalclustering.discovery.RemoteMembersResolver;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.discovery.TopologyServiceMultiRetryStrategy;
import org.neo4j.causalclustering.discovery.TopologyServiceRetryStrategy;
import org.neo4j.causalclustering.discovery.procedures.ClusterOverviewProcedure;
import org.neo4j.causalclustering.discovery.procedures.ReadReplicaRoleProcedure;
import org.neo4j.causalclustering.handlers.DuplexPipelineWrapperFactory;
import org.neo4j.causalclustering.handlers.PipelineWrapper;
import org.neo4j.causalclustering.handlers.VoidPipelineWrapperFactory;
import org.neo4j.causalclustering.helper.CompositeSuspendable;
import org.neo4j.causalclustering.helper.ExponentialBackoffStrategy;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.net.InstalledProtocolHandler;
import org.neo4j.causalclustering.net.Server;
import org.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import org.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import org.neo4j.causalclustering.protocol.Protocol;
import org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocols;
import org.neo4j.causalclustering.protocol.ProtocolInstaller;
import org.neo4j.causalclustering.protocol.ProtocolInstallerRepository;
import org.neo4j.causalclustering.protocol.handshake.ApplicationProtocolRepository;
import org.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;
import org.neo4j.causalclustering.protocol.handshake.HandshakeClientInitializer;
import org.neo4j.causalclustering.protocol.handshake.ModifierProtocolRepository;
import org.neo4j.causalclustering.protocol.handshake.ModifierSupportedProtocols;
import org.neo4j.causalclustering.upstream.NoOpUpstreamDatabaseStrategiesLoader;
import org.neo4j.causalclustering.upstream.UpstreamDatabaseStrategiesLoader;
import org.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;
import org.neo4j.causalclustering.upstream.strategies.ConnectToRandomCoreServerStrategy;
import org.neo4j.com.storecopy.StoreUtil;
import org.neo4j.function.Predicates;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.graphdb.factory.module.edition.DefaultEditionModule;
import org.neo4j.graphdb.factory.module.id.IdContextFactoryBuilder;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ssl.SslPolicyLoader;
import org.neo4j.kernel.enterprise.builtinprocs.EnterpriseBuiltInDbmsProcedures;
import org.neo4j.kernel.enterprise.builtinprocs.EnterpriseBuiltInProcedures;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.ReadOnlyTransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.core.DelegatingTokenHolder;
import org.neo4j.kernel.impl.core.ReadOnlyTokenCreator;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.core.TokenHolder;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.kernel.impl.enterprise.EnterpriseConstraintSemantics;
import org.neo4j.kernel.impl.enterprise.EnterpriseEditionModule;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.enterprise.id.EnterpriseIdTypeConfigurationProvider;
import org.neo4j.kernel.impl.enterprise.transaction.log.checkpoint.ConfigurableIOLimiter;
import org.neo4j.kernel.impl.factory.ReadOnly;
import org.neo4j.kernel.impl.factory.StatementLocksFactorySelector;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.net.DefaultNetworkConnectionTracker;
import org.neo4j.kernel.impl.pagecache.PageCacheWarmer;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.internal.KernelData;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.Group;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.time.Clocks;
import org.neo4j.udc.UsageData;

import static java.util.Collections.singletonList;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.transaction_listen_address;
import static org.neo4j.causalclustering.discovery.ResolutionResolverFactory.chooseResolver;

/**
 * This implementation of {@link AbstractEditionModule} creates the implementations of services
 * that are specific to the Enterprise Read Replica edition.
 */
public class EnterpriseReadReplicaEditionModule extends DefaultEditionModule
{
    private final TopologyService topologyService;
    private final LogProvider logProvider;

    public EnterpriseReadReplicaEditionModule( final PlatformModule platformModule, final DiscoveryServiceFactory discoveryServiceFactory, MemberId myself )
    {
        LogService logging = platformModule.logging;

        ioLimiter = new ConfigurableIOLimiter( platformModule.config );
        platformModule.jobScheduler.setTopLevelGroupName( "ReadReplica " + myself );

        org.neo4j.kernel.impl.util.Dependencies dependencies = platformModule.dependencies;
        Config config = platformModule.config;
        FileSystemAbstraction fileSystem = platformModule.fileSystem;
        PageCache pageCache = platformModule.pageCache;
        final DatabaseLayout databaseLayout = platformModule.storeLayout.databaseLayout( config.get( GraphDatabaseSettings.active_database ) );

        LifeSupport life = platformModule.life;

        threadToTransactionBridge = dependencies.satisfyDependency(
                new ThreadToStatementContextBridge( getGlobalAvailabilityGuard( platformModule.clock, logging, platformModule.config ) ) );
        this.accessCapability = new ReadOnly();

        watcherServiceFactory = dir -> createFileSystemWatcherService( fileSystem, dir, logging,
                platformModule.jobScheduler, config, fileWatcherFileNameFilter() );
        dependencies.satisfyDependencies( watcherServiceFactory );

        ReadReplicaLockManager emptyLockManager = new ReadReplicaLockManager();
        locksSupplier = () -> emptyLockManager;
        statementLocksFactoryProvider = locks -> new StatementLocksFactorySelector( locks, config, logging ).select();

        idContextFactory = IdContextFactoryBuilder.of( new EnterpriseIdTypeConfigurationProvider( config ), platformModule.jobScheduler )
                    .withFileSystem( fileSystem )
                    .build();

        tokenHoldersProvider = databaseName -> new TokenHolders(
                new DelegatingTokenHolder( new ReadOnlyTokenCreator(), TokenHolder.TYPE_PROPERTY_KEY ),
                new DelegatingTokenHolder( new ReadOnlyTokenCreator(), TokenHolder.TYPE_LABEL ),
                new DelegatingTokenHolder( new ReadOnlyTokenCreator(), TokenHolder.TYPE_RELATIONSHIP_TYPE ) );

        File contextDirectory = platformModule.storeLayout.storeDirectory();
        life.add( dependencies.satisfyDependency( new KernelData( fileSystem, pageCache, contextDirectory, config, platformModule.dataSourceManager ) ) );

        headerInformationFactory = TransactionHeaderInformationFactory.DEFAULT;

        schemaWriteGuard = () ->
        {
        };

        transactionStartTimeout = config.get( GraphDatabaseSettings.transaction_start_timeout ).toMillis();

        constraintSemantics = new EnterpriseConstraintSemantics();

        publishEditionInfo( dependencies.resolveDependency( UsageData.class ), platformModule.databaseInfo, config );
        commitProcessFactory = readOnly();

        connectionTracker = dependencies.satisfyDependency( createConnectionTracker() );

        logProvider = platformModule.logging.getInternalLogProvider();
        LogProvider userLogProvider = platformModule.logging.getUserLogProvider();

        logProvider.getLog( getClass() ).info( String.format( "Generated new id: %s", myself ) );

        RemoteMembersResolver hostnameResolver = chooseResolver( config, platformModule.logging );

        configureDiscoveryService( discoveryServiceFactory, dependencies, config, logProvider );

        topologyService = discoveryServiceFactory.readReplicaTopologyService(
                config,
                logProvider,
                platformModule.jobScheduler,
                myself,
                hostnameResolver,
                resolveStrategy( config, logProvider ) );

        life.add( dependencies.satisfyDependency( topologyService ) );

        // We need to satisfy the dependency here to keep users of it, such as BoltKernelExtension, happy.
        dependencies.satisfyDependency( SslPolicyLoader.create( config, logProvider ) );

        DuplexPipelineWrapperFactory pipelineWrapperFactory = pipelineWrapperFactory();
        PipelineWrapper serverPipelineWrapper = pipelineWrapperFactory.forServer( config, dependencies, logProvider, CausalClusteringSettings.ssl_policy );
        PipelineWrapper clientPipelineWrapper = pipelineWrapperFactory.forClient( config, dependencies, logProvider, CausalClusteringSettings.ssl_policy );
        PipelineWrapper backupServerPipelineWrapper = pipelineWrapperFactory.forServer( config, dependencies, logProvider, OnlineBackupSettings.ssl_policy );

        NettyPipelineBuilderFactory clientPipelineBuilderFactory = new NettyPipelineBuilderFactory( clientPipelineWrapper );
        NettyPipelineBuilderFactory serverPipelineBuilderFactory = new NettyPipelineBuilderFactory( serverPipelineWrapper );
        NettyPipelineBuilderFactory backupServerPipelineBuilderFactory = new NettyPipelineBuilderFactory( backupServerPipelineWrapper );

        SupportedProtocolCreator supportedProtocolCreator = new SupportedProtocolCreator( config, logProvider );
        ApplicationSupportedProtocols supportedCatchupProtocols = supportedProtocolCreator.createSupportedCatchupProtocol();
        Collection<ModifierSupportedProtocols> supportedModifierProtocols = supportedProtocolCreator.createSupportedModifierProtocols();

        ApplicationProtocolRepository applicationProtocolRepository =
                new ApplicationProtocolRepository( ApplicationProtocols.values(), supportedCatchupProtocols );
        ModifierProtocolRepository modifierProtocolRepository =
                new ModifierProtocolRepository( Protocol.ModifierProtocols.values(), supportedModifierProtocols );

        Function<CatchUpResponseHandler,ChannelInitializer<SocketChannel>> channelInitializer = handler -> {
            ProtocolInstallerRepository<ProtocolInstaller.Orientation.Client> protocolInstallerRepository = new ProtocolInstallerRepository<>(
                    singletonList( new CatchupProtocolClientInstaller.Factory( clientPipelineBuilderFactory, logProvider, handler ) ),
                    ModifierProtocolInstaller.allClientInstallers );
            Duration handshakeTimeout = config.get( CausalClusteringSettings.handshake_timeout );
            return new HandshakeClientInitializer( applicationProtocolRepository, modifierProtocolRepository, protocolInstallerRepository,
                    clientPipelineBuilderFactory, handshakeTimeout, logProvider, userLogProvider );
        };

        long inactivityTimeoutMs = config.get( CausalClusteringSettings.catch_up_client_inactivity_timeout ).toMillis();

        CatchUpClient catchUpClient = life.add( new CatchUpClient( logProvider, Clocks.systemClock(), inactivityTimeoutMs, channelInitializer ) );

        final Supplier<DatabaseHealth> databaseHealthSupplier =
                () -> platformModule.dataSourceManager.getDataSource().getDependencyResolver().resolveDependency( DatabaseHealth.class );

        StoreFiles storeFiles = new StoreFiles( fileSystem, pageCache );
        LogFiles logFiles = buildLocalDatabaseLogFiles( platformModule, fileSystem, databaseLayout, config );

        LocalDatabase localDatabase =
                new LocalDatabase( databaseLayout, storeFiles, logFiles, platformModule.dataSourceManager, databaseHealthSupplier,
                        getGlobalAvailabilityGuard( platformModule.clock, platformModule.logging, platformModule.config ), logProvider );

        Supplier<TransactionCommitProcess> writableCommitProcess = () -> new TransactionRepresentationCommitProcess(
                localDatabase.dataSource().getDependencyResolver().resolveDependency( TransactionAppender.class ),
                localDatabase.dataSource().getDependencyResolver().resolveDependency( StorageEngine.class ) );

        LifeSupport txPulling = new LifeSupport();
        int maxBatchSize = config.get( CausalClusteringSettings.read_replica_transaction_applier_batch_size );

        CommandIndexTracker commandIndexTracker = platformModule.dependencies.satisfyDependency( new CommandIndexTracker() );
        BatchingTxApplier batchingTxApplier = new BatchingTxApplier(
                maxBatchSize, () -> localDatabase.dataSource().getDependencyResolver().resolveDependency( TransactionIdStore.class ), writableCommitProcess,
                platformModule.monitors, platformModule.tracers.pageCursorTracerSupplier,
                platformModule.versionContextSupplier, commandIndexTracker, logProvider );

        TimerService timerService = new TimerService( platformModule.jobScheduler, logProvider );

        ExponentialBackoffStrategy storeCopyBackoffStrategy =
                new ExponentialBackoffStrategy( 1, config.get( CausalClusteringSettings.store_copy_backoff_max_wait ).toMillis(), TimeUnit.MILLISECONDS );

        RemoteStore remoteStore = new RemoteStore( platformModule.logging.getInternalLogProvider(), fileSystem, platformModule.pageCache,
                new StoreCopyClient( catchUpClient, platformModule.monitors, logProvider, storeCopyBackoffStrategy ),
                new TxPullClient( catchUpClient, platformModule.monitors ),
                new TransactionLogCatchUpFactory(), config, platformModule.monitors );

        CopiedStoreRecovery copiedStoreRecovery =
                new CopiedStoreRecovery( config, platformModule.kernelExtensionFactories, platformModule.pageCache );

        txPulling.add( copiedStoreRecovery );

        CompositeSuspendable servicesToStopOnStoreCopy = new CompositeSuspendable();

        StoreCopyProcess storeCopyProcess = new StoreCopyProcess( fileSystem, pageCache, localDatabase,
                copiedStoreRecovery, remoteStore, logProvider );

        ConnectToRandomCoreServerStrategy defaultStrategy = new ConnectToRandomCoreServerStrategy();
        defaultStrategy.inject( topologyService, config, logProvider, myself );

        UpstreamDatabaseStrategySelector upstreamDatabaseStrategySelector =
                createUpstreamDatabaseStrategySelector( myself, config, logProvider, topologyService, defaultStrategy );

        CatchupPollingProcess catchupProcess =
                new CatchupPollingProcess( logProvider, localDatabase, servicesToStopOnStoreCopy, catchUpClient, upstreamDatabaseStrategySelector,
                        timerService, config.get( CausalClusteringSettings.pull_interval ).toMillis(), batchingTxApplier, platformModule.monitors,
                        storeCopyProcess, databaseHealthSupplier, topologyService );
        dependencies.satisfyDependencies( catchupProcess );

        txPulling.add( batchingTxApplier );
        txPulling.add( catchupProcess );
        txPulling.add( new WaitForUpToDateStore( catchupProcess, logProvider ) );

        ExponentialBackoffStrategy retryStrategy = new ExponentialBackoffStrategy( 1, 30, TimeUnit.SECONDS );
        life.add( new ReadReplicaStartupProcess( remoteStore, localDatabase, txPulling, upstreamDatabaseStrategySelector, retryStrategy, logProvider,
                platformModule.logging.getUserLogProvider(), storeCopyProcess, topologyService ) );

        CheckPointerService checkPointerService =
                new CheckPointerService( () -> localDatabase.dataSource().getDependencyResolver().resolveDependency( CheckPointer.class ),
                        platformModule.jobScheduler, Group.CHECKPOINT );
        RegularCatchupServerHandler catchupServerHandler = new RegularCatchupServerHandler( platformModule.monitors,
                logProvider, localDatabase::storeId, localDatabase::dataSource, localDatabase::isAvailable,
                fileSystem, null, checkPointerService );

        InstalledProtocolHandler installedProtocolHandler = new InstalledProtocolHandler(); // TODO: hook into a procedure
        Server catchupServer = new CatchupServerBuilder( catchupServerHandler )
                .serverHandler( installedProtocolHandler )
                .catchupProtocols( supportedCatchupProtocols )
                .modifierProtocols( supportedModifierProtocols )
                .pipelineBuilder( serverPipelineBuilderFactory )
                .userLogProvider( userLogProvider )
                .debugLogProvider( logProvider )
                .listenAddress( config.get( transaction_listen_address ) )
                .serverName( "catchup-server" )
                .build();

        TransactionBackupServiceProvider transactionBackupServiceProvider =
                new TransactionBackupServiceProvider( logProvider, userLogProvider, supportedCatchupProtocols, supportedModifierProtocols,
                        backupServerPipelineBuilderFactory, catchupServerHandler, installedProtocolHandler );
        Optional<Server> backupCatchupServer = transactionBackupServiceProvider.resolveIfBackupEnabled( config );

        servicesToStopOnStoreCopy.add( catchupServer );
        backupCatchupServer.ifPresent( servicesToStopOnStoreCopy::add );

        life.add( catchupServer ); // must start last and stop first, since it handles external requests
        backupCatchupServer.ifPresent( life::add );
    }

    private UpstreamDatabaseStrategySelector createUpstreamDatabaseStrategySelector( MemberId myself, Config config, LogProvider logProvider,
            TopologyService topologyService, ConnectToRandomCoreServerStrategy defaultStrategy )
    {
        UpstreamDatabaseStrategiesLoader loader;
        if ( config.get( CausalClusteringSettings.multi_dc_license ) )
        {
            loader = new UpstreamDatabaseStrategiesLoader( topologyService, config, myself, logProvider );
            logProvider.getLog( getClass() ).info( "Multi-Data Center option enabled." );
        }
        else
        {
            loader = new NoOpUpstreamDatabaseStrategiesLoader();
        }

        return new UpstreamDatabaseStrategySelector( defaultStrategy, loader, logProvider );
    }

    protected void configureDiscoveryService( DiscoveryServiceFactory discoveryServiceFactory, Dependencies dependencies,
                                              Config config, LogProvider logProvider )
    {
    }

    protected DuplexPipelineWrapperFactory pipelineWrapperFactory()
    {
        return new VoidPipelineWrapperFactory();
    }

    static Predicate<String> fileWatcherFileNameFilter()
    {
        return Predicates.any( fileName -> fileName.startsWith( TransactionLogFiles.DEFAULT_NAME ),
                fileName -> fileName.startsWith( IndexConfigStore.INDEX_DB_FILE_NAME ),
                filename -> filename.startsWith( StoreUtil.BRANCH_SUBDIRECTORY ),
                filename -> filename.startsWith( StoreUtil.TEMP_COPY_DIRECTORY_NAME ),
                filename -> filename.endsWith( PageCacheWarmer.SUFFIX_CACHEPROF ) );
    }

    @Override
    public void registerEditionSpecificProcedures( Procedures procedures ) throws KernelException
    {
        procedures.registerProcedure( EnterpriseBuiltInDbmsProcedures.class, true );
        procedures.registerProcedure( EnterpriseBuiltInProcedures.class, true );
        procedures.register( new ReadReplicaRoleProcedure() );
        procedures.register( new ClusterOverviewProcedure( topologyService, logProvider ) );
    }

    private CommitProcessFactory readOnly()
    {
        return ( appender, storageEngine, config ) -> new ReadOnlyTransactionCommitProcess();
    }

    @Override
    protected NetworkConnectionTracker createConnectionTracker()
    {
        return new DefaultNetworkConnectionTracker();
    }

    @Override
    public void createSecurityModule( PlatformModule platformModule, Procedures procedures )
    {
        EnterpriseEditionModule.createEnterpriseSecurityModule( this, platformModule, procedures );
    }

    private static TopologyServiceRetryStrategy resolveStrategy( Config config, LogProvider logProvider )
    {
        long refreshPeriodMillis = config.get( CausalClusteringSettings.cluster_topology_refresh ).toMillis();
        int pollingFrequencyWithinRefreshWindow = 2;
        int numberOfRetries =
                pollingFrequencyWithinRefreshWindow + 1; // we want to have more retries at the given frequency than there is time in a refresh period
        return new TopologyServiceMultiRetryStrategy( refreshPeriodMillis / pollingFrequencyWithinRefreshWindow, numberOfRetries, logProvider );
    }

    private static LogFiles buildLocalDatabaseLogFiles( PlatformModule platformModule, FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout,
            Config config )
    {
        try
        {
            return LogFilesBuilder.activeFilesBuilder( databaseLayout, fileSystem, platformModule.pageCache ).withConfig( config ).build();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
