/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.core;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.neo4j.causalclustering.ReplicationModule;
import org.neo4j.causalclustering.catchup.storecopy.LocalDatabase;
import org.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import org.neo4j.causalclustering.core.consensus.ConsensusModule;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.RaftProtocolClientInstaller;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.core.replication.ReplicationBenchmarkProcedure;
import org.neo4j.causalclustering.core.replication.Replicator;
import org.neo4j.causalclustering.core.server.CoreServerModule;
import org.neo4j.causalclustering.core.state.ClusterStateDirectory;
import org.neo4j.causalclustering.core.state.ClusterStateException;
import org.neo4j.causalclustering.core.state.ClusteringModule;
import org.neo4j.causalclustering.core.state.machines.CoreStateMachinesModule;
import org.neo4j.causalclustering.core.state.machines.id.FreeIdFilteredIdGeneratorFactory;
import org.neo4j.causalclustering.discovery.CoreTopologyService;
import org.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import org.neo4j.causalclustering.discovery.procedures.ClusterOverviewProcedure;
import org.neo4j.causalclustering.discovery.procedures.CoreRoleProcedure;
import org.neo4j.causalclustering.discovery.procedures.InstalledProtocolsProcedure;
import org.neo4j.causalclustering.handlers.DuplexPipelineWrapperFactory;
import org.neo4j.causalclustering.handlers.PipelineWrapper;
import org.neo4j.causalclustering.handlers.VoidPipelineWrapperFactory;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.load_balancing.LoadBalancingPluginLoader;
import org.neo4j.causalclustering.load_balancing.LoadBalancingProcessor;
import org.neo4j.causalclustering.load_balancing.procedure.GetServersProcedureForMultiDC;
import org.neo4j.causalclustering.load_balancing.procedure.GetServersProcedureForSingleDC;
import org.neo4j.causalclustering.load_balancing.procedure.LegacyGetServersProcedure;
import org.neo4j.causalclustering.logging.BetterMessageLogger;
import org.neo4j.causalclustering.logging.MessageLogger;
import org.neo4j.causalclustering.logging.NullMessageLogger;
import org.neo4j.causalclustering.messaging.LoggingOutbound;
import org.neo4j.causalclustering.messaging.Outbound;
import org.neo4j.causalclustering.messaging.RaftOutbound;
import org.neo4j.causalclustering.messaging.SenderService;
import org.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import org.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import org.neo4j.causalclustering.protocol.ProtocolInstaller;
import org.neo4j.causalclustering.protocol.handshake.HandshakeClientInitializer;
import org.neo4j.causalclustering.protocol.Protocol;
import org.neo4j.causalclustering.protocol.ProtocolInstallerRepository;
import org.neo4j.causalclustering.protocol.handshake.ProtocolRepository;
import org.neo4j.causalclustering.protocol.handshake.ProtocolStack;
import org.neo4j.com.storecopy.StoreUtil;
import org.neo4j.function.Predicates;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.SocketAddress;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.DatabaseAvailability;
import org.neo4j.kernel.api.bolt.BoltConnectionTracker;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ssl.SslPolicyLoader;
import org.neo4j.kernel.enterprise.builtinprocs.EnterpriseBuiltInDbmsProcedures;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.api.TransactionHeaderInformation;
import org.neo4j.kernel.impl.coreapi.CoreAPIAvailabilityGuard;
import org.neo4j.kernel.impl.enterprise.EnterpriseConstraintSemantics;
import org.neo4j.kernel.impl.enterprise.EnterpriseEditionModule;
import org.neo4j.kernel.impl.enterprise.StandardBoltConnectionTracker;
import org.neo4j.kernel.impl.enterprise.transaction.log.checkpoint.ConfigurableIOLimiter;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.factory.StatementLocksFactorySelector;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.pagecache.PageCacheWarmer;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdReuseEligibility;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.internal.DefaultKernelData;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.internal.KernelData;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleStatus;
import org.neo4j.logging.LogProvider;
import org.neo4j.time.Clocks;
import org.neo4j.udc.UsageData;

import static java.util.Collections.singletonList;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.raft_messages_log_path;
import static org.neo4j.helpers.collection.Iterators.asSet;

/**
 * This implementation of {@link org.neo4j.kernel.impl.factory.EditionModule} creates the implementations of services
 * that are specific to the Enterprise Core edition that provides a core cluster.
 */
public class EnterpriseCoreEditionModule extends EditionModule
{
    private final ConsensusModule consensusModule;
    private final ReplicationModule replicationModule;
    private final CoreTopologyService topologyService;
    protected final LogProvider logProvider;
    protected final Config config;
    private final Supplier<Stream<Pair<AdvertisedSocketAddress,ProtocolStack>>> clientInstalledProtocols;
    private final Supplier<Stream<Pair<SocketAddress,ProtocolStack>>> serverInstalledProtocols;
    private CoreStateMachinesModule coreStateMachinesModule;

    public enum RaftLogImplementation
    {
        IN_MEMORY, SEGMENTED
    }

    private LoadBalancingProcessor getLoadBalancingProcessor()
    {
        try
        {
            return LoadBalancingPluginLoader.load( topologyService, consensusModule.raftMachine(), logProvider, config );
        }
        catch ( Throwable e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public void registerEditionSpecificProcedures( Procedures procedures ) throws KernelException
    {
        procedures.registerProcedure( EnterpriseBuiltInDbmsProcedures.class, true );
        procedures.register( new LegacyGetServersProcedure( topologyService, consensusModule.raftMachine(), config, logProvider ) );

        if ( config.get( CausalClusteringSettings.multi_dc_license ) )
        {
            procedures.register( new GetServersProcedureForMultiDC( getLoadBalancingProcessor() ) );
        }
        else
        {
            procedures.register( new GetServersProcedureForSingleDC( topologyService, consensusModule.raftMachine(),
                    config, logProvider ) );
        }

        procedures.register( new ClusterOverviewProcedure( topologyService, consensusModule.raftMachine(), logProvider ) );
        procedures.register( new CoreRoleProcedure( consensusModule.raftMachine() ) );
        procedures.register( new InstalledProtocolsProcedure( clientInstalledProtocols, serverInstalledProtocols ) );
        procedures.registerComponent( Replicator.class, x -> replicationModule.getReplicator(), true );
        procedures.registerProcedure( ReplicationBenchmarkProcedure.class );
    }

    public EnterpriseCoreEditionModule( final PlatformModule platformModule,
            final DiscoveryServiceFactory discoveryServiceFactory )
    {
        final Dependencies dependencies = platformModule.dependencies;
        config = platformModule.config;
        final LogService logging = platformModule.logging;
        final FileSystemAbstraction fileSystem = platformModule.fileSystem;
        final File storeDir = platformModule.storeDir;
        final LifeSupport life = platformModule.life;

        final File dataDir = config.get( GraphDatabaseSettings.data_directory );
        final ClusterStateDirectory clusterStateDirectory = new ClusterStateDirectory( dataDir, storeDir, false );
        try
        {
            clusterStateDirectory.initialize( fileSystem );
        }
        catch ( ClusterStateException e )
        {
            throw new RuntimeException( e );
        }
        dependencies.satisfyDependency( clusterStateDirectory );

        eligibleForIdReuse = IdReuseEligibility.ALWAYS;

        logProvider = logging.getInternalLogProvider();
        final Supplier<DatabaseHealth> databaseHealthSupplier = dependencies.provideDependency( DatabaseHealth.class );

        watcherService = createFileSystemWatcherService( fileSystem, storeDir, logging,
                platformModule.jobScheduler, fileWatcherFileNameFilter() );
        dependencies.satisfyDependencies( watcherService );
        LogFiles logFiles = buildLocalDatabaseLogFiles( platformModule, fileSystem, storeDir );
        LocalDatabase localDatabase = new LocalDatabase( platformModule.storeDir,
                new StoreFiles( fileSystem, platformModule.pageCache ),
                logFiles,
                platformModule.dataSourceManager,
                databaseHealthSupplier,
                watcherService,
                platformModule.availabilityGuard,
                logProvider );

        IdentityModule identityModule = new IdentityModule( platformModule, clusterStateDirectory.get() );

        ClusteringModule clusteringModule = getClusteringModule( platformModule, discoveryServiceFactory,
                clusterStateDirectory, identityModule, dependencies );

        // We need to satisfy the dependency here to keep users of it, such as BoltKernelExtension, happy.
        dependencies.satisfyDependency( SslPolicyLoader.create( config, logProvider ) );

        PipelineWrapper clientPipelineWrapper = pipelineWrapperFactory().forClient( config, dependencies, logProvider );
        PipelineWrapper serverPipelineWrapper = pipelineWrapperFactory().forServer( config, dependencies, logProvider );

        NettyPipelineBuilderFactory clientPipelineBuilderFactory = new NettyPipelineBuilderFactory( clientPipelineWrapper );
        NettyPipelineBuilderFactory serverPipelineBuilderFactory = new NettyPipelineBuilderFactory( serverPipelineWrapper );

        topologyService = clusteringModule.topologyService();

        long logThresholdMillis = config.get( CausalClusteringSettings.unknown_address_logging_throttle ).toMillis();

        ProtocolRepository<Protocol.ApplicationProtocol> applicationProtocolRepository = new ProtocolRepository<>( Protocol.ApplicationProtocols.values() );
        ProtocolRepository<Protocol.ModifierProtocol> modifierProtocolRepository = new ProtocolRepository<>( Protocol.ModifierProtocols.values() );
        ProtocolInstallerRepository<ProtocolInstaller.Orientation.Client> protocolInstallerRepository =
                new ProtocolInstallerRepository<>(
                        singletonList( new RaftProtocolClientInstaller.Factory( clientPipelineBuilderFactory, logProvider ) ),
                        ModifierProtocolInstaller.allClientInstallers );
        HandshakeClientInitializer channelInitializer = new HandshakeClientInitializer( applicationProtocolRepository,
                Protocol.ApplicationProtocolIdentifier.RAFT, modifierProtocolRepository, asSet( Protocol.ModifierProtocolIdentifier.COMPRESSION ),
                protocolInstallerRepository, clientPipelineBuilderFactory, config, logProvider );
        final SenderService raftSender = new SenderService( channelInitializer, logProvider );
        life.add( raftSender );
        this.clientInstalledProtocols = raftSender::installedProtocols;

        final MessageLogger<MemberId> messageLogger = createMessageLogger( config, life, identityModule.myself() );

        RaftOutbound raftOutbound = new RaftOutbound( topologyService, raftSender, clusteringModule.clusterIdentity(),
                logProvider, logThresholdMillis );
        Outbound<MemberId,RaftMessages.RaftMessage> loggingOutbound = new LoggingOutbound<>( raftOutbound,
                identityModule.myself(), messageLogger );

        consensusModule = new ConsensusModule( identityModule.myself(), platformModule, loggingOutbound,
                clusterStateDirectory.get(), topologyService );

        dependencies.satisfyDependency( consensusModule.raftMachine() );

        replicationModule = new ReplicationModule( identityModule.myself(), platformModule, config, consensusModule,
                loggingOutbound, clusterStateDirectory.get(), fileSystem, logProvider );

        coreStateMachinesModule = new CoreStateMachinesModule( identityModule.myself(),
                platformModule, clusterStateDirectory.get(), config, replicationModule.getReplicator(),
                consensusModule.raftMachine(), dependencies, localDatabase );

        this.idTypeConfigurationProvider = coreStateMachinesModule.idTypeConfigurationProvider;

        createIdComponents( platformModule, dependencies, coreStateMachinesModule.idGeneratorFactory );
        dependencies.satisfyDependency( idGeneratorFactory );
        dependencies.satisfyDependency( idController );

        this.labelTokenHolder = coreStateMachinesModule.labelTokenHolder;
        this.propertyKeyTokenHolder = coreStateMachinesModule.propertyKeyTokenHolder;
        this.relationshipTypeTokenHolder = coreStateMachinesModule.relationshipTypeTokenHolder;
        this.lockManager = coreStateMachinesModule.lockManager;
        this.commitProcessFactory = coreStateMachinesModule.commitProcessFactory;
        this.accessCapability = new LeaderCanWrite( consensusModule.raftMachine() );

        CoreServerModule coreServerModule = new CoreServerModule( identityModule, platformModule, consensusModule, coreStateMachinesModule, clusteringModule,
                replicationModule, localDatabase, databaseHealthSupplier, clusterStateDirectory.get(), serverPipelineWrapper, clientPipelineWrapper );

        serverInstalledProtocols = new RaftServerModule(
                platformModule, consensusModule, identityModule, coreServerModule, localDatabase, serverPipelineBuilderFactory, messageLogger,
                topologyService
        ).raftServer()::installedProtocols;

        editionInvariants( platformModule, dependencies, config, logging, life );

        dependencies.satisfyDependency( lockManager );

        life.add( coreServerModule.membershipWaiterLifecycle );
    }

    private LogFiles buildLocalDatabaseLogFiles( PlatformModule platformModule, FileSystemAbstraction fileSystem,
            File storeDir )
    {
        try
        {
            return LogFilesBuilder.activeFilesBuilder( storeDir, fileSystem, platformModule.pageCache ).withConfig( config ).build();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    protected ClusteringModule getClusteringModule( PlatformModule platformModule,
                                                  DiscoveryServiceFactory discoveryServiceFactory,
                                                  ClusterStateDirectory clusterStateDirectory,
                                                  IdentityModule identityModule, Dependencies dependencies )
    {
        return new ClusteringModule( discoveryServiceFactory, identityModule.myself(),
                platformModule, clusterStateDirectory.get() );
    }

    protected DuplexPipelineWrapperFactory pipelineWrapperFactory()
    {
        return new VoidPipelineWrapperFactory();
    }

    @Override
    protected void createIdComponents( PlatformModule platformModule, Dependencies dependencies,
            IdGeneratorFactory editionIdGeneratorFactory )
    {
        super.createIdComponents( platformModule, dependencies, editionIdGeneratorFactory );
        this.idGeneratorFactory =
                new FreeIdFilteredIdGeneratorFactory( this.idGeneratorFactory, coreStateMachinesModule.freeIdCondition );
    }

    static Predicate<String> fileWatcherFileNameFilter()
    {
        return Predicates.any(
                fileName -> fileName.startsWith( TransactionLogFiles.DEFAULT_NAME ),
                fileName -> fileName.startsWith( IndexConfigStore.INDEX_DB_FILE_NAME ),
                filename -> filename.startsWith( StoreUtil.TEMP_COPY_DIRECTORY_NAME ),
                filename -> filename.endsWith( PageCacheWarmer.SUFFIX_CACHEPROF ),
                filename -> filename.endsWith( PageCacheWarmer.SUFFIX_CACHEPROF_TMP )
        );
    }

    private MessageLogger<MemberId> createMessageLogger( Config config, LifeSupport life, MemberId myself )
    {
        final MessageLogger<MemberId> messageLogger;
        if ( config.get( CausalClusteringSettings.raft_messages_log_enable ) )
        {
            File logFile = config.get( raft_messages_log_path );
            messageLogger = life.add( new BetterMessageLogger<>( myself, raftMessagesLog( logFile ), Clocks.systemClock() ) );
        }
        else
        {
            messageLogger = new NullMessageLogger<>();
        }
        return messageLogger;
    }

    private void editionInvariants( PlatformModule platformModule, Dependencies dependencies, Config config,
            LogService logging, LifeSupport life )
    {
        statementLocksFactory = new StatementLocksFactorySelector( lockManager, config, logging ).select();

        dependencies.satisfyDependency(
                createKernelData( platformModule.fileSystem, platformModule.pageCache, platformModule.storeDir,
                        config, platformModule.graphDatabaseFacade, life ) );

        ioLimiter = new ConfigurableIOLimiter( platformModule.config );

        headerInformationFactory = createHeaderInformationFactory();

        schemaWriteGuard = createSchemaWriteGuard();

        transactionStartTimeout = config.get( GraphDatabaseSettings.transaction_start_timeout ).toMillis();

        constraintSemantics = new EnterpriseConstraintSemantics();

        coreAPIAvailabilityGuard =
                new CoreAPIAvailabilityGuard( platformModule.availabilityGuard, transactionStartTimeout );

        registerRecovery( platformModule.databaseInfo, life, dependencies );

        publishEditionInfo( dependencies.resolveDependency( UsageData.class ), platformModule.databaseInfo, config );

        dependencies.satisfyDependency( createSessionTracker() );
    }

    public boolean isLeader()
    {
        return consensusModule.raftMachine().currentRole() == Role.LEADER;
    }

    private static PrintWriter raftMessagesLog( File logFile )
    {
        //noinspection ResultOfMethodCallIgnored
        logFile.getParentFile().mkdirs();
        try
        {

            return new PrintWriter( new FileOutputStream( logFile, true ) );
        }
        catch ( FileNotFoundException e )
        {
            throw new RuntimeException( e );
        }
    }

    private SchemaWriteGuard createSchemaWriteGuard()
    {
        return SchemaWriteGuard.ALLOW_ALL_WRITES;
    }

    private KernelData createKernelData( FileSystemAbstraction fileSystem, PageCache pageCache, File storeDir,
            Config config, GraphDatabaseAPI graphAPI, LifeSupport life )
    {
        DefaultKernelData kernelData = new DefaultKernelData( fileSystem, pageCache, storeDir, config, graphAPI );
        return life.add( kernelData );
    }

    private TransactionHeaderInformationFactory createHeaderInformationFactory()
    {
        return () -> new TransactionHeaderInformation( -1, -1, new byte[0] );
    }

    private void registerRecovery( final DatabaseInfo databaseInfo, LifeSupport life,
            final DependencyResolver dependencyResolver )
    {
        life.addLifecycleListener( ( instance, from, to ) ->
        {
            if ( instance instanceof DatabaseAvailability && LifecycleStatus.STARTED.equals( to ) )
            {
                doAfterRecoveryAndStartup( databaseInfo, dependencyResolver );
            }
        } );
    }

    @Override
    protected BoltConnectionTracker createSessionTracker()
    {
        return new StandardBoltConnectionTracker();
    }

    @Override
    public void setupSecurityModule( PlatformModule platformModule, Procedures procedures )
    {
        EnterpriseEditionModule.setupEnterpriseSecurityModule( platformModule, procedures );
    }
}
