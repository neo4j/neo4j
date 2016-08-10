/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.core;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.function.Supplier;

import org.neo4j.coreedge.ReplicationModule;
import org.neo4j.coreedge.catchup.storecopy.CopiedStoreRecovery;
import org.neo4j.coreedge.catchup.storecopy.LocalDatabase;
import org.neo4j.coreedge.catchup.storecopy.StoreFiles;
import org.neo4j.coreedge.core.consensus.ConsensusModule;
import org.neo4j.coreedge.core.consensus.RaftMachine;
import org.neo4j.coreedge.core.consensus.RaftMessages;
import org.neo4j.coreedge.core.consensus.roles.Role;
import org.neo4j.coreedge.core.server.CoreServerModule;
import org.neo4j.coreedge.core.state.machines.CoreStateMachinesModule;
import org.neo4j.coreedge.core.state.storage.MemberIdStorage;
import org.neo4j.coreedge.discovery.CoreTopologyService;
import org.neo4j.coreedge.discovery.DiscoveryServiceFactory;
import org.neo4j.coreedge.discovery.procedures.AcquireEndpointsProcedure;
import org.neo4j.coreedge.discovery.procedures.ClusterOverviewProcedure;
import org.neo4j.coreedge.discovery.procedures.CoreRoleProcedure;
import org.neo4j.coreedge.discovery.procedures.DiscoverMembersProcedure;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.coreedge.identity.MemberId.MemberIdMarshal;
import org.neo4j.coreedge.logging.BetterMessageLogger;
import org.neo4j.coreedge.logging.MessageLogger;
import org.neo4j.coreedge.logging.NullMessageLogger;
import org.neo4j.coreedge.messaging.CoreReplicatedContentMarshal;
import org.neo4j.coreedge.messaging.LoggingOutbound;
import org.neo4j.coreedge.messaging.NonBlockingChannels;
import org.neo4j.coreedge.messaging.Outbound;
import org.neo4j.coreedge.messaging.RaftChannelInitializer;
import org.neo4j.coreedge.messaging.RaftOutbound;
import org.neo4j.coreedge.messaging.SenderService;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.DatabaseAvailability;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.bolt.SessionTracker;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.api.TransactionHeaderInformation;
import org.neo4j.kernel.impl.api.index.RemoveOrphanConstraintIndexesOnStartup;
import org.neo4j.kernel.impl.coreapi.CoreAPIAvailabilityGuard;
import org.neo4j.kernel.impl.enterprise.EnterpriseConstraintSemantics;
import org.neo4j.kernel.impl.enterprise.StandardSessionTracker;
import org.neo4j.kernel.impl.enterprise.transaction.log.checkpoint.ConfigurableIOLimiter;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.factory.StatementLocksFactorySelector;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.internal.DefaultKernelData;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.internal.KernelData;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleStatus;
import org.neo4j.logging.LogProvider;
import org.neo4j.udc.UsageData;

/**
 * This implementation of {@link org.neo4j.kernel.impl.factory.EditionModule} creates the implementations of services
 * that are specific to the Enterprise Core edition that provides a core cluster.
 */
public class EnterpriseCoreEditionModule extends EditionModule
{
    public static final String CLUSTER_STATE_DIRECTORY_NAME = "cluster-state";
    public static final String CORE_MEMBER_ID_NAME = "core-member-id";

    private final ConsensusModule consensusModule;
    private final CoreTopologyService discoveryService;
    private final LogProvider logProvider;
    private final CoreStateMachinesModule coreStateMachinesModule;

    public enum RaftLogImplementation
    {
        IN_MEMORY, SEGMENTED
    }

    @Override
    public void registerProcedures( Procedures procedures )
    {
        try
        {
            procedures.register( new DiscoverMembersProcedure( discoveryService, logProvider ) );
            procedures.register( new AcquireEndpointsProcedure( discoveryService, consensusModule.raftMachine(), logProvider ) );
            procedures.register( new ClusterOverviewProcedure( discoveryService, consensusModule.raftMachine(), logProvider ) );
            procedures.register( new CoreRoleProcedure( consensusModule.raftMachine()) );
        }
        catch ( ProcedureException e )
        {
            throw new RuntimeException( e );
        }
    }

    EnterpriseCoreEditionModule( final PlatformModule platformModule,
            final DiscoveryServiceFactory discoveryServiceFactory )
    {
        final Dependencies dependencies = platformModule.dependencies;
        final Config config = platformModule.config;
        final LogService logging = platformModule.logging;
        final FileSystemAbstraction fileSystem = platformModule.fileSystem;
        final File storeDir = platformModule.storeDir;
        final File clusterStateDirectory = createClusterStateDirectory( storeDir, fileSystem );
        final LifeSupport life = platformModule.life;

        logProvider = logging.getInternalLogProvider();
        final Supplier<DatabaseHealth> databaseHealthSupplier = dependencies.provideDependency( DatabaseHealth.class );

        CopiedStoreRecovery copiedStoreRecovery = new CopiedStoreRecovery( config,
                platformModule.kernelExtensions.listFactories(), platformModule.pageCache );

        TransactionIdStore offlineTxIdStore;
        try
        {
            offlineTxIdStore = new ReadOnlyTransactionIdStore( platformModule.pageCache, storeDir );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

        LocalDatabase localDatabase = new LocalDatabase( platformModule.storeDir, copiedStoreRecovery,
                new StoreFiles( new DefaultFileSystemAbstraction() ), platformModule.dataSourceManager,
                platformModule.dependencies.provideDependency( TransactionIdStore.class ), () -> offlineTxIdStore,
                databaseHealthSupplier,
                logProvider );

        life.add( localDatabase );

        MemberIdStorage memberIdStorage = new MemberIdStorage( fileSystem, clusterStateDirectory, CORE_MEMBER_ID_NAME, new MemberIdMarshal(), logProvider );
        MemberId myself;
        try
        {
            myself = memberIdStorage.readState();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

        discoveryService = discoveryServiceFactory.coreDiscoveryService( config, myself, logProvider );

        life.add( dependencies.satisfyDependency( discoveryService ) );

        long logThresholdMillis = config.get( CoreEdgeClusterSettings.unknown_address_logging_throttle );
        int maxQueueSize = config.get( CoreEdgeClusterSettings.outgoing_queue_size );

        final SenderService senderService =
                new SenderService( new RaftChannelInitializer( new CoreReplicatedContentMarshal(), logProvider ),
                        logProvider, platformModule.monitors,
                        maxQueueSize, new NonBlockingChannels() );
        life.add( senderService );

        final MessageLogger<MemberId> messageLogger;
        if ( config.get( CoreEdgeClusterSettings.raft_messages_log_enable ) )
        {
            File logsDir = config.get( GraphDatabaseSettings.logs_directory );
            messageLogger = life.add( new BetterMessageLogger<>( myself, raftMessagesLog( logsDir ) ) );
        }
        else
        {
            messageLogger = new NullMessageLogger<>();
        }

        RaftOutbound raftOutbound =
                new RaftOutbound( discoveryService, senderService, localDatabase, logProvider, logThresholdMillis );
        Outbound<MemberId,RaftMessages.RaftMessage> loggingOutbound = new LoggingOutbound<>(
                raftOutbound, myself, messageLogger );

        consensusModule = new ConsensusModule( myself, platformModule, raftOutbound, clusterStateDirectory, discoveryService );

        dependencies.satisfyDependency( consensusModule.raftMachine() );

        ReplicationModule replicationModule = new ReplicationModule( myself, platformModule, config, consensusModule,
                loggingOutbound, clusterStateDirectory,
                fileSystem, databaseHealthSupplier, logProvider );

        coreStateMachinesModule = new CoreStateMachinesModule( myself, platformModule, clusterStateDirectory,
                databaseHealthSupplier, config, replicationModule.getReplicator(), consensusModule.raftMachine(),
                dependencies, localDatabase );

        this.idGeneratorFactory = coreStateMachinesModule.idGeneratorFactory;
        this.idTypeConfigurationProvider = coreStateMachinesModule.idTypeConfigurationProvider;
        this.labelTokenHolder = coreStateMachinesModule.labelTokenHolder;
        this.propertyKeyTokenHolder = coreStateMachinesModule.propertyKeyTokenHolder;
        this.relationshipTypeTokenHolder = coreStateMachinesModule.relationshipTypeTokenHolder;
        this.lockManager = coreStateMachinesModule.lockManager;
        this.commitProcessFactory = coreStateMachinesModule.commitProcessFactory;

        CoreServerModule coreServerModule = new CoreServerModule( myself, platformModule, consensusModule,
                coreStateMachinesModule, replicationModule, clusterStateDirectory, discoveryService, localDatabase,
                messageLogger );

        editionInvariants( platformModule, dependencies, config, logging, life );

        dependencies.satisfyDependency( lockManager );

        life.add( coreStateMachinesModule.replicatedIdGeneratorFactory );
        life.add( coreServerModule.startupLifecycle );
        life.add( consensusModule.raftTimeoutService() );
        life.add( coreServerModule.membershipWaiterLifecycle );
    }

    private void editionInvariants( PlatformModule platformModule, Dependencies dependencies, Config config,
            LogService logging, LifeSupport life )
    {
        statementLocksFactory = new StatementLocksFactorySelector( lockManager, config, logging ).select();

        dependencies.satisfyDependency(
                createKernelData( platformModule.fileSystem, platformModule.pageCache, platformModule.storeDir,
                        config, platformModule.graphDatabaseFacade, life ) );

        life.add( dependencies.satisfyDependency( createAuthManager( config, logging ) ) );

        ioLimiter = new ConfigurableIOLimiter( platformModule.config );

        headerInformationFactory = createHeaderInformationFactory();

        schemaWriteGuard = createSchemaWriteGuard();

        transactionStartTimeout = config.get( GraphDatabaseSettings.transaction_start_timeout );

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

    private File createClusterStateDirectory( File dir, FileSystemAbstraction fileSystem )
    {
        File raftLogDir = new File( dir, CLUSTER_STATE_DIRECTORY_NAME );

        try
        {
            fileSystem.mkdirs( raftLogDir );
            return raftLogDir;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private static PrintWriter raftMessagesLog( File logsDir )
    {
        //noinspection ResultOfMethodCallIgnored
        logsDir.mkdirs();
        try
        {

            return new PrintWriter( new FileOutputStream( new File( logsDir, "raft-messages.log" ), true ) );
        }
        catch ( FileNotFoundException e )
        {
            throw new RuntimeException( e );
        }
    }

    private SchemaWriteGuard createSchemaWriteGuard()
    {
        return () -> {};
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
        life.addLifecycleListener( ( instance, from, to ) -> {
            if ( instance instanceof DatabaseAvailability && LifecycleStatus.STARTED.equals( to ) )
            {
                doAfterRecoveryAndStartup( databaseInfo, dependencyResolver );
            }
        } );
    }

    @Override
    protected void doAfterRecoveryAndStartup( DatabaseInfo databaseInfo, DependencyResolver dependencyResolver )
    {
        super.doAfterRecoveryAndStartup( databaseInfo, dependencyResolver );

        if ( dependencyResolver.resolveDependency( RaftMachine.class ).isLeader() )
        {
            new RemoveOrphanConstraintIndexesOnStartup(
                    dependencyResolver.resolveDependency( NeoStoreDataSource.class ).getKernel(),
                    dependencyResolver.resolveDependency( LogService.class ).getInternalLogProvider() ).perform();
        }
    }

    @Override
    protected SessionTracker createSessionTracker()
    {
        return new StandardSessionTracker();
    }
}
