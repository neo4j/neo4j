/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.backup;

import java.io.File;
import java.nio.file.Path;
import java.time.Clock;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyClient;
import org.neo4j.causalclustering.catchup.tx.TransactionLogCatchUpFactory;
import org.neo4j.causalclustering.catchup.tx.TxPullClient;
import org.neo4j.causalclustering.handlers.NoOpPipelineHandlerAppender;
import org.neo4j.causalclustering.handlers.PipelineHandlerAppender;
import org.neo4j.com.storecopy.FileMovePropagator;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.admin.RealOutsideWorld;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.helpers.OptionalHostnamePort;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.pagecache.ConfigurableStandalonePageCacheFactory;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.time.SystemNanoClock;

/**
 * Use this builder when you want to see if backup code works, but don't need all the details of how it works.
 */
public class BackupStrategyCoordinatorBuilder
{
    private Optional<OutsideWorld> outsideWorld = Optional.empty();
    private Optional<AddressResolutionHelper> addressResolutionHelper = Optional.empty();
    private Optional<ProgressMonitorFactory> progressMonitorFactory = Optional.empty();
    private Optional<LogProvider> logProvider = Optional.empty();
    private Optional<BackupRecoveryService> backupRecoveryService = Optional.empty();
    private Optional<Config> config = Optional.empty();
    private Optional<PageCache> pageCache = Optional.empty();
    private Optional<FileSystemAbstraction> fileSystemAbstraction = Optional.empty();
    private Optional<FileMovePropagator> fileMovePropagator = Optional.empty();
    private Optional<ClearIdService> clearIdService = Optional.empty();
    private Optional<IdGeneratorWrapper> idGeneratorWrapper = Optional.empty();
    private Optional<StoreCopyClient> storeCopyClient = Optional.empty();
    private Optional<CatchUpClient> catchUpClient = Optional.empty();
    private Optional<Clock> clock = Optional.empty();
    private Optional<Monitors> monitors = Optional.empty();
    private Optional<PipelineHandlerAppender> pipelineHandlerAppender = Optional.empty();
    private Optional<RemoteStore> remoteStore = Optional.empty();
    private Optional<TransactionLogCatchUpFactory> transactionLogCatchUpFactory = Optional.empty();
    private Optional<TxPullClient> txPullClient = Optional.empty();
    private Optional<BackupProtocolService> backupProtocolService = Optional.empty();
    private Optional<ConsistencyCheckService> consistencyCheckService = Optional.empty();
    private Optional<ConsistencyFlags> consistencyFlags = Optional.empty();
    private Optional<Boolean> checkGraph = Optional.empty();
    private Optional<Boolean> checkIndexes = Optional.empty();
    private Optional<Boolean> checkLabelScanStore = Optional.empty();
    private Optional<Boolean> checkPropertyOwners = Optional.empty();
    private Optional<Long> timeout = Optional.empty();
    private Optional<Boolean> consistencyCheck = Optional.empty();
    private Optional<Boolean> fallbackToFull = Optional.empty();
    private Optional<Path> reportDir = Optional.empty();
    private Optional<Path> neo4jHome = Optional.empty();
    private Optional<Path> additionalConfig = Optional.empty();
    private Optional<String> backupName = Optional.empty();
    private Optional<OptionalHostnamePort> optionalHostnamePort = Optional.empty();
    private Optional<String> hostname = Optional.empty();
    private Optional<Integer> port = Optional.empty();
    private Optional<Integer> upperRangePort = Optional.empty();

    public enum StrategyEnum
    {
        CC( BackupStrategyCoordinatorBuilder::getCausalClusteringBackupStrategy ),
        HA( BackupStrategyCoordinatorBuilder::getHaBackupStrategy );

        private final Function<BackupStrategyCoordinatorBuilder,BackupStrategy> solution;

        public Function<BackupStrategyCoordinatorBuilder,BackupStrategy> getSolution()
        {
            return builder -> wrappedInLifecycle( solution.apply( builder ) );
        }

        StrategyEnum( Function<BackupStrategyCoordinatorBuilder,BackupStrategy> solution )
        {
            this.solution = solution;
        }

        public static BackupStrategy wrappedInLifecycle( BackupStrategy sourceStrategy )
        {
            return new BackupStrategy()
            {
                private PotentiallyErroneousState<BackupStageOutcome> wrapped( Supplier<PotentiallyErroneousState<BackupStageOutcome>> method )
                {
                    LifeSupport lifeSupport = new LifeSupport();
                    lifeSupport.add( sourceStrategy );
                    System.out.println("Starting");
                    lifeSupport.start();
                    PotentiallyErroneousState<BackupStageOutcome> state = method.get();
                    lifeSupport.shutdown();
                    return state;
                }

                @Override
                public PotentiallyErroneousState<BackupStageOutcome> performIncrementalBackup( File desiredBackupLocation, Config config,
                        OptionalHostnamePort userProvidedAddress )
                {
                    return wrapped( () -> sourceStrategy.performIncrementalBackup( desiredBackupLocation, config, userProvidedAddress ) );
                }

                @Override
                public PotentiallyErroneousState<BackupStageOutcome> performFullBackup( File desiredBackupLocation, Config config,
                        OptionalHostnamePort userProvidedAddress )
                {
                    return wrapped( () -> sourceStrategy.performFullBackup( desiredBackupLocation, config, userProvidedAddress ) );
                }

                @Override
                public void init() throws Throwable
                {
                    sourceStrategy.init();
                }

                @Override
                public void start() throws Throwable
                {
                    sourceStrategy.start();
                }

                @Override
                public void stop() throws Throwable
                {
                    sourceStrategy.stop();
                }

                @Override
                public void shutdown() throws Throwable
                {
                    sourceStrategy.shutdown();
                }
            };
        }
    }

    /**
     * Use this for testing, as it can be used with the parametrised method above for creating single backup strategy end-to-end solutions
     *
     * @param singleStrategy a single backup strategy which can be CC, HA or any kind of mock strategy
     * @return a service that will handle all the features supported by the backup tool, but bounded to only one strategy
     */
    public BackupStrategyCoordinator fromSingleStrategy( BackupStrategyWrapper singleStrategy )
    {
        return new BackupStrategyCoordinator( getConsistencyCheckService(), getOutsideWorld(), getLogProvider(), getProgressMonitorFactory(),
                Collections.singletonList( singleStrategy ) );
    }

    /**
     * Use this for testing, as it can be used with the parametrised method above for creating single backup strategy end-to-end solutions
     *
     * @param singleStrategy a single backup strategy which can be CC, HA or any kind of mock strategy
     * @return a service that will handle all the features supported by the backup tool, but bounded to only one strategy
     */
    public BackupStrategyCoordinator fromSingleStrategy( BackupStrategy singleStrategy )
    {
        return fromSingleStrategy( strategyWrapperFromStrategy( singleStrategy ) );
    }

    public BackupStrategyCoordinator fromSingleStrategy( StrategyEnum strategyEnum )
    {
        return fromSingleStrategy( strategyEnum.solution.apply( this ) );
    }

    /**
     * Convenience method in the event that you want to use the builder only partially for causal clustering
     *
     * @return a causal clustering backup strategy which can be used with {@link BackupStrategyCoordinatorBuilder#fromSingleStrategy(BackupStrategyWrapper)}
     */
    public CausalClusteringBackupStrategy getCausalClusteringBackupStrategy()
    {
        return new CausalClusteringBackupStrategy( getBackupDelegator(), getAddressResolutionHelper() );
    }

    public HaBackupStrategy getHaBackupStrategy()
    {
        return new HaBackupStrategy( getBackupProtocolService(), getAddressResolutionHelper(), getTimeout() );
    }

    BackupStrategyWrapper strategyWrapperFromStrategy( StrategyEnum strategyEnum )
    {
        return strategyWrapperFromStrategy( strategyEnum.solution.apply( this ) );
    }

    BackupStrategyWrapper strategyWrapperFromStrategy( BackupStrategy backupStrategy )
    {
        return new BackupStrategyWrapper( backupStrategy, getBackupCopyService(), getPageCache(), getConfig(), getBackupRecoveryService(), getLogProvider() );
    }

    public OnlineBackupContext getOnlineBackupContext()
    {
        return new OnlineBackupContext( getOnlineBackupRequiredArguments(), getConfig(), getConsistencyFlags() );
    }

    // Public methods above, Builder methods below

    /**
     * There is no single field `backup directory`. It is recommended that you set the neo4jHome location then provide a name.
     * This method is reverse engineering the above - neo4jHome becomes the parent of backupDirectory and backup name will
     * be the name of the directory for backupDirectory
     *
     * @param backupDirectory the location where the backup should be stored
     * @return The mutable builder which is the same instance as that which was operated on
     */
    public BackupStrategyCoordinatorBuilder withBackupDirectory( Path backupDirectory )
    {
        return this.withNeo4jHome( backupDirectory.getParent() ).withBackupName( backupDirectory.toFile().getName() );
    }

    public BackupStrategyCoordinatorBuilder withBackupName( String backupName )
    {
        this.backupName = Optional.of( backupName );
        return this;
    }

    public BackupStrategyCoordinatorBuilder withNeo4jHome( Path neo4jHome )
    {
        this.neo4jHome = Optional.of( neo4jHome );
        return this;
    }

    public BackupStrategyCoordinatorBuilder withOptionalHostnamePort( OptionalHostnamePort optionalHostnamePort )
    {
        this.optionalHostnamePort = Optional.of( optionalHostnamePort );
        return this;
    }

    public BackupStrategyCoordinatorBuilder withOutsideWorld( OutsideWorld outsideWorld )
    {
        this.outsideWorld = Optional.of( outsideWorld );
        return this;
    }

    public BackupStrategyCoordinatorBuilder withAddressResolutionHelper( AddressResolutionHelper addressResolutionHelper )
    {
        this.addressResolutionHelper = Optional.of( addressResolutionHelper );
        return this;
    }

    public BackupStrategyCoordinatorBuilder withProgressMonitorFactory( ProgressMonitorFactory progressMonitorFactory )
    {
        this.progressMonitorFactory = Optional.of( progressMonitorFactory );
        return this;
    }

    public BackupStrategyCoordinatorBuilder withLogProvider( LogProvider logProvider )
    {
        this.logProvider = Optional.of( logProvider );
        return this;
    }

    public BackupStrategyCoordinatorBuilder withBackupRecoveryService( BackupRecoveryService backupRecoveryService )
    {
        this.backupRecoveryService = Optional.of( backupRecoveryService );
        return this;
    }

    public BackupStrategyCoordinatorBuilder withConfig( Config config )
    {
        this.config = Optional.of( config );
        return this;
    }

    public BackupStrategyCoordinatorBuilder withHostname( String hostname )
    {
        this.hostname = Optional.of( hostname );
        return this;
    }

    public BackupStrategyCoordinatorBuilder withPort( Integer port )
    {
        this.port = Optional.of( port );
        return this;
    }

    public BackupStrategyCoordinatorBuilder withConsistencyCheck( Boolean consistencyCheck )
    {
        this.consistencyCheck = Optional.of( consistencyCheck );
        return this;
    }

    public BackupStrategyCoordinatorBuilder withTimeout( Long timeout )
    {
        this.timeout = Optional.of( timeout );
        return this;
    }

    public BackupStrategyCoordinatorBuilder withPageCache( PageCache pageCache )
    {
        this.pageCache = Optional.of( pageCache );
        return this;
    }

    public BackupStrategyCoordinatorBuilder withFileSystemAbstraction( FileSystemAbstraction fileSystemAbstraction )
    {
        this.fileSystemAbstraction = Optional.of( fileSystemAbstraction );
        return this;
    }

    public BackupStrategyCoordinatorBuilder withFallbackToFull( Boolean fallbackToFull )
    {
        this.fallbackToFull = Optional.of( fallbackToFull );
        return this;
    }

    public BackupStrategyCoordinatorBuilder withFileMovePropagator( FileMovePropagator fileMovePropagator )
    {
        this.fileMovePropagator = Optional.of( fileMovePropagator );
        return this;
    }

    public BackupStrategyCoordinatorBuilder withClearIdService( ClearIdService clearIdService )
    {
        this.clearIdService = Optional.of( clearIdService );
        return this;
    }

    public BackupStrategyCoordinatorBuilder withIdGeneratorWrapper( IdGeneratorWrapper idGeneratorWrapper )
    {
        this.idGeneratorWrapper = Optional.of( idGeneratorWrapper );
        return this;
    }

    public BackupStrategyCoordinatorBuilder withStoreCopyClient( StoreCopyClient storeCopyClient )
    {
        this.storeCopyClient = Optional.of( storeCopyClient );
        return this;
    }

    public BackupStrategyCoordinatorBuilder withCatchUpClient( CatchUpClient catchUpClient )
    {
        this.catchUpClient = Optional.of( catchUpClient );
        return this;
    }

    public BackupStrategyCoordinatorBuilder withClock( Clock clock )
    {
        this.clock = Optional.of( clock );
        return this;
    }

    public BackupStrategyCoordinatorBuilder withMonitors( Monitors monitors )
    {
        this.monitors = Optional.of( monitors );
        return this;
    }

    public BackupStrategyCoordinatorBuilder withPipelineHandlerAppender( PipelineHandlerAppender pipelineHandlerAppender )
    {
        this.pipelineHandlerAppender = Optional.of( pipelineHandlerAppender );
        return this;
    }

    public BackupStrategyCoordinatorBuilder withRemoteStore( RemoteStore remoteStore )
    {
        this.remoteStore = Optional.of( remoteStore );
        return this;
    }

    public BackupStrategyCoordinatorBuilder withTransactionLogCatchUpFactory( TransactionLogCatchUpFactory transactionLogCatchUpFactory )
    {
        this.transactionLogCatchUpFactory = Optional.of( transactionLogCatchUpFactory );
        return this;
    }

    public BackupStrategyCoordinatorBuilder withTxPullClient( TxPullClient txPullClient )
    {
        this.txPullClient = Optional.of( txPullClient );
        return this;
    }

    public BackupStrategyCoordinatorBuilder withBackupProtocolService( BackupProtocolService backupProtocolService )
    {
        this.backupProtocolService = Optional.of( backupProtocolService );
        return this;
    }

    public BackupStrategyCoordinatorBuilder withConsistencyCheckService( ConsistencyCheckService consistencyCheckService )
    {
        this.consistencyCheckService = Optional.of( consistencyCheckService );
        return this;
    }

    // Builder methods above, private getters below

    private ConsistencyFlags getConsistencyFlags()
    {
        return consistencyFlags.orElse( new ConsistencyFlags( getCheckGraph(), getCheckIndexes(), getCheckLabelScanStore(), getCheckPropertyOwners() ) );
    }

    private boolean getCheckPropertyOwners()
    {
        return checkPropertyOwners.orElse( false );
    }

    private boolean getCheckLabelScanStore()
    {
        return checkLabelScanStore.orElse( false );
    }

    private boolean getCheckIndexes()
    {
        return checkIndexes.orElse( false );
    }

    private Boolean getCheckGraph()
    {
        return checkGraph.orElse( false );
    }

    private OnlineBackupRequiredArguments getOnlineBackupRequiredArguments()
    {
        return new OnlineBackupRequiredArguments( getBackupAddress(), getNeo4jHome(), getBackupName(), getFallbackToFull(), getIsDoConsistencyCheck(),
                getTimeout(), getAdditionalConfig(), getReportDir() );
    }

    private Path getReportDir()
    {
        return reportDir.orElse( getNeo4jHome().resolve( "logs" ) ); // TODO verify
    }

    private Optional<Path> getAdditionalConfig()
    {
        return additionalConfig;
    }

    private Long getTimeout()
    {
        return timeout.orElse( 5000L );
    }

    private Boolean getIsDoConsistencyCheck()
    {
        return consistencyCheck.orElse( false );
    }

    private Boolean getFallbackToFull()
    {
        return fallbackToFull.orElse( false );
    }

    private Path getNeo4jHome()
    {
        return neo4jHome.orElseThrow(
                () -> new IllegalStateException( "NEO4J Home must be provided to builder in order to perform backup. This can be an empty directory" ) );
    }

    private String getBackupName()
    {
        return backupName.orElseThrow(
                () -> new IllegalStateException( "Backup needs a name. If you don't care about name then use withBackupDirectory instead." ) );
    }

    private OptionalHostnamePort getBackupAddress()
    {
        return optionalHostnamePort.orElse( new OptionalHostnamePort( hostname, port, upperRangePort ) );
    }

    private ProgressMonitorFactory getProgressMonitorFactory()
    {
        return progressMonitorFactory.orElse( ProgressMonitorFactory.NONE );
    }

    private LogProvider getLogProvider()
    {
        return logProvider.orElse( NullLogProvider.getInstance() );
    }

    private BackupRecoveryService getBackupRecoveryService()
    {
        return backupRecoveryService.orElse( new BackupRecoveryService() );
    }

    private Config getConfig()
    {
        return config.orElse( Config.defaults() );
    }

    private PageCache getPageCache()
    {
        return pageCache.orElse( ConfigurableStandalonePageCacheFactory.createPageCache( getFileSystemAbstraction(), getConfig() ) );
    }

    private FileSystemAbstraction getFileSystemAbstraction()
    {
        return fileSystemAbstraction.orElse( new DefaultFileSystemAbstraction() );
    }

    private BackupCopyService getBackupCopyService()
    {
        return new BackupCopyService( getPageCache(), getFileMovePropagator() );
    }

    private FileMovePropagator getFileMovePropagator()
    {
        return fileMovePropagator.orElse( new FileMovePropagator() );
    }

    private AddressResolutionHelper getAddressResolutionHelper()
    {
        return addressResolutionHelper.orElse( new AddressResolutionHelper() );
    }

    private BackupDelegator getBackupDelegator()
    {
        return new BackupDelegator( getRemoteStore(), getCatchUpClient(), getStoreCopyClient(), getClearIdService() );
    }

    private ClearIdService getClearIdService()
    {
        return clearIdService.orElse( new ClearIdService( getIdGeneratorWrapper() ) );
    }

    private IdGeneratorWrapper getIdGeneratorWrapper()
    {
        return idGeneratorWrapper.orElse( new IdGeneratorWrapper() );
    }

    private StoreCopyClient getStoreCopyClient()
    {
        return storeCopyClient.orElse( new StoreCopyClient( getCatchUpClient(), getLogProvider() ) );
    }

    private CatchUpClient getCatchUpClient()
    {
        long seconds = 1000;
        long inactivityTimeoutMillis = 10 * seconds;
        return catchUpClient.orElse( new CatchUpClient( getLogProvider(), getClock(), inactivityTimeoutMillis, getMonitors(), getPipelineHandlerAppender() ) );
    }

    private Clock getClock()
    {
        return clock.orElse( SystemNanoClock.systemUTC() );
    }

    private Monitors getMonitors()
    {
        return monitors.orElse( new Monitors() );
    }

    private PipelineHandlerAppender getPipelineHandlerAppender()
    {
        return pipelineHandlerAppender.orElse( new NoOpPipelineHandlerAppender( null, null ) ); // instances not needed, because it's no-op
    }

    private RemoteStore getRemoteStore()
    {
        return remoteStore.orElse( new RemoteStore( getLogProvider(), getFileSystemAbstraction(), getPageCache(), getStoreCopyClient(), getTxPullClient(),
                getTransactionLogCatchUpFactory(), getMonitors() ) );
    }

    private TransactionLogCatchUpFactory getTransactionLogCatchUpFactory()
    {
        return transactionLogCatchUpFactory.orElse( new TransactionLogCatchUpFactory() );
    }

    private TxPullClient getTxPullClient()
    {
        return txPullClient.orElse( new TxPullClient( getCatchUpClient(), getMonitors() ) );
    }

    private BackupProtocolService getBackupProtocolService()
    {
        return backupProtocolService.orElse( new BackupProtocolService() );
    }

    private OutsideWorld getOutsideWorld()
    {
        return outsideWorld.orElse( new RealOutsideWorld() );
    }

    private ConsistencyCheckService getConsistencyCheckService()
    {
        return consistencyCheckService.orElse( new ConsistencyCheckService() );
    }
}
