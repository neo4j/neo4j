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
package org.neo4j.backup.impl;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

import java.io.OutputStream;
import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.CatchUpResponseHandler;
import org.neo4j.causalclustering.catchup.CatchupProtocolClientInstaller;
import org.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyClient;
import org.neo4j.causalclustering.catchup.tx.TransactionLogCatchUpFactory;
import org.neo4j.causalclustering.catchup.tx.TxPullClient;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.SupportedProtocolCreator;
import org.neo4j.causalclustering.handlers.PipelineWrapper;
import org.neo4j.causalclustering.handlers.VoidPipelineWrapperFactory;
import org.neo4j.causalclustering.helper.ExponentialBackoffStrategy;
import org.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import org.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocols;
import org.neo4j.causalclustering.protocol.Protocol.ModifierProtocols;
import org.neo4j.causalclustering.protocol.ProtocolInstaller;
import org.neo4j.causalclustering.protocol.ProtocolInstallerRepository;
import org.neo4j.causalclustering.protocol.handshake.ApplicationProtocolRepository;
import org.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;
import org.neo4j.causalclustering.protocol.handshake.HandshakeClientInitializer;
import org.neo4j.causalclustering.protocol.handshake.ModifierProtocolRepository;
import org.neo4j.causalclustering.protocol.handshake.ModifierSupportedProtocols;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.pagecache.ConfigurableStandalonePageCacheFactory;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;

import static java.util.Collections.singletonList;

/**
 * The dependencies for the backup strategies require a valid configuration for initialisation.
 * By having this factory we can wait until the configuration has been loaded and the provide all the classes required
 * for backups that are dependant on the config.
 */
public class BackupSupportingClassesFactory
{
    private static final long INACTIVITY_TIMEOUT_MILLIS = 60 * 1000;

    protected final LogProvider logProvider;
    protected final Clock clock;
    protected final Monitors monitors;
    protected final FileSystemAbstraction fileSystemAbstraction;
    protected final TransactionLogCatchUpFactory transactionLogCatchUpFactory;
    protected final OutputStream logDestination;

    protected BackupSupportingClassesFactory( BackupModule backupModule )
    {
        this.logProvider = backupModule.getLogProvider();
        this.clock = backupModule.getClock();
        this.monitors = backupModule.getMonitors();
        this.fileSystemAbstraction = backupModule.getFileSystemAbstraction();
        this.transactionLogCatchUpFactory = backupModule.getTransactionLogCatchUpFactory();
        this.logDestination = backupModule.getOutsideWorld().outStream();
    }

    /**
     * Resolves all the backing solutions used for performing various backups while sharing key classes and
     * configuration.
     *
     * @param config user selected during runtime for performing backups
     * @return grouping of instances used for performing backups
     */
    BackupSupportingClasses createSupportingClasses( Config config )
    {
        PageCache pageCache = createPageCache( fileSystemAbstraction, config );
        return new BackupSupportingClasses(
                backupDelegatorFromConfig( pageCache, config ),
                haFromConfig( pageCache ),
                pageCache );
    }

    private BackupProtocolService haFromConfig( PageCache pageCache )
    {
        Supplier<FileSystemAbstraction> fileSystemSupplier = () -> fileSystemAbstraction;
        return new BackupProtocolService( fileSystemSupplier, logProvider, logDestination, monitors, pageCache );
    }

    private BackupDelegator backupDelegatorFromConfig( PageCache pageCache, Config config )
    {
        CatchUpClient catchUpClient = new CatchUpClient( logProvider, clock, INACTIVITY_TIMEOUT_MILLIS, channelInitializer( config ) );
        TxPullClient txPullClient = new TxPullClient( catchUpClient, monitors );
        ExponentialBackoffStrategy backOffStrategy =
                new ExponentialBackoffStrategy( 1, config.get( CausalClusteringSettings.store_copy_backoff_max_wait ).toMillis(), TimeUnit.MILLISECONDS );
        StoreCopyClient storeCopyClient = new StoreCopyClient( catchUpClient, logProvider, backOffStrategy );

        RemoteStore remoteStore = new RemoteStore(
                logProvider, fileSystemAbstraction, pageCache, storeCopyClient,
                txPullClient,
                transactionLogCatchUpFactory, config, monitors );

        return backupDelegator( remoteStore, catchUpClient, storeCopyClient );
    }

    protected PipelineWrapper createPipelineWrapper( Config config )
    {
        return new VoidPipelineWrapperFactory().forServer( config, null, logProvider );
    }

    private Function<CatchUpResponseHandler,ChannelInitializer<SocketChannel>> channelInitializer( Config config )
    {
        SupportedProtocolCreator supportedProtocolCreator = new SupportedProtocolCreator( config, logProvider );
        ApplicationSupportedProtocols supportedCatchupProtocols = supportedProtocolCreator.createSupportedCatchupProtocol();
        Collection<ModifierSupportedProtocols> supportedModifierProtocols = supportedProtocolCreator.createSupportedModifierProtocols();

        ApplicationProtocolRepository applicationProtocolRepository = new ApplicationProtocolRepository( ApplicationProtocols.values(),
                supportedCatchupProtocols );
        ModifierProtocolRepository modifierProtocolRepository = new ModifierProtocolRepository( ModifierProtocols.values(), supportedModifierProtocols );

        NettyPipelineBuilderFactory clientPipelineBuilderFactory = new NettyPipelineBuilderFactory( createPipelineWrapper( config ) );

        return handler -> {
            ProtocolInstallerRepository<ProtocolInstaller.Orientation.Client> protocolInstallerRepository = new ProtocolInstallerRepository<>(
                    singletonList( new CatchupProtocolClientInstaller.Factory( clientPipelineBuilderFactory, logProvider, handler ) ),
                    ModifierProtocolInstaller.allClientInstallers );
            Duration handshakeTimeout = config.get( CausalClusteringSettings.handshake_timeout );
            return new HandshakeClientInitializer( applicationProtocolRepository, modifierProtocolRepository, protocolInstallerRepository,
                    clientPipelineBuilderFactory, handshakeTimeout , logProvider );
        };
    }

    private static BackupDelegator backupDelegator(
            RemoteStore remoteStore, CatchUpClient catchUpClient, StoreCopyClient storeCopyClient )
    {
        return new BackupDelegator( remoteStore, catchUpClient, storeCopyClient );
    }

    private static PageCache createPageCache( FileSystemAbstraction fileSystemAbstraction, Config config )
    {
        return ConfigurableStandalonePageCacheFactory.createPageCache( fileSystemAbstraction, config );
    }
}
