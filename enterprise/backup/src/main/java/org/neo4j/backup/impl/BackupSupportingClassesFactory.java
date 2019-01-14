/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.backup.impl;

import java.io.OutputStream;
import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.CatchupClientBuilder;
import org.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyClient;
import org.neo4j.causalclustering.catchup.tx.TransactionLogCatchUpFactory;
import org.neo4j.causalclustering.catchup.tx.TxPullClient;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.SupportedProtocolCreator;
import org.neo4j.causalclustering.handlers.PipelineWrapper;
import org.neo4j.causalclustering.handlers.VoidPipelineWrapperFactory;
import org.neo4j.causalclustering.helper.ExponentialBackoffStrategy;
import org.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import org.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;
import org.neo4j.causalclustering.protocol.handshake.ModifierSupportedProtocols;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.pagecache.ConfigurableStandalonePageCacheFactory;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;

/**
 * The dependencies for the backup strategies require a valid configuration for initialisation.
 * By having this factory we can wait until the configuration has been loaded and the provide all the classes required
 * for backups that are dependant on the config.
 */
public class BackupSupportingClassesFactory
{
    protected final LogProvider logProvider;
    protected final Clock clock;
    protected final Monitors monitors;
    protected final FileSystemAbstraction fileSystemAbstraction;
    protected final TransactionLogCatchUpFactory transactionLogCatchUpFactory;
    protected final OutputStream logDestination;
    protected final OutsideWorld outsideWorld;

    protected BackupSupportingClassesFactory( BackupModule backupModule )
    {
        this.logProvider = backupModule.getLogProvider();
        this.clock = backupModule.getClock();
        this.monitors = backupModule.getMonitors();
        this.fileSystemAbstraction = backupModule.getFileSystemAbstraction();
        this.transactionLogCatchUpFactory = backupModule.getTransactionLogCatchUpFactory();
        this.logDestination = backupModule.getOutsideWorld().outStream();
        this.outsideWorld = backupModule.getOutsideWorld();
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
        monitors.addMonitorListener( new BackupOutputMonitor( outsideWorld ) );
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
        CatchUpClient catchUpClient = catchUpClient( config );
        TxPullClient txPullClient = new TxPullClient( catchUpClient, monitors );
        ExponentialBackoffStrategy backOffStrategy =
                new ExponentialBackoffStrategy( 1, config.get( CausalClusteringSettings.store_copy_backoff_max_wait ).toMillis(), TimeUnit.MILLISECONDS );
        StoreCopyClient storeCopyClient = new StoreCopyClient( catchUpClient, monitors, logProvider, backOffStrategy );

        RemoteStore remoteStore = new RemoteStore(
                logProvider, fileSystemAbstraction, pageCache, storeCopyClient,
                txPullClient,
                transactionLogCatchUpFactory, config, monitors );

        return backupDelegator( remoteStore, catchUpClient, storeCopyClient );
    }

    protected PipelineWrapper createPipelineWrapper( Config config )
    {
        return new VoidPipelineWrapperFactory().forClient( config, null, logProvider, OnlineBackupSettings.ssl_policy );
    }

    private CatchUpClient catchUpClient( Config config )
    {
        SupportedProtocolCreator supportedProtocolCreator = new SupportedProtocolCreator( config, logProvider );
        ApplicationSupportedProtocols supportedCatchupProtocols = supportedProtocolCreator.createSupportedCatchupProtocol();
        Collection<ModifierSupportedProtocols> supportedModifierProtocols = supportedProtocolCreator.createSupportedModifierProtocols();
        NettyPipelineBuilderFactory clientPipelineBuilderFactory = new NettyPipelineBuilderFactory( createPipelineWrapper( config ) );
        Duration handshakeTimeout = config.get( CausalClusteringSettings.handshake_timeout );
        long inactivityTimeoutMillis = TimeUnit.MINUTES.toMillis( 1 );
        return new CatchupClientBuilder( supportedCatchupProtocols, supportedModifierProtocols, clientPipelineBuilderFactory, handshakeTimeout,
                inactivityTimeoutMillis, logProvider, clock )
                .build();
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
