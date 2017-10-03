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

import java.io.OutputStream;
import java.time.Clock;

import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyClient;
import org.neo4j.causalclustering.catchup.tx.TransactionLogCatchUpFactory;
import org.neo4j.causalclustering.catchup.tx.TxPullClient;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.handlers.NoOpPipelineHandlerAppenderFactory;
import org.neo4j.causalclustering.handlers.PipelineHandlerAppender;
import org.neo4j.causalclustering.handlers.PipelineHandlerAppenderFactory;
import org.neo4j.helpers.Service;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ssl.SslPolicyLoader;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.pagecache.ConfigurableStandalonePageCacheFactory;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.ssl.SslPolicy;

/**
 * The dependencies for the backup strategies require a valid configuration for initialisation.
 * By having this factory we can wait until the configuration has been loaded and the provide all the classes required for backups that are dependant on the
 * config.
 */
abstract class AbstractBackupSupportingClassesFactory
{
    private static final long INACTIVITY_TIMEOUT_MILLIS = 60 * 1000;

    private final LogProvider logProvider;
    private final Clock clock;
    private final Monitors monitors;
    private final FileSystemAbstraction fileSystemAbstraction;
    private final TransactionLogCatchUpFactory transactionLogCatchUpFactory;
    private final OutputStream logDestination;

    AbstractBackupSupportingClassesFactory( BackupModuleResolveAtRuntime backupModuleResolveAtRuntime )
    {
        this.logProvider = backupModuleResolveAtRuntime.getLogProvider();
        this.clock = backupModuleResolveAtRuntime.getClock();
        this.monitors = backupModuleResolveAtRuntime.getMonitors();
        this.fileSystemAbstraction = backupModuleResolveAtRuntime.getFileSystemAbstraction();
        this.transactionLogCatchUpFactory = backupModuleResolveAtRuntime.getTransactionLogCatchUpFactory();
        this.logDestination = backupModuleResolveAtRuntime.getOutsideWorld().errorStream();
    }

    /**
     * Resolves all the backing solutions used for performing various backups while sharing key classes and configuration
     *
     * @param config user selected during runtime for performing backups
     * @return grouping of instances used for performing backups
     */
    BackupSupportingClasses createSupportingClassesForBackupStrategies( Config config )
    {
        PageCache pageCache = ConfigurableStandalonePageCacheFactory.createPageCache( fileSystemAbstraction, config );
        return new BackupSupportingClasses( backupDelegatorFormConfig( pageCache, config ), haFromConfig( pageCache ) );
    }

    private BackupProtocolService haFromConfig( PageCache pageCache )
    {
        return new BackupProtocolService( () -> fileSystemAbstraction, logProvider, logDestination, monitors, pageCache );
    }

    private BackupDelegator backupDelegatorFormConfig( PageCache pageCache, Config config )
    {
        PipelineHandlerAppenderFactory pipelineHandlerAppenderFactory = getPipelineHandlerAppenderFactory();
        Dependencies dependencies = getDependencies();
        PipelineHandlerAppender pipelineHandlerAppender = pipelineHandlerAppenderFactory.create( config, dependencies, logProvider );
        CatchUpClient catchUpClient = new CatchUpClient( logProvider, clock, INACTIVITY_TIMEOUT_MILLIS, monitors, pipelineHandlerAppender );
        TxPullClient txPullClient = new TxPullClient( catchUpClient, monitors );
        StoreCopyClient storeCopyClient = new StoreCopyClient( catchUpClient, logProvider );

        RemoteStore remoteStore =
                new RemoteStore( logProvider, fileSystemAbstraction, pageCache, storeCopyClient, txPullClient, transactionLogCatchUpFactory, monitors );

        return backupDelegator( remoteStore, catchUpClient, storeCopyClient );
    }

    private static BackupDelegator backupDelegator( RemoteStore remoteStore, CatchUpClient catchUpClient, StoreCopyClient storeCopyClient )
    {
        return new BackupDelegator( remoteStore, catchUpClient, storeCopyClient, new ClearIdService( new IdGeneratorWrapper() ) );
    }

    abstract Dependencies getDependencies();

    abstract PipelineHandlerAppenderFactory getPipelineHandlerAppenderFactory();

    abstract int getPriority();
}
