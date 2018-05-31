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
package org.neo4j.backup;

import java.net.URI;
import java.util.function.Supplier;

import org.neo4j.backup.impl.BackupImpl;
import org.neo4j.backup.impl.BackupServer;
import org.neo4j.cluster.BindingListener;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.cluster.com.BindingNotifier;
import org.neo4j.cluster.member.ClusterMemberAvailability;
import org.neo4j.cluster.member.ClusterMemberEvents;
import org.neo4j.cluster.member.ClusterMemberListener;
import org.neo4j.com.ServerUtil;
import org.neo4j.com.monitor.RequestMonitor;
import org.neo4j.com.storecopy.StoreCopyServer;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.kernel.impl.util.UnsatisfiedDependencyException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;

import static org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings.online_backup_server;

/**
 * @deprecated This will be moved to an internal package in the future.
 */
@Deprecated
public class OnlineBackupKernelExtension implements Lifecycle
{
    private Object startBindingListener;
    private Object bindingListener;

    @Deprecated
    public interface BackupProvider
    {
        TheBackupInterface newBackup();
    }

    // This is the role used to announce that a cluster member can handle backups
    public static final String BACKUP = "backup";
    // In this context, the IPv4 zero-address is understood as "any address on this host."
    public static final String INADDR_ANY = "0.0.0.0";

    private final Config config;
    private final GraphDatabaseAPI graphDatabaseAPI;
    private final LogProvider logProvider;
    private final Monitors monitors;
    private BackupServer server;
    private final BackupProvider backupProvider;
    private volatile URI me;

    public OnlineBackupKernelExtension( Config config, final GraphDatabaseAPI graphDatabaseAPI, final LogProvider logProvider,
                                        final Monitors monitors, final NeoStoreDataSource neoStoreDataSource,
                                        final Supplier<CheckPointer> checkPointerSupplier,
                                        final Supplier<TransactionIdStore> transactionIdStoreSupplier,
                                        final Supplier<LogicalTransactionStore> logicalTransactionStoreSupplier,
                                        final Supplier<LogFileInformation> logFileInformationSupplier,
                                        final FileSystemAbstraction fileSystemAbstraction,
                                        final PageCache pageCache,
                                        final StoreCopyCheckPointMutex storeCopyCheckPointMutex )
    {
        this( config, graphDatabaseAPI, () ->
        {
            TransactionIdStore transactionIdStore = transactionIdStoreSupplier.get();
            StoreCopyServer copier = new StoreCopyServer( neoStoreDataSource, checkPointerSupplier.get(),
                    fileSystemAbstraction, graphDatabaseAPI.getStoreDir(),
                    monitors.newMonitor( StoreCopyServer.Monitor.class ), storeCopyCheckPointMutex );
            LogicalTransactionStore logicalTransactionStore = logicalTransactionStoreSupplier.get();
            LogFileInformation logFileInformation = logFileInformationSupplier.get();
            return new BackupImpl( copier, logicalTransactionStore, transactionIdStore, logFileInformation,
                    graphDatabaseAPI::storeId, logProvider );
        }, monitors, logProvider );
    }

    public OnlineBackupKernelExtension( Config config, GraphDatabaseAPI graphDatabaseAPI, BackupProvider provider,
                                        Monitors monitors, LogProvider logProvider )
    {
        this.config = config;
        this.graphDatabaseAPI = graphDatabaseAPI;
        this.backupProvider = provider;
        this.monitors = monitors;
        this.logProvider = logProvider;
    }

    @Override
    public void init()
    {
    }

    @Override
    public void start()
    {
        if ( config.<Boolean>get( OnlineBackupSettings.online_backup_enabled ) )
        {
            try
            {
                server = new BackupServer( backupProvider.newBackup(), config.get( online_backup_server ),
                        logProvider, monitors.newMonitor( ByteCounterMonitor.class, BackupServer.class ),
                        monitors.newMonitor( RequestMonitor.class, BackupServer.class ) );
                server.init();
                server.start();

                try
                {
                    startBindingListener = new StartBindingListener();
                    graphDatabaseAPI.getDependencyResolver().resolveDependency( ClusterMemberEvents.class).addClusterMemberListener(
                            (ClusterMemberListener) startBindingListener );

                    bindingListener = (BindingListener) myUri -> me = myUri;
                    graphDatabaseAPI.getDependencyResolver().resolveDependency( BindingNotifier.class ).addBindingListener(
                            (BindingListener) bindingListener );
                }
                catch ( NoClassDefFoundError | UnsatisfiedDependencyException e )
                {
                    // Not running HA
                }
            }
            catch ( Throwable t )
            {
                throw new RuntimeException( t );
            }
        }
    }

    @Override
    public void stop() throws Throwable
    {
        if ( server != null )
        {
            server.stop();
            server.shutdown();
            server = null;

            try
            {
                graphDatabaseAPI.getDependencyResolver().resolveDependency( ClusterMemberEvents.class).removeClusterMemberListener(
                        (ClusterMemberListener) startBindingListener );
                graphDatabaseAPI.getDependencyResolver().resolveDependency( BindingNotifier.class ).removeBindingListener(
                        (BindingListener) bindingListener );

                ClusterMemberAvailability client = getClusterMemberAvailability();
                client.memberIsUnavailable( BACKUP );
            }
            catch ( NoClassDefFoundError | UnsatisfiedDependencyException e )
            {
                // Not running HA
            }
        }
    }

    @Override
    public void shutdown()
    {
    }

    private class StartBindingListener extends ClusterMemberListener.Adapter
    {

        @Override
        public void memberIsAvailable( String role, InstanceId available, URI availableAtUri, StoreId storeId )
        {
            if ( graphDatabaseAPI.getDependencyResolver().resolveDependency( ClusterClient.class ).
                    getServerId().equals( available ) && "master".equals( role ) )
            {
                // It was me and i am master - yey!
                {
                    try
                    {
                        URI backupUri = createBackupURI();
                        ClusterMemberAvailability ha = getClusterMemberAvailability();
                        ha.memberIsAvailable( BACKUP, backupUri, storeId );
                    }
                    catch ( Throwable t )
                    {
                        throw new RuntimeException( t );
                    }
                }
            }
        }

        @Override
        public void memberIsUnavailable( String role, InstanceId unavailableId )
        {
            if ( graphDatabaseAPI.getDependencyResolver().resolveDependency( ClusterClient.class ).
                    getServerId().equals( unavailableId ) && "master".equals( role ) )
            {
                // It was me and i am master - yey!
                {
                    try
                    {
                        ClusterMemberAvailability ha = getClusterMemberAvailability();
                        ha.memberIsUnavailable( BACKUP );
                    }
                    catch ( Throwable t )
                    {
                        throw new RuntimeException( t );
                    }
                }
            }
        }
    }

    private ClusterMemberAvailability getClusterMemberAvailability()
    {
        return graphDatabaseAPI.getDependencyResolver().resolveDependency( ClusterMemberAvailability.class );
    }

    private URI createBackupURI()
    {
        String hostString = ServerUtil.getHostString( server.getSocketAddress() );
        String host = hostString.contains( INADDR_ANY ) ? me.getHost() : hostString;
        int port = server.getSocketAddress().getPort();
        return URI.create("backup://" + host + ":" + port);
    }
}
