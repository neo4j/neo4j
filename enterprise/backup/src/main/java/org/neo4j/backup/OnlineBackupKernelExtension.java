/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import java.net.URI;

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
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.Provider;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;
import org.neo4j.kernel.impl.transaction.log.LogRotationControl;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.kernel.monitoring.Monitors;

import static org.neo4j.backup.OnlineBackupSettings.online_backup_server;

public class OnlineBackupKernelExtension implements Lifecycle
{
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
    private final Logging logging;
    private final Monitors monitors;
    private BackupServer server;
    private final BackupProvider backupProvider;
    private volatile URI me;

    public OnlineBackupKernelExtension( Config config, final GraphDatabaseAPI graphDatabaseAPI,
                                        final KernelPanicEventGenerator kpeg, final Logging logging,
                                        final Monitors monitors )
    {
        this( config, graphDatabaseAPI, new BackupProvider()
        {
            @Override
            public TheBackupInterface newBackup()
            {
                DependencyResolver resolver = graphDatabaseAPI.getDependencyResolver();
                TransactionIdStore transactionIdStore = resolver.resolveDependency( TransactionIdStore.class );
                StoreCopyServer copier = new StoreCopyServer(
                        transactionIdStore,
                        resolver.resolveDependency( DataSourceManager.class ).getDataSource(),
                        resolver.resolveDependency( LogRotationControl.class ),
                        resolver.resolveDependency( FileSystemAbstraction.class ),
                        new File( graphDatabaseAPI.getStoreDir() ),
                        monitors.newMonitor( StoreCopyServer.Monitor.class ) );
                LogicalTransactionStore logicalTransactionStore = resolver.resolveDependency( LogicalTransactionStore.class );
                LogFileInformation logFileInformation = resolver.resolveDependency( LogFileInformation.class );
                return new BackupImpl( copier, monitors,
                        logicalTransactionStore, transactionIdStore, logFileInformation, new Provider<StoreId>()
                        {
                            @Override
                            public StoreId instance()
                            {
                                return graphDatabaseAPI.storeId();
                            }
                        } );
            }
        }, monitors, logging );
    }

    public OnlineBackupKernelExtension( Config config, GraphDatabaseAPI graphDatabaseAPI, BackupProvider provider,
                                        Monitors monitors, Logging logging )
    {
        this.config = config;
        this.graphDatabaseAPI = graphDatabaseAPI;
        this.backupProvider = provider;
        this.monitors = monitors;
        this.logging = logging;
    }

    @Override
    public void init() throws Throwable
    {
    }

    @Override
    public void start() throws Throwable
    {
        if ( config.<Boolean>get( OnlineBackupSettings.online_backup_enabled ) )
        {
            try
            {
                server = new BackupServer( backupProvider.newBackup(), config.get( online_backup_server ),
                        logging, monitors.newMonitor( ByteCounterMonitor.class, BackupServer.class ), monitors.newMonitor( RequestMonitor.class, BackupServer.class ) );
                server.init();
                server.start();

                try
                {
                    graphDatabaseAPI.getDependencyResolver().resolveDependency( ClusterMemberEvents.class).addClusterMemberListener(
                            new StartBindingListener() );

                    graphDatabaseAPI.getDependencyResolver().resolveDependency( BindingNotifier.class ).addBindingListener( new BindingListener()
                            {
                                @Override
                                public void listeningAt( URI myUri )
                                {
                                    me = myUri;
                                }
                            } );
                }
                catch ( NoClassDefFoundError | IllegalArgumentException e )
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
                ClusterMemberAvailability client = getClusterMemberAvailability();
                client.memberIsUnavailable( BACKUP );
            }
            catch ( NoClassDefFoundError | IllegalArgumentException e )
            {
                // Not running HA
            }
        }
    }

    @Override
    public void shutdown() throws Throwable
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

    private ClusterMemberAvailability getClusterMemberAvailability() {
        return graphDatabaseAPI.getDependencyResolver().resolveDependency( ClusterMemberAvailability.class );
    }

    private URI createBackupURI() {
        String hostString = ServerUtil.getHostString( server.getSocketAddress() );
        String host = hostString.contains( INADDR_ANY ) ? me.getHost() : hostString;
        int port = server.getSocketAddress().getPort();
        return URI.create("backup://" + host + ":" + port);
    }
}
