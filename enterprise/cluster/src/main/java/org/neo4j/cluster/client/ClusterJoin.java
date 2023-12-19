/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cluster.client;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.ProtocolServer;
import org.neo4j.cluster.protocol.cluster.Cluster;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;

/**
 * This service starts quite late, and is available for the instance to join as a member in the cluster.
 * <p>
 * On start it will try to join the cluster specified by the initial hosts. After start finishes it will have
 * either joined an existing cluster or created a new one. On stop it will leave the cluster, but will fail
 * and continue the stop after one minute.
 */
public class ClusterJoin
        extends LifecycleAdapter
{
    public interface Configuration
    {
        List<HostnamePort> getInitialHosts();

        String getClusterName();

        boolean isAllowedToCreateCluster();

        long getClusterJoinTimeout();
    }

    private final Configuration config;
    private final ProtocolServer protocolServer;
    private final Log userLog;
    private final Log messagesLog;
    private Cluster cluster;

    public ClusterJoin( Configuration config, ProtocolServer protocolServer, LogService logService )
    {
        this.config = config;
        this.protocolServer = protocolServer;
        this.userLog = logService.getUserLog( getClass() );
        this.messagesLog = logService.getInternalLog( getClass() );
    }

    @Override
    public void start() throws Throwable
    {
        cluster = protocolServer.newClient( Cluster.class );

        joinByConfig();
    }

    @Override
    public void stop()
    {
        final Semaphore semaphore = new Semaphore( 0 );

        cluster.addClusterListener( new ClusterListener.Adapter()
        {
            @Override
            public void leftCluster()
            {
                cluster.removeClusterListener( this );
                semaphore.release();
            }
        } );

        cluster.leave();

        try
        {
            if ( !semaphore.tryAcquire( 60, TimeUnit.SECONDS ) )
            {
                messagesLog.info( "Unable to leave cluster, timeout" );
            }
        }
        catch ( InterruptedException e )
        {
            Thread.interrupted();
            messagesLog.warn( "Unable to leave cluster, interrupted", e );
        }
    }

    private void joinByConfig() throws TimeoutException
    {
        List<HostnamePort> hosts = config.getInitialHosts();

        cluster.addClusterListener( new UnknownJoiningMemberWarning( hosts ) );

        if ( hosts == null || hosts.size() == 0 )
        {
            userLog.info( "No cluster hosts specified. Creating cluster %s", config.getClusterName() );
            cluster.create( config.getClusterName() );
        }
        else
        {
            URI[] memberURIs = hosts.stream()
                    .map( member -> URI.create( "cluster://" + resolvePortOnlyHost( member ) ) )
                    .toArray( URI[]::new );

            while ( true )
            {
                userLog.info( "Attempting to join cluster of %s", hosts.toString() );
                Future<ClusterConfiguration> clusterConfig =
                        cluster.join( this.config.getClusterName(), memberURIs );

                try
                {
                    ClusterConfiguration clusterConf =
                            config.getClusterJoinTimeout() > 0
                            ? clusterConfig.get( config.getClusterJoinTimeout(), TimeUnit.MILLISECONDS )
                            : clusterConfig.get();
                    userLog.info( "Joined cluster: %s", clusterConf );
                    return;
                }
                catch ( InterruptedException e )
                {
                    userLog.warn( "Could not join cluster, interrupted. Retrying..." );
                }
                catch ( ExecutionException e )
                {
                    messagesLog.debug( "Could not join cluster " + this.config.getClusterName() );
                    if ( e.getCause() instanceof IllegalStateException )
                    {
                        throw (IllegalStateException) e.getCause();
                    }

                    if ( config.isAllowedToCreateCluster() )
                    {
                        // Failed to join cluster, create new one
                        userLog.info( "Could not join cluster of %s", hosts.toString() );
                        userLog.info( "Creating new cluster with name [%s]...", config.getClusterName() );
                        cluster.create( config.getClusterName() );
                        break;
                    }

                    userLog.warn( "Could not join cluster, timed out. Retrying..." );
                }
            }
        }
    }

    private String resolvePortOnlyHost( HostnamePort host )
    {
        try
        {
            return host.toString( InetAddress.getLocalHost().getHostAddress() );
        }
        catch ( UnknownHostException e )
        {
            throw new RuntimeException( e );
        }
    }

    private class UnknownJoiningMemberWarning extends ClusterListener.Adapter
    {
        private final List<HostnamePort> initialHosts;

        private UnknownJoiningMemberWarning( List<HostnamePort> initialHosts )
        {
            this.initialHosts = initialHosts;
        }

        @Override
        public void joinedCluster( InstanceId member, URI uri )
        {
            for ( HostnamePort host : initialHosts )
            {
                if ( host.matches( uri ) )
                {
                    return;
                }
            }
            messagesLog.info( "Member " + member + "(" + uri + ") joined cluster but was not part of initial hosts (" +
                              initialHosts + ")" );
        }

        @Override
        public void leftCluster()
        {
            cluster.removeClusterListener( this );
        }
    }
}
