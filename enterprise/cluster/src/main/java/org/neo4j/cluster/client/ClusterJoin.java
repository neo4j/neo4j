/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cluster.client;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;

import org.neo4j.cluster.BindingListener;
import org.neo4j.cluster.ProtocolServer;
import org.neo4j.cluster.protocol.cluster.Cluster;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.logging.Logging;

/**
 * This service starts quite late, and is available for the instance to join as a member in the cluster.
 * <p/>
 * It can either use manual listing of hosts, or auto discovery protocols.
 */
public class ClusterJoin
        extends LifecycleAdapter
{
    public interface Configuration
    {
        List<HostnamePort> getInitialHosts();

        String getClusterName();

        boolean isAllowedToCreateCluster();
    }

    private final Configuration config;
    private final ProtocolServer protocolServer;
    private final StringLogger logger;
    private URI clustersUri;
    private Clusters clusters;
    private Cluster cluster;
    private URI serverUri;
    private DocumentBuilder builder;
    private Transformer transformer;

    public ClusterJoin( Configuration config, ProtocolServer protocolServer, Logging logger )
    {
        this.config = config;
        this.protocolServer = protocolServer;
        this.logger = logger.getLogger( getClass() );
    }

    @Override
    public void init() throws Throwable
    {
        builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        transformer = TransformerFactory.newInstance().newTransformer();
        cluster = protocolServer.newClient( Cluster.class );
    }

    @Override
    public void start() throws Throwable
    {
        cluster = protocolServer.newClient( Cluster.class );
        acquireServerUri();

        // Now that we have our own id, do cluster join
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
            if ( !semaphore.tryAcquire( 5, TimeUnit.SECONDS ) )
            {
                logger.info( "Unable to leave cluster, timeout" );
            }
        }
        catch ( InterruptedException e )
        {
            Thread.interrupted();
            logger.warn( "Unable to leave cluster, interrupted", e );
        }
    }

    private void acquireServerUri() throws RuntimeException
    {
        final Semaphore semaphore = new Semaphore( 0 );

        protocolServer.addBindingListener( new BindingListener()
        {
            @Override
            public void listeningAt( URI me )
            {
                serverUri = me;
                semaphore.release();
                protocolServer.removeBindingListener( this );
            }
        } );
        try
        {
            if ( !semaphore.tryAcquire( 1, TimeUnit.MINUTES ) )
            {
                throw new RuntimeException( "Unable to acquire server URI, timed out" );
            }
        }
        catch ( InterruptedException e )
        {
            Thread.interrupted();
            throw new RuntimeException( "Unable to acquire server URI, interrupted", e );
        }
    }

    private void joinByConfig()
    {
        List<HostnamePort> hosts = config.getInitialHosts();

        cluster.addClusterListener( new UnknownJoiningMemberWarning( hosts ) );

        if ( hosts == null || hosts.size() == 0 )
        {
            logger.info( "No cluster hosts specified. Creating cluster " + config.getClusterName() );
            cluster.create( config.getClusterName() );
        }
        else
        {
            URI[] memberURIs = Iterables.toArray(URI.class,
                    Iterables.filter( new Predicate<URI>()
                    {
                        @Override
                        public boolean accept( URI uri )
                        {
                            return !serverUri.equals( uri );
                        }
                    },
                    Iterables.map( new Function<HostnamePort, URI>()
                    {
                        @Override
                        public URI apply( HostnamePort member)
                        {
                            return URI.create( "cluster://" + resolvePortOnlyHost( member ) );
                        }
                    }, hosts)));

            while( true )
            {
                logger.debug( "Attempting to join " + hosts.toString() );
                Future<ClusterConfiguration> clusterConfig =
                        cluster.join( this.config.getClusterName(), memberURIs );
                try
                {
                    logger.debug( "Joined cluster:" + clusterConfig.get() );
                    return;
                }
                catch ( InterruptedException e )
                {
                    e.printStackTrace();
                }
                catch ( ExecutionException e )
                {
                    logger.debug( "Could not join cluster " + this.config.getClusterName() );
                }

                if ( config.isAllowedToCreateCluster() )
                {
                    // Failed to join cluster, create new one
                    cluster.create( config.getClusterName() );
                    break;
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
        public void joinedCluster( URI member )
        {
            for ( HostnamePort host : initialHosts )
            {
                if ( host.matches( member ) )
                {
                    return;
                }
            }
            logger.warn( "Member " + member + " joined cluster but was not part of initial hosts (" +
                    initialHosts + ")" );
        }

        @Override
        public void leftCluster()
        {
            cluster.removeClusterListener( this );
        }
    }
}
