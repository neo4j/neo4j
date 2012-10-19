/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

package org.neo4j.kernel.ha.cluster.discovery;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.neo4j.cluster.BindingListener;
import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.ProtocolServer;
import org.neo4j.cluster.protocol.cluster.Cluster;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

/**
 * This service starts quite late, and is available for the instance to join as a member in the cluster.
 * <p/>
 * It can either use manual listing of hosts, or auto discovery protocols.
 */
public class ClusterJoin
        extends LifecycleAdapter
{
    private Config config;
    private ProtocolServer protocolServer;
    private StringLogger logger;
    private ClusterListener clusterListener;
    private URI clustersUri;
    private Clusters clusters;
    private Cluster cluster;
    private URI serverId;
    private DocumentBuilder builder;
    private Transformer transformer;

    public ClusterJoin( Config config, ProtocolServer protocolServer, StringLogger logger,
                        ClusterListener clusterListener )
    {
        this.config = config;
        this.protocolServer = protocolServer;
        this.logger = logger;
        this.clusterListener = clusterListener;
    }

    @Override
    public void init() throws Throwable
    {
        cluster = protocolServer.newClient( Cluster.class );
        cluster.addClusterListener( clusterListener );
        builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        transformer = TransformerFactory.newInstance().newTransformer();
    }

    @Override
    public void start() throws Throwable
    {
        acquireServerId();

        // Now that we have our own id, do cluster join
        if ( config.get( HaSettings.cluster_discovery_enabled ) )
        {
            clusterDiscovery();
        }
        else
        {
            clusterByConfig();
        }
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
                logger.logMessage( "Unable to leave cluster, timeout" );
            }
        }
        catch ( InterruptedException e )
        {
            Thread.interrupted();
            logger.logMessage( "Unable to leave cluster, interrupted", e );
        }
    }

    private void acquireServerId() throws RuntimeException
    {
        final Semaphore semaphore = new Semaphore( 0 );

        protocolServer.addBindingListener( new BindingListener()
        {
            @Override
            public void listeningAt( URI me )
            {
                serverId = me;
                semaphore.release();
                protocolServer.removeBindingListener( this );
            }
        } );
        try
        {
            if ( !semaphore.tryAcquire( 1, TimeUnit.MINUTES ) )
            {
                throw new RuntimeException( "Unable to acquire server id, timed out" );
            }
        }
        catch ( InterruptedException e )
        {
            Thread.interrupted();
            throw new RuntimeException( "Unable to acquire server id, interrupted", e );
        }
    }

    private void clusterDiscovery() throws URISyntaxException, ParserConfigurationException, SAXException, IOException
    {
        determineUri();

        readClustersXml();

        // Now try to join or create cluster
        if ( clusters != null )
        {
            Clusters.Cluster clusterConfig = clusters.getCluster( config.get( ClusterSettings.cluster_name ) );
            if ( clusterConfig != null )
            {
                for ( Clusters.Member member : clusterConfig.getMembers() )
                {
                    URI joinUri = new URI( "cluster://" + member.getHost() );
                    if ( !joinUri.equals( serverId ) )
                    {
                        Future<ClusterConfiguration> config = cluster.join( joinUri );
                        try
                        {
                            logger.logMessage( "Joined cluster:" + config.get() );

                            try
                            {
                                updateMyInfo();
                            }
                            catch ( TransformerException e )
                            {
                                throw new RuntimeException( e );
                            }

                            return;
                        }
                        catch ( InterruptedException e )
                        {
                            e.printStackTrace();
                        }
                        catch ( ExecutionException e )
                        {
                            logger.logMessage( "Could not join cluster member " + member.getHost() );
                        }
                    }
                }
            }

            // Could not find cluster or not join nodes in cluster - create it!
            if ( clusterConfig == null )
            {
                clusterConfig = new Clusters.Cluster( config.get( ClusterSettings.cluster_name ) );
                clusters.getClusters().add( clusterConfig );
            }

            cluster.create( clusterConfig.getName() );

            if ( clusterConfig.getByUri( serverId ) == null )
            {
                clusterConfig.getMembers().add( new Clusters.Member( serverId.toString() ) );

                try
                {
                    updateMyInfo();
                }
                catch ( TransformerException e )
                {
                    logger.logMessage( "Could not update cluster discovery file:" + clustersUri, e );
                }
            }
        }
    }

    private void determineUri() throws URISyntaxException
    {
        if ( config.isSet( HaSettings.cluster_discovery_url ) )
        {
            String clusterUrlString = config.get( HaSettings.cluster_discovery_url );
            if ( clusterUrlString.startsWith( "/" ) )
            {
                clustersUri = Thread.currentThread().getContextClassLoader().getResource( clusterUrlString ).toURI();
            }
            else
            {
                clustersUri = new URI( clusterUrlString );
            }
        }
        else
        {
            URL classpathURL = Thread.currentThread().getContextClassLoader().getResource( "clusters.xml" );
            if ( classpathURL != null )
            {
                clustersUri = classpathURL.toURI();
            }
        }
    }

    private void readClustersXml() throws SAXException, IOException
    {
        if ( clustersUri.getScheme().equals( "file" ) )
        {
            File file = new File( clustersUri );
            if ( file.exists() )
            {
                Document doc = builder.parse( file );
                clusters = new ClustersXMLSerializer( builder ).read( doc );
                clusters.setTimestamp( file.lastModified() );
            }
        }
    }

    private void updateMyInfo() throws TransformerException, IOException, SAXException
    {
        Clusters.Cluster cluster = clusters.getCluster( config.get( ClusterSettings.cluster_name ) );

        if ( cluster == null )
        {
            clusters.getClusters().add( cluster = new Clusters.Cluster( config.get( ClusterSettings.cluster_name ) ) );
        }

        if ( cluster.contains( serverId ) )
        {
            // Do nothing
        }
        else
        {
            // Add myself to list
            cluster.getMembers().add( new Clusters.Member( serverId.getHost() + (serverId.getPort() == -1 ? "" : ":" +
                    serverId.getPort()) ) );

            Document document = new ClustersXMLSerializer( builder ).write( clusters );

            // Save new version
            if ( clustersUri.getScheme().equals( "file" ) )
            {
                File clustersFile = new File( clustersUri );
                if ( clustersFile.lastModified() != clusters.getTimestamp() )
                {
                    readClustersXml(); // Re-read XML file
                    updateMyInfo(); // Try again
                    return;
                }

                // Save new version
                transformer.transform( new DOMSource( document ), new StreamResult( clustersFile ) );
                clusters.setTimestamp( clustersFile.lastModified() );
            }
            else
            {
                // TODO Implement HTTP version
            }
        }
    }

    private void clusterByConfig()
    {
        String hosts = config.get( HaSettings.initial_hosts );

        if ( hosts.equals( "" ) )
        {
            logger.logMessage( "Creating cluster " + config.get( ClusterSettings.cluster_name ) );
            cluster.create( config.get( ClusterSettings.cluster_name ) );
        }
        else
        {
            try
            {
                for ( String host : hosts.split( "," ) )
                {
                    if ( serverId.toString().endsWith( host ) )
                    {
                        continue; // Don't try to join myself
                    }

                    logger.info( "Attempting to join " + host );
                    Future<ClusterConfiguration> clusterConfig = cluster.join( new URI( "cluster://" + host ) );
                    try
                    {
                        logger.info( "Joined cluster:" + clusterConfig.get() );
                        return;
                    }
                    catch ( InterruptedException e )
                    {
                        e.printStackTrace();
                    }
                    catch ( ExecutionException e )
                    {
                        logger.error( "Could not join cluster member " + host );
                    }
                }

                // Failed to join cluster, create new one
                cluster.create( config.get( ClusterSettings.cluster_name ) );
            }
            catch ( URISyntaxException e )
            {
                // This
                e.printStackTrace();
            }
        }
    }
}
