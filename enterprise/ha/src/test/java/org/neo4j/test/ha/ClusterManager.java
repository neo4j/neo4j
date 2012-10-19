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

package org.neo4j.test.ha;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.cluster.discovery.Clusters;
import org.neo4j.kernel.ha.cluster.discovery.ClustersXMLSerializer;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;

public class ClusterManager
        extends LifecycleAdapter
{
    LifeSupport life;
    URI clustersXml;
    private File root;
    private Map<String, String> commonConfig;
    private Map<String, List<HighlyAvailableGraphDatabase>> clusterMap = new HashMap<String,
            List<HighlyAvailableGraphDatabase>>();

    public ClusterManager( URI clustersXml, File root, Map<String, String> commonConfig )
    {
        this.clustersXml = clustersXml;
        this.root = root;
        this.commonConfig = commonConfig;
    }

    @Override
    public void start() throws Throwable
    {
        DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        Document clustersXmlDoc = documentBuilder.parse( clustersXml.toURL().openStream() );

        Clusters clusters = new ClustersXMLSerializer( documentBuilder ).read( clustersXmlDoc );

        life = new LifeSupport();

        Logger logger = LoggerFactory.getLogger( "clustermanager" );
        int serverCount = 0;
        for ( int i = 0; i < clusters.getClusters().size(); i++ )
        {
            Clusters.Cluster cluster = clusters.getClusters().get( i );

            List<HighlyAvailableGraphDatabase> clusterNodes = new ArrayList<HighlyAvailableGraphDatabase>();
            clusterMap.put( cluster.getName(), clusterNodes );

            for ( int j = 0; j < cluster.getMembers().size(); j++ )
            {
                Clusters.Member member = cluster.getMembers().get( j );

                int haPort = new URI( "cluster://" + member.getHost() ).getPort() + 3000;

                GraphDatabaseBuilder graphDatabaseBuilder = new HighlyAvailableGraphDatabaseFactory()
                        .newHighlyAvailableDatabaseBuilder( new File( root,
                                "server" + (++serverCount) ).getAbsolutePath() ).
                                setConfig( ClusterSettings.cluster_name, cluster.getName() ).
                                setConfig( HaSettings.initial_hosts, cluster.getMembers().get( 0 ).getHost() ).
                                setConfig( HaSettings.server_id, j + 1 + "" ).
                                setConfig( HaSettings.cluster_server, member.getHost() ).
                                setConfig( HaSettings.ha_server, "localhost:" + haPort ).
                                setConfig( commonConfig );

                config( graphDatabaseBuilder, serverCount );

                logger.info( "Starting cluster node " + j );
                final GraphDatabaseService graphDatabase = graphDatabaseBuilder.
                        newGraphDatabase();

                clusterNodes.add( (HighlyAvailableGraphDatabase) graphDatabase );

                life.add( new LifecycleAdapter()
                {
                    @Override
                    public void stop() throws Throwable
                    {
                        graphDatabase.shutdown();
                    }
                } );

                logger.info( "Started cluster node " + j );
//                Thread.sleep( 10 * 1000 );
            }

            logger.info( "Started cluster " + cluster.getName() );
        }

        life.start();
    }

    @Override
    public void stop() throws Throwable
    {
        life.stop();
    }

    public List<HighlyAvailableGraphDatabase> getCluster( String name )
    {
        return clusterMap.get( name );
    }

    public HighlyAvailableGraphDatabase getMaster( String name )
    {
        for ( HighlyAvailableGraphDatabase graphDatabaseService : getCluster( name ) )
        {
            if ( graphDatabaseService.isMaster() )
            {
                return graphDatabaseService;
            }
        }

        throw new IllegalStateException( "No master found in cluster " + name );
    }

    public HighlyAvailableGraphDatabase getAnySlave( String name )
    {
        for ( HighlyAvailableGraphDatabase graphDatabaseService : getCluster( name ) )
        {
            if ( graphDatabaseService.getInstanceState().equals( "SLAVE" ) )
            {
                return graphDatabaseService;
            }
        }

        throw new IllegalStateException( "No master found in cluster " + name );
    }

    protected void config( GraphDatabaseBuilder builder, int serverCount )
    {

    }
}
