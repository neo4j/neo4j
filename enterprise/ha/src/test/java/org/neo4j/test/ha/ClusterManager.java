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

import static java.util.Arrays.asList;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.IteratorUtil.count;

import java.io.File;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.cluster.client.Clusters;
import org.neo4j.cluster.client.ClustersXMLSerializer;
import org.neo4j.cluster.com.NetworkInstance;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.helpers.Predicate;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.Slaves;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;

public class ClusterManager
        extends LifecycleAdapter
{
    private static final Logger logger = LoggerFactory.getLogger( "clustermanager" );
    
    /**
     * Provides a specification of which clusters to start in {@link ClusterManager#start()}.
     */
    public interface Provider
    {
        Clusters clusters() throws Throwable;
    }

    /**
     * Provider pointing out an XML file to read.
     *
     * @param clustersXml the XML file containing the cluster specifications.
     */
    public static Provider fromXml( final URI clustersXml )
    {
        return new Provider()
        {
            @Override
            public Clusters clusters() throws Exception
            {
                DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
                Document clustersXmlDoc = documentBuilder.parse( clustersXml.toURL().openStream() );
                return new ClustersXMLSerializer( documentBuilder ).read( clustersXmlDoc );
            }
        };
    }

    /**
     * Provides a cluster specification with default values
     *
     * @param memberCount the total number of members in the cluster to start.
     */
    public static Provider clusterOfSize( int memberCount )
    {
        Clusters.Cluster cluster = new Clusters.Cluster( "neo4j.ha" );
        for ( int i = 0; i < memberCount; i++ )
        {
            cluster.getMembers().add( new Clusters.Member( 5001 + i ) );
        }

        final Clusters clusters = new Clusters();
        clusters.getClusters().add( cluster );

        return new Provider()
        {
            @Override
            public Clusters clusters() throws Throwable
            {
                return clusters;
            }
        };
    }

    LifeSupport life;
    private final File root;
    private final Map<String, String> commonConfig;
    private final Map<String, ManagedCluster> clusterMap = new HashMap<String, ManagedCluster>();
    private final Provider clustersProvider;

    public ClusterManager( Provider clustersProvider, File root, Map<String, String> commonConfig )
    {
        this.clustersProvider = clustersProvider;
        this.root = root;
        this.commonConfig = commonConfig;
    }

    @Override
    public void start() throws Throwable
    {
        Clusters clusters = clustersProvider.clusters();

        life = new LifeSupport();

        for ( int i = 0; i < clusters.getClusters().size(); i++ )
        {
            Clusters.Cluster cluster = clusters.getClusters().get( i );
            ManagedCluster managedCluster = new ManagedCluster( cluster );
            clusterMap.put( cluster.getName(), managedCluster );
            life.add( managedCluster );
        }

        life.start();
    }

    @Override
    public void stop() throws Throwable
    {
        life.stop();
    }

    /**
     * Represent one cluster. It can retrieve the current master, random slave
     * or all members. It can also temporarily fail an instance or shut it down.
     */
    public class ManagedCluster extends LifecycleAdapter
    {
        private final Clusters.Cluster spec;
        private final String name;
        private final Map<Integer, HighlyAvailableGraphDatabase> members = new HashMap<Integer,
                HighlyAvailableGraphDatabase>();

        ManagedCluster( Clusters.Cluster spec ) throws URISyntaxException
        {
            this.spec = spec;
            this.name = spec.getName();
            for ( int i = 0; i < spec.getMembers().size(); i++ )
            {
                startMember( i + 1 );
            }
        }

        @Override
        public void stop() throws Throwable
        {
            for ( HighlyAvailableGraphDatabase member : members.values() )
            {
                member.shutdown();
            }
        }

        /**
         * @return all started members in this cluster.
         */
        public Iterable<HighlyAvailableGraphDatabase> getAllMembers()
        {
            return members.values();
        }

        /**
         * @return the current master in the cluster.
         * @throws IllegalStateException if there's no current master.
         */
        public HighlyAvailableGraphDatabase getMaster()
        {
            for ( HighlyAvailableGraphDatabase graphDatabaseService : getAllMembers() )
            {
                if ( graphDatabaseService.isMaster() )
                {
                    return graphDatabaseService;
                }
            }
            throw new IllegalStateException( "No master found in cluster " + name );
        }

        /**
         * @param except do not return any of the dbs found in this array
         * @return a slave in this cluster.
         * @throws IllegalStateException if no slave was found in this cluster.
         */
        public HighlyAvailableGraphDatabase getAnySlave( HighlyAvailableGraphDatabase... except )
        {
            Set<HighlyAvailableGraphDatabase> exceptSet = new HashSet<HighlyAvailableGraphDatabase>( asList( except ) );
            for ( HighlyAvailableGraphDatabase graphDatabaseService : getAllMembers() )
            {
                if ( graphDatabaseService.getInstanceState().equals( "SLAVE" ) && !exceptSet.contains(
                        graphDatabaseService ) )
                {
                    return graphDatabaseService;
                }
            }

            throw new IllegalStateException( "No slave found in cluster " + name );
        }

        /**
         * @param serverId the server id to return the db for.
         * @return the {@link HighlyAvailableGraphDatabase} with the given server id.
         * @throws IllegalStateException if that db isn't started or no such
         *                               db exists in the cluster.
         */
        public HighlyAvailableGraphDatabase getMemberByServerId( int serverId )
        {
            HighlyAvailableGraphDatabase db = members.get( serverId );
            if ( db == null )
            {
                throw new IllegalStateException( "Db " + serverId + " not found at the moment in " + name );
            }
            return db;
        }

        /**
         * Shuts down a member of this cluster. A {@link RepairKit} is returned
         * which is able to restore the instance (i.e. start it again).
         *
         * @param db the {@link HighlyAvailableGraphDatabase} to shut down.
         * @return a {@link RepairKit} which can start it again.
         * @throws IllegalArgumentException if the given db isn't a member of this cluster.
         */
        public RepairKit shutdown( HighlyAvailableGraphDatabase db )
        {
            assertMember( db );
            int serverId = db.getDependencyResolver().resolveDependency( Config.class ).get( HaSettings.server_id );
            members.remove( serverId );
            life.remove( db );
            db.shutdown();
            return new StartDatabaseAgainKit( this, serverId );
        }

        private void assertMember( HighlyAvailableGraphDatabase db )
        {
            if ( !members.values().contains( db ) )
            {
                throw new IllegalArgumentException( "Db " + db + " not a member of this cluster " + name );
            }
        }

        /**
         * WARNING: beware of hacks.
         * <p/>
         * Fails a member of this cluster by making it not respond to heart beats.
         * A {@link RepairKit} is returned which is able to repair the instance
         * (i.e start the network) again.
         *
         * @param db the {@link HighlyAvailableGraphDatabase} to fail.
         * @return a {@link RepairKit} which can repair the failure.
         * @throws IllegalArgumentException if the given db isn't a member of this cluster.
         */
        public RepairKit fail( HighlyAvailableGraphDatabase db ) throws Throwable
        {
            assertMember( db );
            ClusterClient clusterClient = db.getDependencyResolver().resolveDependency( ClusterClient.class );
            LifeSupport clusterClientLife = (LifeSupport) accessible( clusterClient.getClass().getDeclaredField(
                    "life" ) ).get( clusterClient );
            NetworkInstance network = instance( NetworkInstance.class, clusterClientLife.getLifecycleInstances() );
            network.stop();
            return new StartNetworkAgainKit( network );
        }

        private void startMember( int serverId ) throws URISyntaxException
        {
            Clusters.Member member = spec.getMembers().get( serverId - 1 );
            int haPort = new URI( "cluster://" + member.getHost() ).getPort() + 3000;

            StringBuilder initialHosts = new StringBuilder( spec.getMembers().get( 0 ).getHost() );
            for (int i = 1; i < spec.getMembers().size(); i++)
            {
                initialHosts.append( "," ).append( spec.getMembers().get( i ).getHost() );
            }
            GraphDatabaseBuilder graphDatabaseBuilder = new HighlyAvailableGraphDatabaseFactory()
                    .newHighlyAvailableDatabaseBuilder( new File( new File( root, name ),
                            "server" + serverId ).getAbsolutePath() ).
                            setConfig( ClusterSettings.cluster_name, name ).
                            setConfig( ClusterSettings.initial_hosts, initialHosts.toString() ).
                            setConfig( ClusterSettings.cluster_server, member.getHost() ).
                            setConfig( HaSettings.server_id, serverId + "" ).
                            setConfig( HaSettings.ha_server, ":" + haPort ).
                            setConfig( commonConfig );

            config( graphDatabaseBuilder, name, serverId );

            // logger.info( "Starting cluster node " + serverId + " in cluster "
            // + name );
            final GraphDatabaseService graphDatabase = graphDatabaseBuilder.
                    newGraphDatabase();

            members.put( serverId, (HighlyAvailableGraphDatabase) graphDatabase );

            life.add( new LifecycleAdapter()
            {
                @Override
                public void stop() throws Throwable
                {
                    graphDatabase.shutdown();
                }
            } );

            // logger.info( "Started cluster node " + serverId + " in cluster "
            // + name );
        }

        /**
         * Will await a condition for the default max time.
         *
         * @param predicate {@link Predicate} that should return true
         *                  signalling that the condition has been met.
         * @throws IllegalStateException if the condition wasn't met
         *                               during within the max time.
         */
        public void await( Predicate<ManagedCluster> predicate )
        {
            await( predicate, 60 );
        }

        /**
         * Will await a condition for the given max time.
         *
         * @param predicate {@link Predicate} that should return true
         *                  signalling that the condition has been met.
         * @throws IllegalStateException if the condition wasn't met
         *                               during within the max time.
         */
        public void await( Predicate<ManagedCluster> predicate, int maxSeconds )
        {
            long end = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis( maxSeconds );
            while ( System.currentTimeMillis() < end )
            {
                if ( predicate.accept( this ) )
                {
                    return;
                }

                try
                {
                    Thread.sleep( 100 );
                }
                catch ( InterruptedException e )
                {
                    // Ignore
                }
            }
            throw new IllegalStateException( "Awaited condition never met, waited " + maxSeconds );
        }

        /**
         * The total number of members of the cluster.
         */
        public int size()
        {
            return spec.getMembers().size();
        }
        
        public int getServerId( HighlyAvailableGraphDatabase db )
        {
            assertMember( db );
            return db.getConfig().get( HaSettings.server_id );
        }
    }

    /**
     * The current master sees this many slaves as available.
     *
     * @param count number of slaves to see as available.
     */
    public static Predicate<ManagedCluster> masterSeesSlavesAsAvailable( final int count )
    {
        return new Predicate<ClusterManager.ManagedCluster>()
        {
            @Override
            public boolean accept( ManagedCluster cluster )
            {
                return count( cluster.getMaster().getDependencyResolver().resolveDependency( Slaves.class ).getSlaves
                        () ) >= count;
            }
        };
    }

    /**
     * The current master sees all slaves in the cluster as available.
     * Based on the total number of members in the cluster.
     */
    public static Predicate<ManagedCluster> masterSeesAllSlavesAsAvailable()
    {
        return new Predicate<ClusterManager.ManagedCluster>()
        {
            @Override
            public boolean accept( ManagedCluster cluster )
            {
                return count( cluster.getMaster().getDependencyResolver().resolveDependency( Slaves.class ).getSlaves
                        () ) >= cluster.size() - 1;
            }
        };
    }

    /**
     * There must be a master available.
     */
    public static Predicate<ManagedCluster> masterAvailable()
    {
        return new Predicate<ClusterManager.ManagedCluster>()
        {
            @Override
            public boolean accept( ManagedCluster cluster )
            {
                for ( HighlyAvailableGraphDatabase graphDatabaseService : cluster.getAllMembers() )
                {
                    if ( graphDatabaseService.isMaster() )
                    {
                        return true;
                    }
                }
                return false;
            }
        };
    }

    private <T> T instance( Class<T> classToFind, Iterable<?> from )
    {
        for ( Object item : from )
        {
            if ( classToFind.isAssignableFrom( item.getClass() ) )
            {
                return (T) item;
            }
        }
        fail( "Couldn't find the network instance to fail. Internal field, so fragile sensitive to changes though" );
        return null; // it will never get here.
    }

    private Field accessible( Field field )
    {
        field.setAccessible( true );
        return field;
    }

    public ManagedCluster getCluster( String name )
    {
        if ( !clusterMap.containsKey( name ) )
        {
            throw new IllegalArgumentException( name );
        }
        return clusterMap.get( name );
    }

    public ManagedCluster getDefaultCluster()
    {
        return getCluster( "neo4j.ha" );
    }

    protected void config( GraphDatabaseBuilder builder, String clusterName, int serverId )
    {
    }

    public interface RepairKit
    {
        void repair() throws Throwable;
    }

    private class StartNetworkAgainKit implements RepairKit
    {
        private NetworkInstance network;

        StartNetworkAgainKit( NetworkInstance network )
        {
            this.network = network;
        }

        @Override
        public void repair() throws Throwable
        {
            network.start();
        }
    }

    private class StartDatabaseAgainKit implements RepairKit
    {
        private int serverId;
        private ManagedCluster cluster;

        public StartDatabaseAgainKit( ManagedCluster cluster, int serverId )
        {
            this.cluster = cluster;
            this.serverId = serverId;
        }

        @Override
        public void repair() throws Throwable
        {
            cluster.startMember( serverId );
        }
    }
}
