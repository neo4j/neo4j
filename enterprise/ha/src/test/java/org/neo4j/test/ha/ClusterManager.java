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
package org.neo4j.test.ha;

import java.io.File;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.cluster.client.Clusters;
import org.neo4j.cluster.client.ClustersXMLSerializer;
import org.neo4j.cluster.com.NetworkInstance;
import org.neo4j.cluster.protocol.election.NotElectableElectionCredentialsProvider;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.Settings;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.jmx.Kernel;
import org.neo4j.jmx.impl.JmxKernelExtension;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.UpdatePuller;
import org.neo4j.kernel.ha.com.master.Slaves;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.logging.LogbackService;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.management.ClusterMemberInfo;
import org.neo4j.management.Neo4jManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.StaticLoggerBinder;
import org.w3c.dom.Document;

import ch.qos.logback.classic.LoggerContext;

import static java.util.Arrays.asList;

import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.IteratorUtil.count;

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
            cluster.getMembers().add( new Clusters.Member( 5001 + i, true ) );
        }

        final Clusters clusters = new Clusters();
        clusters.getClusters().add( cluster );
        return provided( clusters );
    }


    /**
     * Provides a cluster specification with default values
     * @param haMemberCount the total number of members in the cluster to start.
     */
    public static Provider clusterWithAdditionalClients( int haMemberCount, int additionalClientCount )
    {
        Clusters.Cluster cluster = new Clusters.Cluster( "neo4j.ha" );
        int counter = 0;
        for ( int i = 0; i < haMemberCount; i++, counter++ )
        {
            cluster.getMembers().add( new Clusters.Member( 5001 + counter, true ) );
        }
        for ( int i = 0; i < additionalClientCount; i++, counter++ )
        {
            cluster.getMembers().add( new Clusters.Member( 5001 + counter, false ) );
        }
        
        final Clusters clusters = new Clusters();
        clusters.getClusters().add( cluster );
        return provided( clusters );
    }
    
    /**
     * Provides a cluster specification with default values
     * @param haMemberCount the total number of members in the cluster to start.
     */
    public static Provider clusterWithAdditionalArbiters( int haMemberCount, int arbiterCount)
    {
        Clusters.Cluster cluster = new Clusters.Cluster( "neo4j.ha" );
        int counter = 0;
        for ( int i = 0; i < arbiterCount; i++, counter++ )
        {
            cluster.getMembers().add( new Clusters.Member( 5001 + counter, false ) );
        }
        for ( int i = 0; i < haMemberCount; i++, counter++ )
        {
            cluster.getMembers().add( new Clusters.Member( 5001 + counter, true ) );
        }

        final Clusters clusters = new Clusters();
        clusters.getClusters().add( cluster );
        return provided( clusters );
    }

    public static Provider provided( final Clusters clusters )
    {
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
    private final Map<Integer, Map<String, String>> instanceConfig;
    private final Map<String, ManagedCluster> clusterMap = new HashMap<String, ManagedCluster>();
    private final Provider clustersProvider;
    private final HighlyAvailableGraphDatabaseFactory dbFactory;

    public ClusterManager( Provider clustersProvider, File root, Map<String, String> commonConfig,
                           Map<Integer, Map<String, String>> instanceConfig,
                           HighlyAvailableGraphDatabaseFactory dbFactory )
    {
        this.clustersProvider = clustersProvider;
        this.root = root;
        this.commonConfig = commonConfig;
        this.instanceConfig = instanceConfig;
        this.dbFactory = dbFactory;
    }

    public ClusterManager( Provider clustersProvider, File root, Map<String, String> commonConfig,
            Map<Integer, Map<String, String>> instanceConfig )
    {
        this( clustersProvider, root, commonConfig, instanceConfig, new HighlyAvailableGraphDatabaseFactory() );
    }
    
    public ClusterManager( Provider clustersProvider, File root, Map<String, String> commonConfig )
    {
        this( clustersProvider, root, commonConfig, Collections.<Integer, Map<String, String>>emptyMap(),
                new HighlyAvailableGraphDatabaseFactory() );
    }

    @Override
    public void start() throws Throwable
    {
        Clusters clusters = clustersProvider.clusters();

        life = new LifeSupport();

        // Started so instances added here will be started immediately, and in case of exceptions they can be
        // shutdown() or stop()ped properly
        life.start();

        for ( int i = 0; i < clusters.getClusters().size(); i++ )
        {
            Clusters.Cluster cluster = clusters.getClusters().get( i );
            ManagedCluster managedCluster = new ManagedCluster( cluster );
            clusterMap.put( cluster.getName(), managedCluster );
            life.add( managedCluster );
        }
    }

    @Override
    public void stop() throws Throwable
    {
        life.stop();
    }

    @Override
    public void shutdown() throws Throwable
    {
        life.shutdown();
    }

    /**
     * Represent one cluster. It can retrieve the current master, random slave
     * or all members. It can also temporarily fail an instance or shut it down.
     */
    public class ManagedCluster extends LifecycleAdapter
    {
        private final Clusters.Cluster spec;
        private final String name;
        private final Map<Integer, HighlyAvailableGraphDatabaseProxy> members = new HashMap<Integer,
                HighlyAvailableGraphDatabaseProxy>();

        ManagedCluster( Clusters.Cluster spec ) throws URISyntaxException
        {
            this.spec = spec;
            this.name = spec.getName();
            for ( int i = 0; i < spec.getMembers().size(); i++ )
            {
                startMember( i + 1 );
            }
            for ( HighlyAvailableGraphDatabaseProxy member : members.values() )
            {
                insertInitialData( member.get(), name, member.get().getConfig().get( ClusterSettings.server_id ) );
            }
        }
        
        public String getInitialHostsConfigString()
        {
            StringBuilder result = new StringBuilder();
            for ( HighlyAvailableGraphDatabase member : getAllMembers() )
                result.append( result.length() > 0 ? "," : "" ).append( ":" +
                        member.getDependencyResolver().resolveDependency(
                                ClusterClient.class ).getClusterServer().getPort() );
            return result.toString();
        }
        
        @Override
        public void stop() throws Throwable
        {
            for ( HighlyAvailableGraphDatabaseProxy member : members.values() )
            {
                member.get().shutdown();
            }
        }

        /**
         * @return all started members in this cluster.
         */
        public Iterable<HighlyAvailableGraphDatabase> getAllMembers()
        {
            return Iterables.map( new Function<HighlyAvailableGraphDatabaseProxy, HighlyAvailableGraphDatabase>()
            {

                @Override
                public HighlyAvailableGraphDatabase apply( HighlyAvailableGraphDatabaseProxy from )
                {
                    return from.get();
                }
            }, members.values() );
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
            HighlyAvailableGraphDatabase db = members.get( serverId ).get();
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
            int serverId = db.getDependencyResolver().resolveDependency( Config.class ).get( ClusterSettings.server_id );
            members.remove( serverId );
            life.remove( db );
            db.shutdown();
            return new StartDatabaseAgainKit( this, serverId );
        }

        private void assertMember( HighlyAvailableGraphDatabase db )
        {
            for ( HighlyAvailableGraphDatabaseProxy highlyAvailableGraphDatabaseProxy : members.values() )
            {
                if ( highlyAvailableGraphDatabaseProxy.get().equals( db ) )
                {
                    return;
                }
            }
            throw new IllegalArgumentException( "Db " + db + " not a member of this cluster " + name );
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
            
            return new StartNetworkAgainKit( db, network );
        }

        private void startMember( int serverId ) throws URISyntaxException
        {
            Clusters.Member member = spec.getMembers().get( serverId-1 );
            StringBuilder initialHosts = new StringBuilder( spec.getMembers().get( 0 ).getHost() );
            for (int i = 1; i < spec.getMembers().size(); i++)
                initialHosts.append( "," ).append( spec.getMembers().get( i ).getHost() );
            if ( member.isFullHaMember() )
            {
                int haPort = new URI( "cluster://" + member.getHost() ).getPort() + 3000;
                GraphDatabaseBuilder graphDatabaseBuilder = dbFactory
                        .newHighlyAvailableDatabaseBuilder( new File( new File( root, name ),
                                "server" + serverId ).getAbsolutePath() ).
                                setConfig( ClusterSettings.cluster_name, name ).
                                setConfig( ClusterSettings.initial_hosts, initialHosts.toString() ).
                                setConfig( ClusterSettings.server_id, serverId + "" ).
                                setConfig( ClusterSettings.cluster_server, member.getHost() ).
                                setConfig( HaSettings.ha_server, ":" + haPort ).
                                setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE ).
                                setConfig( commonConfig );
                if ( instanceConfig.containsKey( serverId ) )
                {
                   graphDatabaseBuilder.setConfig( instanceConfig.get( serverId ) );
                }

                config( graphDatabaseBuilder, name, serverId );

                final HighlyAvailableGraphDatabaseProxy graphDatabase = new HighlyAvailableGraphDatabaseProxy(
                        graphDatabaseBuilder );

                members.put( serverId, graphDatabase );

                life.add( new LifecycleAdapter()
                {
                    @Override
                    public void stop() throws Throwable
                    {
                        graphDatabase.get().shutdown();
                    }
                } );
            }
            else
            {
                Map<String, String> config = MapUtil.stringMap(
                        ClusterSettings.cluster_name.name(), name,
                        ClusterSettings.initial_hosts.name(), initialHosts.toString(),
                        ClusterSettings.server_id.name(), serverId + "",
                        ClusterSettings.cluster_server.name(), member.getHost() );
                Config config1 = new Config( config );
                Logging clientLogging =life.add( new LogbackService(
                        config1, (LoggerContext) StaticLoggerBinder.getSingleton().getLoggerFactory() ) );
                life.add( new FutureLifecycleAdapter<ClusterClient>( new ClusterClient( ClusterClient.adapt( config1 ),
                        clientLogging, new NotElectableElectionCredentialsProvider() ) ) );
            }
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
            throw new IllegalStateException( "Awaited condition never met, waited " + maxSeconds + " for " + predicate );
        }

        /**
         * The total number of members of the cluster.
         */
        public int size()
        {
            return spec.getMembers().size();
        }
        
        public int getServerId( HighlyAvailableGraphDatabase member )
        {
            assertMember( member );
            return member.getConfig().get( ClusterSettings.server_id );
        }

        public File getStoreDir( HighlyAvailableGraphDatabase member )
        {
            assertMember( member );
            return member.getConfig().get( GraphDatabaseSettings.store_dir );
        }
        
        public void sync( HighlyAvailableGraphDatabase... except )
        {
            Set<HighlyAvailableGraphDatabase> exceptSet = new HashSet<HighlyAvailableGraphDatabase>( asList( except ) );
            for ( HighlyAvailableGraphDatabase db : getAllMembers() )
                if ( !exceptSet.contains( db ) )
                    db.getDependencyResolver().resolveDependency( UpdatePuller.class ).pullUpdates();
        }
    }

    private static final class HighlyAvailableGraphDatabaseProxy
    {
        private GraphDatabaseService result;
        private Future<GraphDatabaseService> untilThen;

        public HighlyAvailableGraphDatabaseProxy( final GraphDatabaseBuilder graphDatabaseBuilder )
        {
            Callable<GraphDatabaseService> starter = new Callable<GraphDatabaseService>()
            {
                @Override
                public GraphDatabaseService call() throws Exception
                {
                    return graphDatabaseBuilder.newGraphDatabase();
                }
            };
            ExecutorService executor = Executors.newFixedThreadPool( 1 );
            untilThen = executor.submit( starter );
        }

        public HighlyAvailableGraphDatabase get()
        {
            if ( result == null )
            {
                try
                {
                    result = untilThen.get();
                }
                catch ( InterruptedException e )
                {
                    throw new RuntimeException( e );
                }
                catch ( ExecutionException e )
                {
                    throw new RuntimeException( e );
                }
            }
            return (HighlyAvailableGraphDatabase) result;
        }
    }

    private static final class FutureLifecycleAdapter<T extends Lifecycle> extends LifecycleAdapter
    {
        private final T wrapped;
        private Future<Void> currentFuture;
        private final ExecutorService starter;

        public FutureLifecycleAdapter( T toWrap)
        {
            wrapped = toWrap;
            starter = Executors.newFixedThreadPool( 1 );
        }

        @Override
        public void init() throws Throwable
        {
            currentFuture = starter.submit( new Callable<Void>()
            {
                @Override
                public Void call() throws Exception
                {
                    try
                    {
                        wrapped.init();
                    }
                    catch ( Throwable throwable )
                    {
                        throw new RuntimeException( throwable );
                    }
                    return null;
                }
            } );
        }

        @Override
        public void start() throws Throwable
        {
            currentFuture.get();
            currentFuture = starter.submit( new Callable<Void>()
            {
                @Override
                public Void call() throws Exception
                {
                    try
                    {
                        wrapped.start();
                    }
                    catch ( Throwable throwable )
                    {
                        throw new RuntimeException( throwable );
                    }
                    return null;
                }
            } );
        }

        @Override
        public void stop() throws Throwable
        {
            currentFuture.get();
            currentFuture = starter.submit( new Callable<Void>()
            {
                @Override
                public Void call() throws Exception
                {
                    try
                    {
                        wrapped.stop();
                    }
                    catch ( Throwable throwable )
                    {
                        throw new RuntimeException( throwable );
                    }
                    return null;
                }
            } );
        }

        @Override
        public void shutdown() throws Throwable
        {
            currentFuture = starter.submit( new Callable<Void>()
            {
                @Override
                public Void call() throws Exception
                {
                    try
                    {
                        wrapped.shutdown();
                    }
                    catch ( Throwable throwable )
                    {
                        throw new RuntimeException( throwable );
                    }
                    return null;
                }
            } );
            currentFuture.get();
            starter.shutdownNow();
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
            
            @Override
            public String toString()
            {
                return "Master should see " + count + " slaves as available";
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

            @Override
            public String toString()
            {
                return "Master should see all slaves as available";
            }
        };
    }

    /**
     * There must be a master available. Optionally exceptions, useful for when awaiting a
     * re-election of a different master.
     */
    public static Predicate<ManagedCluster> masterAvailable( HighlyAvailableGraphDatabase... except )
    {
        final Set<HighlyAvailableGraphDatabase> exceptSet = new HashSet<HighlyAvailableGraphDatabase>( asList( except ) );
        return new Predicate<ClusterManager.ManagedCluster>()
        {
            @Override
            public boolean accept( ManagedCluster cluster )
            {
                for ( HighlyAvailableGraphDatabase graphDatabaseService : cluster.getAllMembers() )
                {
                    if ( !exceptSet.contains( graphDatabaseService ) && graphDatabaseService.isMaster() )
                    {
                        return true;
                    }
                }
                return false;
            }
            
            @Override
            public String toString()
            {
                return "There's an available master";
            }
        };
    }
    
    /**
     * The current master sees this many slaves as available.
     * @param count number of slaves to see as available.
     */
    public static Predicate<ManagedCluster> masterSeesMembers( final int count )
    {
        return new Predicate<ClusterManager.ManagedCluster>()
        {
            @Override
            public boolean accept( ManagedCluster cluster )
            {
//                return ((ClusterMembers)cluster.getMaster().getDependencyResolver().resolveDependency( Slaves.class )).getMembers().length >= count;
                Neo4jManager jmx = new Neo4jManager( cluster.getMaster().getDependencyResolver().resolveDependency( JmxKernelExtension
                        .class ).getSingleManagementBean( Kernel.class ) );
                return jmx.getHighAvailabilityBean().getInstancesInCluster().length >= count;
            }
            
            @Override
            public String toString()
            {
                return "Master should see " + count + " members";
            }
        };
    }

    public static Predicate<ManagedCluster> allSeesAllAsAvailable()
    {
        return new Predicate<ManagedCluster>()
        {
            @Override
            public boolean accept( ManagedCluster cluster )
            {

                for ( HighlyAvailableGraphDatabase database : cluster.getAllMembers() )
                {
                    Neo4jManager jmx = new Neo4jManager( database.getDependencyResolver().resolveDependency( JmxKernelExtension.class ).getSingleManagementBean( Kernel.class ) );

                    for ( ClusterMemberInfo clusterMemberInfo : jmx.getHighAvailabilityBean().getInstancesInCluster() )
                    {
                        if (!clusterMemberInfo.isAvailable())
                            return false;
                    }
                }

                // Everyone sees everyone else as available!
                return true;
            }

            @Override
            public String toString()
            {
                return "All instances should see all others as available";
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

    protected void insertInitialData( GraphDatabaseService db, String name, int serverId )
    {
    }
    
    public interface RepairKit
    {
        HighlyAvailableGraphDatabase repair() throws Throwable;
    }

    private class StartNetworkAgainKit implements RepairKit
    {
        private final HighlyAvailableGraphDatabase db;
        private final NetworkInstance network;

        StartNetworkAgainKit( HighlyAvailableGraphDatabase db, NetworkInstance network )
        {
            this.db = db;
            this.network = network;
        }

        @Override
        public HighlyAvailableGraphDatabase repair() throws Throwable
        {
            network.start();
            return db;
        }
    }

    private class StartDatabaseAgainKit implements RepairKit
    {
        private final int serverId;
        private final ManagedCluster cluster;

        public StartDatabaseAgainKit( ManagedCluster cluster, int serverId )
        {
            this.cluster = cluster;
            this.serverId = serverId;
        }

        @Override
        public HighlyAvailableGraphDatabase repair() throws Throwable
        {
            cluster.startMember( serverId );
            return cluster.getMemberByServerId( serverId );
        }
    }
}
