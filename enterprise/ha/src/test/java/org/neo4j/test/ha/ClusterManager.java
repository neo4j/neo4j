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
package org.neo4j.test.ha;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import ch.qos.logback.classic.LoggerContext;
import org.w3c.dom.Document;

import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.cluster.client.Clusters;
import org.neo4j.cluster.client.ClustersXMLSerializer;
import org.neo4j.cluster.com.NetworkReceiver;
import org.neo4j.cluster.com.NetworkSender;
import org.neo4j.cluster.member.ClusterMemberEvents;
import org.neo4j.cluster.member.ClusterMemberListener;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectStreamFactory;
import org.neo4j.cluster.protocol.election.NotElectableElectionCredentialsProvider;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.graphdb.factory.TestHighlyAvailableGraphDatabaseFactory;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.Settings;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.UpdatePullerClient;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState;
import org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.ha.cluster.member.ClusterMember;
import org.neo4j.kernel.ha.cluster.member.ClusterMembers;
import org.neo4j.kernel.ha.com.master.Slaves;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.logging.LogbackService;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;

import static org.junit.Assert.fail;

import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.io.fs.FileUtils.copyRecursively;

public class ClusterManager
        extends LifecycleAdapter
{
    public interface StoreDirInitializer
    {
        void initializeStoreDir( int serverId, File storeDir ) throws IOException;
    }

    /**
     * Provides a specification of which clusters to start in {@link ClusterManager#start()}.
     */
    public interface Provider
    {
        Clusters clusters() throws Throwable;
    }

    public interface RepairKit
    {
        HighlyAvailableGraphDatabase repair() throws Throwable;
    }

    private final File root;
    private final Map<String,String> commonConfig;
    private final Map<Integer,Map<String,String>> instanceConfig;
    private final Map<String,ManagedCluster> clusterMap = new HashMap<>();
    private final Provider clustersProvider;
    private final HighlyAvailableGraphDatabaseFactory dbFactory;
    private final StoreDirInitializer storeDirInitializer;
    LifeSupport life;

    public ClusterManager( Provider clustersProvider, File root, Map<String,String> commonConfig,
                           Map<Integer,Map<String,String>> instanceConfig,
                           HighlyAvailableGraphDatabaseFactory dbFactory )
    {
        this.clustersProvider = clustersProvider;
        this.root = root;
        this.commonConfig = commonConfig;
        this.instanceConfig = instanceConfig;
        this.dbFactory = dbFactory;
        this.storeDirInitializer = null;
    }

    public ClusterManager( Builder builder )
    {
        this.clustersProvider = builder.provider;
        this.root = builder.root;
        this.commonConfig = builder.commonConfig;
        this.instanceConfig = builder.instanceConfig;
        this.dbFactory = builder.factory;
        this.storeDirInitializer = builder.initializer;
    }

    public ClusterManager( Provider clustersProvider, File root, Map<String,String> commonConfig,
                           Map<Integer,Map<String,String>> instanceConfig )
    {
        this( clustersProvider, root, commonConfig, instanceConfig, new TestHighlyAvailableGraphDatabaseFactory() );
    }

    public ClusterManager( Provider clustersProvider, File root, Map<String,String> commonConfig )
    {
        this( clustersProvider, root, commonConfig, Collections.<Integer,Map<String,String>>emptyMap(),
                new TestHighlyAvailableGraphDatabaseFactory() );
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
     *
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
     *
     * @param haMemberCount the total number of members in the cluster to start.
     */
    public static Provider clusterWithAdditionalArbiters( int haMemberCount, int arbiterCount )
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
        final Set<HighlyAvailableGraphDatabase> exceptSet = new HashSet<>( asList( except ) );
        return new Predicate<ClusterManager.ManagedCluster>()
        {
            @Override
            public boolean accept( ManagedCluster cluster )
            {
                for ( HighlyAvailableGraphDatabase graphDatabaseService : cluster.getAllMembers() )
                {
                    if ( !exceptSet.contains( graphDatabaseService ) )
                    {
                        if ( graphDatabaseService.isAvailable( 0 ) && graphDatabaseService.isMaster() )
                        {
                            return true;
                        }
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
     *
     * @param count number of slaves to see as available.
     */
    public static Predicate<ManagedCluster> masterSeesMembers( final int count )
    {
        return new Predicate<ClusterManager.ManagedCluster>()
        {
            @Override
            public boolean accept( ManagedCluster cluster )
            {
                ClusterMembers members =
                        cluster.getMaster().getDependencyResolver().resolveDependency( ClusterMembers.class );
                return count( members.getMembers() ) == count;
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
                if ( !allSeesAllAsJoined().accept( cluster ) )
                {
                    return false;
                }

                for ( HighlyAvailableGraphDatabase database : cluster.getAllMembers() )
                {
                    ClusterMembers members = database.getDependencyResolver().resolveDependency( ClusterMembers.class );

                    for ( ClusterMember clusterMember : members.getMembers() )
                    {
                        if ( clusterMember.getHARole().equals( HighAvailabilityModeSwitcher.UNKNOWN ) )
                        {
                            return false;
                        }
                    }
                }

                // Everyone sees everyone else as available!
                for( HighlyAvailableGraphDatabase database : cluster.getAllMembers() )
                {
                    StringLogger logger = database.getDependencyResolver().resolveDependency( StringLogger.class );
                    logger.debug( this.toString() );
                }
                return true;
            }

            @Override
            public String toString()
            {
                return "All instances should see all others as available";
            }
        };
    }

    public static Predicate<ManagedCluster> allSeesAllAsJoined()
    {
        return new Predicate<ManagedCluster>()
        {
            @Override
            public boolean accept( ManagedCluster cluster )
            {
                int nrOfMembers = cluster.spec.getMembers().size();

                for ( HighlyAvailableGraphDatabase database : cluster.getAllMembers() )
                {
                    ClusterMembers members = database.getDependencyResolver().resolveDependency( ClusterMembers.class );

                    if ( count( members.getMembers() ) < nrOfMembers )
                    {
                        return false;
                    }
                }

                for ( ClusterMembers clusterMembers : cluster.getArbiters() )
                {
                    if ( count( clusterMembers.getMembers() ) < nrOfMembers )
                    {
                        return false;
                    }
                }

                // Everyone sees everyone else as joined!
                return true;
            }

            @Override
            public String toString()
            {
                return "All instances should see all others as joined";
            }
        };
    }

    public static Predicate<ManagedCluster> allAvailabilityGuardsReleased()
    {
        return new Predicate<ManagedCluster>()
        {
            @Override
            public boolean accept( ManagedCluster item )
            {
                for ( HighlyAvailableGraphDatabaseProxy member : item.members.values() )
                {
                    try
                    {
                        member.get().beginTx().close();
                    }
                    catch ( TransactionFailureException e )
                    {
                        return false;
                    }
                }
                return true;
            }
        };
    }

    public static String stateToString( ManagedCluster cluster )
    {
        StringBuilder buf = new StringBuilder( "\n" );
        for ( HighlyAvailableGraphDatabase database : cluster.getAllMembers() )
        {
            ClusterClient client = database.getDependencyResolver().resolveDependency( ClusterClient.class );
            buf.append( "Instance " ).append( client.getServerId() )
               .append( ":State " ).append( database.getInstanceState() )
               .append( " (" ).append( client.getClusterServer() ).append( "):" ).append( "\n" );

            ClusterMembers members = database.getDependencyResolver().resolveDependency( ClusterMembers.class );

            for ( ClusterMember clusterMember : members.getMembers() )
            {
                buf.append( "  " ).append( clusterMember.getInstanceId() ).append( ":" )
                   .append( clusterMember.getHARole() )
                   .append( " (is alive = " ).append( clusterMember.isAlive() ).append( ")" )
                   .append( "\n" );
            }
        }

        return buf.toString();
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

    @SuppressWarnings( "unchecked" )
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

    protected void config( GraphDatabaseBuilder builder, String clusterName, InstanceId serverId )
    {
    }

    protected void insertInitialData( GraphDatabaseService db, String name, InstanceId serverId )
    {
    }

    public static class Builder
    {
        private final File root;
        private final Map<Integer,Map<String,String>> instanceConfig = new HashMap<>();
        private Provider provider = clusterOfSize( 3 );
        private Map<String,String> commonConfig = emptyMap();
        private HighlyAvailableGraphDatabaseFactory factory = new TestHighlyAvailableGraphDatabaseFactory();
        private StoreDirInitializer initializer;

        public Builder( File root )
        {
            this.root = root;
        }

        public Builder withSeedDir( final File seedDir )
        {
            return withStoreDirInitializer( new StoreDirInitializer()
            {
                @Override
                public void initializeStoreDir( int serverId, File storeDir ) throws IOException
                {
                    copyRecursively( seedDir, storeDir );
                }
            } );
        }

        public Builder withStoreDirInitializer( StoreDirInitializer initializer )
        {
            this.initializer = initializer;
            return this;
        }

        public Builder withDbFactory( HighlyAvailableGraphDatabaseFactory dbFactory )
        {
            this.factory = dbFactory;
            return this;
        }

        public Builder withProvider( Provider provider )
        {
            this.provider = provider;
            return this;
        }

        public Builder withCommonConfig( Map<String,String> commonConfig )
        {
            this.commonConfig = commonConfig;
            return this;
        }

        public Builder withInstanceConfig( int instanceNr, Map<String,String> instanceConfig )
        {
            this.instanceConfig.put( instanceNr, instanceConfig );
            return this;
        }

        public ClusterManager build()
        {
            return new ClusterManager( this );
        }
    }

    private static final class HighlyAvailableGraphDatabaseProxy
    {
        private final ExecutorService executor;
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
            executor = Executors.newFixedThreadPool( 1 );
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
                catch ( InterruptedException | ExecutionException e )
                {
                    throw new RuntimeException( e );
                }
                finally
                {
                    executor.shutdownNow();
                }
            }
            return (HighlyAvailableGraphDatabase) result;
        }
    }

    private static final class FutureLifecycleAdapter<T extends Lifecycle> extends LifecycleAdapter
    {
        private final T wrapped;
        private final ExecutorService starter;
        private Future<Void> currentFuture;

        public FutureLifecycleAdapter( T toWrap )
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
     * Represent one cluster. It can retrieve the current master, random slave
     * or all members. It can also temporarily fail an instance or shut it down.
     */
    public class ManagedCluster extends LifecycleAdapter
    {
        private final Clusters.Cluster spec;
        private final String name;
        private final Map<InstanceId,HighlyAvailableGraphDatabaseProxy> members = new ConcurrentHashMap<>();
        private final List<ClusterMembers> arbiters = new ArrayList<>();

        ManagedCluster( Clusters.Cluster spec ) throws URISyntaxException, IOException
        {
            this.spec = spec;
            this.name = spec.getName();
            for ( int i = 0; i < spec.getMembers().size(); i++ )
            {
                startMember( new InstanceId( i + 1 ) );
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
            {
                result.append( result.length() > 0 ? "," : "" ).append( ":" )
                      .append( member.getDependencyResolver().resolveDependency(
                              ClusterClient.class ).getClusterServer().getPort() );
            }
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
            return Iterables.map( new Function<HighlyAvailableGraphDatabaseProxy,HighlyAvailableGraphDatabase>()
            {

                @Override
                public HighlyAvailableGraphDatabase apply( HighlyAvailableGraphDatabaseProxy from )
                {
                    return from.get();
                }
            }, members.values() );
        }

        public Iterable<ClusterMembers> getArbiters()
        {
            return arbiters;
        }

        /**
         * @return the current master in the cluster.
         * @throws IllegalStateException if there's no current master.
         */
        public HighlyAvailableGraphDatabase getMaster()
        {
            for ( HighlyAvailableGraphDatabase graphDatabaseService : getAllMembers() )
            {
                if ( graphDatabaseService.isAvailable( 0 ) && graphDatabaseService.isMaster() )
                {
                    return graphDatabaseService;
                }
            }
            throw new IllegalStateException( "No master found in cluster " + name + stateToString( this ) );
        }

        /**
         * @param except do not return any of the dbs found in this array
         * @return a slave in this cluster.
         * @throws IllegalStateException if no slave was found in this cluster.
         */
        public HighlyAvailableGraphDatabase getAnySlave( HighlyAvailableGraphDatabase... except )
        {
            Set<HighlyAvailableGraphDatabase> exceptSet = new HashSet<>( asList( except ) );
            for ( HighlyAvailableGraphDatabase graphDatabaseService : getAllMembers() )
            {
                if ( graphDatabaseService.getInstanceState() == HighAvailabilityMemberState.SLAVE
                        && !exceptSet.contains( graphDatabaseService ) )
                {
                    return graphDatabaseService;
                }
            }
            throw new IllegalStateException( "No slave found in cluster " + name + stateToString( this ) );
        }

        /**
         * @param serverId the server id to return the db for.
         * @return the {@link HighlyAvailableGraphDatabase} with the given server id.
         * @throws IllegalStateException if that db isn't started or no such
         * db exists in the cluster.
         */
        public HighlyAvailableGraphDatabase getMemberByServerId( InstanceId serverId )
        {
            HighlyAvailableGraphDatabase db = members.get( serverId ).get();
            if ( db == null )
            {
                throw new IllegalStateException( "Db " + serverId + " not found at the moment in " + name +
                                                 stateToString( this ) );
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
            InstanceId serverId =
                    db.getDependencyResolver().resolveDependency( Config.class ).get( ClusterSettings.server_id );
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
            throw new IllegalArgumentException( "Db " + db + " not a member of this cluster " + name +
                                                stateToString( this ) );
        }

        /**
         * WARNING: beware of hacks.
         * <p>
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

            NetworkReceiver networkReceiver = instance( NetworkReceiver.class, clusterClientLife.getLifecycleInstances() );

            NetworkSender networkSender = instance( NetworkSender.class, clusterClientLife.getLifecycleInstances() );

            networkReceiver.setPaused( true );
            networkSender.setPaused( true );

            return new StartNetworkAgainKit( db, networkReceiver, networkSender );
        }

        private void startMember( InstanceId serverId ) throws URISyntaxException, IOException
        {
            Clusters.Member member = spec.getMembers().get( serverId.toIntegerIndex() - 1 );
            StringBuilder initialHosts = new StringBuilder( spec.getMembers().get( 0 ).getHost() );
            for ( int i = 1; i < spec.getMembers().size(); i++ )
            {
                initialHosts.append( "," ).append( spec.getMembers().get( i ).getHost() );
            }
            File parent = new File( root, name );
            URI clusterUri = new URI( "cluster://" + member.getHost() );
            if ( member.isFullHaMember() )
            {
                int clusterPort = clusterUri.getPort();
                int haPort = clusterUri.getPort() + 3000;
                File storeDir = new File( parent, "server" + serverId );
                if ( storeDirInitializer != null )
                {
                    storeDirInitializer.initializeStoreDir( serverId.toIntegerIndex(), storeDir );
                }
                GraphDatabaseBuilder builder =
                        dbFactory.newHighlyAvailableDatabaseBuilder( storeDir.getAbsolutePath() );
                builder.setConfig( ClusterSettings.cluster_name, name );
                builder.setConfig( ClusterSettings.initial_hosts, initialHosts.toString() );
                builder.setConfig( ClusterSettings.server_id, serverId + "" );
                builder.setConfig( ClusterSettings.cluster_server, "0.0.0.0:" + clusterPort );
                builder.setConfig( HaSettings.ha_server, ":" + haPort );
                builder.setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE );
                builder.setConfig( commonConfig );
                if ( instanceConfig.containsKey( serverId.toIntegerIndex() ) )
                {
                    builder.setConfig( instanceConfig.get( serverId.toIntegerIndex() ) );
                }

                config( builder, name, serverId );

                final HighlyAvailableGraphDatabaseProxy graphDatabase = new HighlyAvailableGraphDatabaseProxy(
                        builder );

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
                Map<String,String> config = MapUtil.stringMap(
                        ClusterSettings.cluster_name.name(), name,
                        ClusterSettings.initial_hosts.name(), initialHosts.toString(),
                        ClusterSettings.server_id.name(), serverId + "",
                        ClusterSettings.cluster_server.name(), "0.0.0.0:" + clusterUri.getPort(),
                        GraphDatabaseSettings.store_dir.name(),
                        new File( parent, "arbiter" + serverId ).getAbsolutePath() );
                Config config1 = new Config( config, InternalAbstractGraphDatabase.Configuration.class,
                        GraphDatabaseSettings.class );

                Logging clientLogging = life.add( new LogbackService( config1, new LoggerContext() ) );
                ObjectStreamFactory objectStreamFactory = new ObjectStreamFactory();
                ClusterClient clusterClient = new ClusterClient( new Monitors(), ClusterClient.adapt( config1 ),
                        clientLogging, new NotElectableElectionCredentialsProvider(), objectStreamFactory,
                        objectStreamFactory );

                arbiters.add( new ClusterMembers( clusterClient, clusterClient, new ClusterMemberEvents()
                {
                    @Override
                    public void addClusterMemberListener( ClusterMemberListener listener )
                    {
                        // noop
                    }

                    @Override
                    public void removeClusterMemberListener( ClusterMemberListener listener )
                    {
                        // noop
                    }
                }, clusterClient.getServerId() ) );

                life.add( new FutureLifecycleAdapter<>( clusterClient ) );
            }
        }

        /**
         * Will await a condition for the default max time.
         *
         * @param predicate {@link Predicate} that should return true
         * signalling that the condition has been met.
         * @throws IllegalStateException if the condition wasn't met
         * during within the max time.
         */
        public void await( Predicate<ManagedCluster> predicate )
        {
            await( predicate, 60 );
        }

        /**
         * Will await a condition for the given max time.
         *
         * @param predicate {@link Predicate} that should return true
         * signalling that the condition has been met.
         * @throws IllegalStateException if the condition wasn't met
         * during within the max time.
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
            String state = stateToString( this );
            throw new IllegalStateException( format(
                    "Awaited condition never met, waited %s secondes for %s:%n%s", maxSeconds, predicate, state ) );
        }

        /**
         * The total number of members of the cluster.
         */
        public int size()
        {
            return spec.getMembers().size();
        }

        public InstanceId getServerId( HighlyAvailableGraphDatabase member )
        {
            assertMember( member );
            return member.getConfig().get( ClusterSettings.server_id );
        }

        public File getStoreDir( HighlyAvailableGraphDatabase member )
        {
            assertMember( member );
            return member.getConfig().get( GraphDatabaseSettings.store_dir );
        }

        public void sync( HighlyAvailableGraphDatabase... except ) throws InterruptedException
        {
            Set<HighlyAvailableGraphDatabase> exceptSet = new HashSet<>( asList( except ) );
            for ( HighlyAvailableGraphDatabase db : getAllMembers() )
            {
                if ( !exceptSet.contains( db ) )
                {
                    UpdatePullerClient client = db.getDependencyResolver().resolveDependency( UpdatePullerClient.class );
                    try
                    {
                        if ( db.isAvailable( 60000 ) ) // wait for 1 min for db to become available
                        {
                            client.pullUpdates();
                        }
                    }
                    catch ( Exception e )
                    {
                        throw new IllegalStateException( stateToString( this ), e );
                    }
                }
            }
        }

        public void info( String message )
        {
            for ( HighlyAvailableGraphDatabase db : getAllMembers() )
            {
                Logging logging = db.getDependencyResolver().resolveDependency( Logging.class );
                StringLogger messagesLog = logging.getMessagesLog( HighlyAvailableGraphDatabase.class );
                messagesLog.info( message );
            }
        }
    }

    private class StartNetworkAgainKit implements RepairKit
    {
        private final HighlyAvailableGraphDatabase db;
        private final NetworkReceiver networkReceiver;
        private final NetworkSender networkSender;

        StartNetworkAgainKit( HighlyAvailableGraphDatabase db,
                NetworkReceiver networkReceiver,
                NetworkSender networkSender )
        {
            this.db = db;
            this.networkReceiver = networkReceiver;
            this.networkSender = networkSender;
        }

        @Override
        public HighlyAvailableGraphDatabase repair() throws Throwable
        {
            networkSender.setPaused( false );
            networkReceiver.setPaused( false );

            return db;
        }
    }

    private class StartDatabaseAgainKit implements RepairKit
    {
        private final InstanceId serverId;
        private final ManagedCluster cluster;

        public StartDatabaseAgainKit( ManagedCluster cluster, InstanceId serverId )
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
