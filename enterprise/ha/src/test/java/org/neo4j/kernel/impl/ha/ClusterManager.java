/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.ha;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.client.Cluster;
import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.cluster.client.ClusterClientModule;
import org.neo4j.cluster.com.NetworkReceiver;
import org.neo4j.cluster.com.NetworkSender;
import org.neo4j.cluster.member.ClusterMemberEvents;
import org.neo4j.cluster.member.ClusterMemberListener;
import org.neo4j.cluster.protocol.election.NotElectableElectionCredentialsProvider;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.graphdb.factory.builder.GraphDatabaseBuilder;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.UpdatePuller;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState;
import org.neo4j.kernel.ha.cluster.member.ClusterMember;
import org.neo4j.kernel.ha.cluster.member.ClusterMembers;
import org.neo4j.kernel.ha.cluster.member.ObservedClusterMembers;
import org.neo4j.kernel.ha.cluster.modeswitch.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.ha.com.master.Slaves;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.impl.util.Listener;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.storageengine.api.StorageEngine;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableMap;
import static org.neo4j.helpers.ArrayUtil.contains;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.io.fs.FileUtils.copyRecursively;

/**
 * Utility for spinning up an HA cluster inside the same JVM. Only intended for being used in tests
 * as well as other tools that may need a cluster conveniently within the same JVM.
 */
public class ClusterManager
        extends LifecycleAdapter
{
    private static final int CLUSTER_MIN_PORT = 10_000;
    private static final int CLUSTER_MAX_PORT = 20_000;
    private static final int HA_MIN_PORT = CLUSTER_MAX_PORT + 1;
    private static final int HA_MAX_PORT = HA_MIN_PORT + 10_000;

    /**
     * Network Flags for passing into {@link ManagedCluster#fail(HighlyAvailableGraphDatabase, NetworkFlag...)}
     */
    public enum NetworkFlag
    {
        /**
         * Fail outgoing cluster network traffic.
         */
        OUT,
        /**
         * Fail incoming cluster network traffic.
         */
        IN;
    }

    public static final long DEFAULT_TIMEOUT_SECONDS = 60L;
    public static final Map<String,String> CONFIG_FOR_SINGLE_JVM_CLUSTER = unmodifiableMap( stringMap(
            GraphDatabaseSettings.pagecache_memory.name(), "8m" ) );

    public interface StoreDirInitializer
    {
        void initializeStoreDir( int serverId, File storeDir ) throws IOException;
    }

    public interface RepairKit
    {
        HighlyAvailableGraphDatabase repair() throws Throwable;
    }

    public static IntFunction<String> constant( final String value )
    {
        return ignored -> value;
    }

    private final String localAddress;
    private final File root;
    private final Map<String,IntFunction<String>> commonConfig;
    private final Supplier<Cluster> clustersProvider;
    private final HighlyAvailableGraphDatabaseFactory dbFactory;
    private final StoreDirInitializer storeDirInitializer;
    private final Listener<GraphDatabaseService> initialDatasetCreator;
    private final List<Predicate<ManagedCluster>> availabilityChecks;
    private ManagedCluster managedCluster;
    LifeSupport life;

    private ClusterManager( Builder builder )
    {
        this.localAddress = getLocalAddress();
        this.clustersProvider = builder.provider;
        this.root = builder.root;
        this.commonConfig = withDefaults( builder.commonConfig );
        this.dbFactory = builder.factory;
        this.storeDirInitializer = builder.initializer;
        this.initialDatasetCreator = builder.initialDatasetCreator;
        this.availabilityChecks = builder.availabilityChecks;
    }

    private Map<String,IntFunction<String>> withDefaults( Map<String,IntFunction<String>> commonConfig )
    {
        Map<String,IntFunction<String>> result = new HashMap<>();
        for ( Map.Entry<String,String> conf : CONFIG_FOR_SINGLE_JVM_CLUSTER.entrySet() )
        {
            result.put( conf.getKey(), constant( conf.getValue() ) );
        }
        result.putAll( commonConfig );
        return result;
    }

    private static String getLocalAddress()
    {
        try
        {
            // Null corresponds to localhost
            return InetAddress.getByName( null ).getHostAddress();
        }
        catch ( UnknownHostException e )
        {
            // Fetching the localhost address won't throw this exception, so this should never happen, but if it
            // were, then the computer doesn't even have a loopback interface, so crash now rather than later
            throw new AssertionError( e );
        }
    }

    /**
     * Provides a cluster specification with default values
     *
     * @param memberCount the total number of members in the cluster to start.
     */
    public static Supplier<Cluster> clusterOfSize( int memberCount )
    {
        return clusterOfSize( getLocalAddress(), memberCount );
    }

    /**
     * Provides a cluster specification with default values on specified hostname
     *
     * @param hostname the hostname/ip-address to bind to
     * @param memberCount the total number of members in the cluster to start.
     */
    public static Supplier<Cluster> clusterOfSize( String hostname, int memberCount )
    {
        return () -> {
            final Cluster cluster = new Cluster();


            HashSet<Integer> takenPorts = new HashSet<>();
            try
            {
                for ( int i = 0; i < memberCount; i++ )
                {
                    int port = findFreePort( CLUSTER_MIN_PORT, CLUSTER_MAX_PORT, takenPorts );
                    takenPorts.add( port );
                    cluster.getMembers().add( new Cluster.Member( hostname + ":" + port, true ) );
                }
            }
            catch ( IOException e )
            {
                // you can't throw a normal exception in a TestRule
                throw new AssertionError( "Failed to find an open port" );
            }
            return cluster;
        };
    }

    /**
     * Find an available port number which is also not included in the except list.
     * @param minPort minimum port number to probe
     * @param maxPort maximum port number to probe
     * @param except port numbers which should be considered unavailable and already taken
     * @return a port number
     * @throws IOException if no open port could be found
     */
    private static int findFreePort( final int minPort, final int maxPort, final Set<Integer> except )
            throws IOException
    {
        int port;
        for ( port = minPort; port <= maxPort; port++ )
        {
            if ( except.contains( port ) )
            {
                // This port is already taken but not bound yet, ignore it
                continue;
            }
            try
            {
                ServerSocket socket = new ServerSocket( port );
                // Port is available, return it
                socket.close();
                return port;
            }
            catch ( IOException ex )
            {
                // Port was already bound, try the next one
                except.add( port );
            }
        }

        throw new IOException( "No open port could be found" );
    }

    /**
     * Provides a cluster specification with default values
     *
     * @param haMemberCount the total number of members in the cluster to start.
     */
    public static Supplier<Cluster> clusterWithAdditionalClients( int haMemberCount, int additionalClientCount )
    {
        return () -> {
            Cluster cluster = new Cluster();
            HashSet<Integer> takenPorts = new HashSet<>();

            try
            {
                for ( int i = 0; i < haMemberCount; i++ )
                {
                    int port = findFreePort( CLUSTER_MIN_PORT, CLUSTER_MAX_PORT, takenPorts );
                    takenPorts.add( port );
                    cluster.getMembers().add( new Cluster.Member( port, true ) );
                }
                for ( int i = 0; i < additionalClientCount; i++ )
                {
                    int port = findFreePort( CLUSTER_MIN_PORT, CLUSTER_MAX_PORT, takenPorts );
                    takenPorts.add( port );
                    cluster.getMembers().add( new Cluster.Member( port, false ) );
                }
            }
            catch ( IOException e )
            {
                // you can't throw a normal exception in a TestRule
                throw new AssertionError( "Failed to find an open port" );
            }

            return cluster;
        };
    }

    /**
     * Provides a cluster specification with default values
     *
     * @param haMemberCount the total number of members in the cluster to start.
     */
    public static Supplier<Cluster> clusterWithAdditionalArbiters( int haMemberCount, int arbiterCount )
    {
        return () -> {
            Cluster cluster = new Cluster();
            HashSet<Integer> takenPorts = new HashSet<>();

            try
            {
                for ( int i = 0; i < arbiterCount; i++ )
                {
                    int port = findFreePort( CLUSTER_MIN_PORT, CLUSTER_MAX_PORT, takenPorts );
                    takenPorts.add( port );
                    cluster.getMembers().add( new Cluster.Member( port, false ) );
                }
                for ( int i = 0; i < haMemberCount; i++ )
                {
                    int port = findFreePort( CLUSTER_MIN_PORT, CLUSTER_MAX_PORT, takenPorts );
                    takenPorts.add( port );
                    cluster.getMembers().add( new Cluster.Member( port, true ) );
                }
            }
            catch ( IOException e )
            {
                // you can't throw a normal exception in a TestRule
                throw new AssertionError( "Failed to find an open port" );
            }
            return cluster;
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
            public boolean test( ManagedCluster cluster )
            {
                return count( cluster.getMaster().getDependencyResolver().resolveDependency( Slaves.class )
                        .getSlaves() ) >= count;
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
            public boolean test( ManagedCluster cluster )
            {
                return count( cluster.getMaster().getDependencyResolver().resolveDependency( Slaves.class )
                        .getSlaves() ) >= cluster.size() - 1;
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
    public static Predicate<ManagedCluster> masterAvailable( final HighlyAvailableGraphDatabase... except )
    {
        final Collection<HighlyAvailableGraphDatabase> excludedNodes = asList( except );
        return new Predicate<ClusterManager.ManagedCluster>()
        {
            @Override
            public boolean test( ManagedCluster cluster )
            {
                Predicate<HighlyAvailableGraphDatabase> filterMasterPredicate =
                        node -> !excludedNodes.contains( node ) &&
                               node.isAvailable( 0 ) &&
                               node.isMaster();
                return Iterables.filter( filterMasterPredicate, cluster.getAllMembers() ).iterator().hasNext();
            }

            @Override
            public String toString()
            {
                return "There's an available master";
            }
        };
    }

    /**
     * The current master sees this many members (including itself).
     *
     * @param count number of members to see.
     */
    public static Predicate<ManagedCluster> masterSeesMembers( final int count )
    {
        return new Predicate<ClusterManager.ManagedCluster>()
        {
            @Override
            public boolean test( ManagedCluster cluster )
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
            public boolean test( ManagedCluster cluster )
            {
                if ( !allSeesAllAsJoined().test( cluster ) )
                {
                    return false;
                }

                for ( HighlyAvailableGraphDatabase database : cluster.getAllMembers() )
                {
                    ClusterMembers members = database.getDependencyResolver().resolveDependency( ClusterMembers.class );

                    for ( ClusterMember clusterMember : members.getMembers() )
                    {
                        if ( !cluster.isAvailable( clusterMember ) )
                        {
                            return false;
                        }
                    }
                }

                // Everyone sees everyone else as available!
                for ( HighlyAvailableGraphDatabase database : cluster.getAllMembers() )
                {
                    Log log = database.getDependencyResolver().resolveDependency( LogService.class ).getInternalLog(
                            getClass() );
                    log.debug( this.toString() );
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
            public boolean test( ManagedCluster cluster )
            {
                int clusterSize = cluster.size();

                for ( HighlyAvailableGraphDatabase database : cluster.getAllMembers() )
                {
                    ClusterMembers members = database.getDependencyResolver().resolveDependency( ClusterMembers.class );

                    if ( count( members.getMembers() ) < clusterSize )
                    {
                        return false;
                    }
                }

                for ( ObservedClusterMembers arbiter : cluster.getArbiters() )
                {
                    if ( count( arbiter.getMembers() ) < clusterSize )
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
        return item -> {
            for ( HighlyAvailableGraphDatabase member : item.getAllMembers())
            {
                try
                {
                    member.beginTx().close();
                }
                catch ( TransactionFailureException e )
                {
                    return false;
                }
            }
            return true;
        };
    }

    public static Predicate<ManagedCluster> memberSeesOtherMemberAsFailed(
            final HighlyAvailableGraphDatabase observer, final HighlyAvailableGraphDatabase observed )
    {
        return cluster -> {
            InstanceId observedServerId = observed.getDependencyResolver().resolveDependency( Config.class )
                    .get( ClusterSettings.server_id );
            for ( ClusterMember member : observer.getDependencyResolver().resolveDependency(
                    ClusterMembers.class ).getMembers() )
            {
                if ( member.getInstanceId().equals( observedServerId ) )
                {
                    return !member.isAlive();
                }
            }
            throw new IllegalStateException( observed + " not a member according to " + observer );
        };
    }

    public static Predicate<ManagedCluster> entireClusterSeesMemberAsNotAvailable(
            final HighlyAvailableGraphDatabase observed )
    {
        return cluster -> {
            InstanceId observedServerId = observed.getDependencyResolver().resolveDependency( Config.class ).get(
                    ClusterSettings.server_id );

            for ( HighlyAvailableGraphDatabase observer : cluster.getAllMembers( observed ) )
            {
                for ( ClusterMember member : observer.getDependencyResolver().resolveDependency( ClusterMembers.class )
                        .getMembers() )
                {
                    if ( member.getInstanceId().equals( observedServerId ) )
                    {
                        if ( cluster.isAvailable( member ) )
                        {
                            return false;
                        } // else, keep looping to see if anyone else sees it as alive
                    }
                }
            }
            // No one could see it as alive
            return true;
        };
    }

    public static Predicate<ManagedCluster> memberThinksItIsRole(
            final HighlyAvailableGraphDatabase member, final String role )
    {
        return cluster -> role.equals( member.role() );
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
            buf.append( members );
        }

        return buf.toString();
    }


    @Override
    public void start() throws Throwable
    {
        Cluster cluster = clustersProvider.get();

        life = new LifeSupport();

        // Started so instances added here will be started immediately, and in case of exceptions they can be
        // shutdown() or stop()ped properly
        life.start();

        managedCluster = new ManagedCluster( cluster );
        life.add( managedCluster );

        availabilityChecks.forEach( managedCluster::await );

        if ( initialDatasetCreator != null )
        {
            initialDatasetCreator.receive( managedCluster.getMaster() );
            managedCluster.sync();
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
     * Shutdown the cluster and catch any exceptions which might be thrown as a result. If an exception is thrown,
     * the stacktrace is printed.
     *
     * This is intended for unit tests where a failure in cluster shutdown might mask the actual error in the test.
     */
    public void safeShutdown() {
        try
        {
            shutdown();
        }
        catch ( Throwable throwable )
        {
            throwable.printStackTrace();
        }
    }

    public ManagedCluster getCluster()
    {
        return managedCluster;
    }

    public interface ClusterBuilder<SELF>
    {
        SELF withRootDirectory( File root );

        SELF withSeedDir( File seedDir );

        SELF withStoreDirInitializer( StoreDirInitializer initializer );

        SELF withDbFactory( HighlyAvailableGraphDatabaseFactory dbFactory );

        SELF withCluster( Supplier<Cluster> provider );

        /**
         * Supplies configuration where config values, as opposed to {@link #withSharedConfig(Map)},
         * are a function of (one-based) server id. The function may return {@code null} which means
         * that the particular member doesn't have that config value, or at least not specifically
         * set, such that any default value would be used.
         */
        SELF withInstanceConfig( Map<String,IntFunction<String>> commonConfig );

        /**
         * Like {@link #withInstanceConfig(Map)}, but for individual settings, conveniently using
         * {@link Setting} instance as key as well.
         */
        SELF withInstanceSetting( Setting<?> setting, IntFunction<String> valueFunction );

        /**
         * Supplies configuration where config values are shared with all instances in the cluster.
         */
        SELF withSharedConfig( Map<String,String> commonConfig );

        /**
         * Like {@link #withInstanceSetting(Setting, IntFunction)}, but for individual settings, conveniently using
         * {@link Setting} instance as key as well.
         */
        SELF withSharedSetting( Setting<?> setting, String value );

        /**
         * Initial dataset to be created once the cluster is up and running.
         *
         * @param transactor the {@link Listener} receiving a call to create the dataset on the master.
         */
        SELF withInitialDataset( Listener<GraphDatabaseService> transactor );

        /**
         * Checks that must pass before cluster is considered to be up.
         *
         * @param checks availability checks that must pass before considering the cluster online.
         */
        SELF withAvailabilityChecks( Predicate<ManagedCluster>... checks );
    }

    public static class Builder implements ClusterBuilder<Builder>
    {
        private File root;
        private Supplier<Cluster> provider = clusterOfSize( 3 );
        private final Map<String,IntFunction<String>> commonConfig = new HashMap<>();
        private HighlyAvailableGraphDatabaseFactory factory = new HighlyAvailableGraphDatabaseFactory();
        private StoreDirInitializer initializer;
        private Listener<GraphDatabaseService> initialDatasetCreator;
        private List<Predicate<ManagedCluster>> availabilityChecks = Collections.emptyList();

        public Builder( File root )
        {
            this.root = root;
        }

        public Builder()
        {
            // We want this, at least in the ClusterRule case where we fill this Builder instances
            // with all our behavior, but we don't know about the root directory until we evaluate the rule.
        }

        @Override
        public Builder withRootDirectory( File root )
        {
            this.root = root;
            return this;
        }

        @Override
        public Builder withSeedDir( final File seedDir )
        {
            return withStoreDirInitializer( ( serverId, storeDir ) -> copyRecursively( seedDir, storeDir ) );
        }

        @Override
        public Builder withStoreDirInitializer( StoreDirInitializer initializer )
        {
            this.initializer = initializer;
            return this;
        }

        @Override
        public Builder withDbFactory( HighlyAvailableGraphDatabaseFactory dbFactory )
        {
            this.factory = dbFactory;
            return this;
        }

        @Override
        public Builder withCluster( Supplier<Cluster> provider )
        {
            this.provider = provider;
            return this;
        }

        @Override
        public Builder withInstanceConfig( Map<String,IntFunction<String>> commonConfig )
        {
            this.commonConfig.putAll( commonConfig );
            return this;
        }

        @Override
        public Builder withInstanceSetting( Setting<?> setting, IntFunction<String> valueFunction )
        {
            this.commonConfig.put( setting.name(), valueFunction );
            return this;
        }

        @Override
        public Builder withSharedConfig( Map<String,String> commonConfig )
        {
            Map<String,IntFunction<String>> dynamic = new HashMap<>();
            for ( Map.Entry<String,String> entry : commonConfig.entrySet() )
            {
                dynamic.put( entry.getKey(), constant( entry.getValue() ) );
            }
            return withInstanceConfig( dynamic );
        }

        @Override
        public Builder withSharedSetting( Setting<?> setting, String value )
        {
            return withInstanceSetting( setting, constant( value ) );
        }

        @Override
        public Builder withInitialDataset( Listener<GraphDatabaseService> transactor )
        {
            this.initialDatasetCreator = transactor;
            return this;
        }

        @Override
        @SafeVarargs
        public final Builder withAvailabilityChecks( Predicate<ManagedCluster>... checks )
        {
            this.availabilityChecks = Arrays.asList( checks );
            return this;
        }

        public ClusterManager build()
        {
            if ( provider == null )
            {
                provider = clusterOfSize( 3 );
            }
            return new ClusterManager( this );
        }
    }

    private static final class HighlyAvailableGraphDatabaseProxy
    {
        private final ExecutorService executor;
        private GraphDatabaseService result;
        private final Future<GraphDatabaseService> untilThen;

        public HighlyAvailableGraphDatabaseProxy( final GraphDatabaseBuilder graphDatabaseBuilder )
        {
            Callable<GraphDatabaseService> starter = graphDatabaseBuilder::newGraphDatabase;
            executor = Executors.newFixedThreadPool( 1 );
            untilThen = executor.submit( starter );
        }

        public HighlyAvailableGraphDatabase get( long timeoutSeconds )
        {
            if ( result == null )
            {
                try
                {
                    result = untilThen.get( timeoutSeconds, TimeUnit.SECONDS );
                }
                catch ( InterruptedException | ExecutionException | TimeoutException e )
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
            currentFuture = starter.submit( (Callable<Void>) () -> {
                try
                {
                    wrapped.init();
                }
                catch ( Throwable throwable )
                {
                    throw new RuntimeException( throwable );
                }
                return null;
            } );
        }

        @Override
        public void start() throws Throwable
        {
            currentFuture.get();
            currentFuture = starter.submit( (Callable<Void>) () -> {
                try
                {
                    wrapped.start();
                }
                catch ( Throwable throwable )
                {
                    throw new RuntimeException( throwable );
                }
                return null;
            } );
        }

        @Override
        public void stop() throws Throwable
        {
            currentFuture.get();
            currentFuture = starter.submit( (Callable<Void>) () -> {
                try
                {
                    wrapped.stop();
                }
                catch ( Throwable throwable )
                {
                    throw new RuntimeException( throwable );
                }
                return null;
            } );
        }

        @Override
        public void shutdown() throws Throwable
        {
            currentFuture = starter.submit( (Callable<Void>) () -> {
                try
                {
                    wrapped.shutdown();
                }
                catch ( Throwable throwable )
                {
                    throw new RuntimeException( throwable );
                }
                return null;
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
        private final Cluster spec;
        private final String name;
        private final Map<InstanceId,HighlyAvailableGraphDatabaseProxy> members = new ConcurrentHashMap<>();
        private final List<ObservedClusterMembers> arbiters = new ArrayList<>();
        private final Set<RepairKit> pendingRepairs = Collections.synchronizedSet( new HashSet<RepairKit>() );
        private final HashSet<Integer> takenHaPorts = new HashSet<>();

        ManagedCluster( Cluster spec ) throws URISyntaxException, IOException
        {
            this.spec = spec;
            this.name = spec.getName();
            for ( int i = 0; i < spec.getMembers().size(); i++ )
            {
                startMember( new InstanceId( i + 1 ) );
            }
            for ( HighlyAvailableGraphDatabaseProxy member : members.values() )
            {
                HighlyAvailableGraphDatabase graphDatabase = member.get( DEFAULT_TIMEOUT_SECONDS );
                Config config = graphDatabase.getDependencyResolver().resolveDependency( Config.class );
            }
        }

        public String getInitialHostsConfigString()
        {
            StringBuilder result = new StringBuilder();
            for ( HighlyAvailableGraphDatabase member : getAllMembers() )
            {
                result.append( result.length() > 0 ? "," : "" )
                      .append( localAddress ).append( ":" )
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
                member.get( DEFAULT_TIMEOUT_SECONDS ).shutdown();
            }
        }

        /**
         * @return all started members in this cluster.
         */
        public Iterable<HighlyAvailableGraphDatabase> getAllMembers( HighlyAvailableGraphDatabase... except )
        {
            Set<HighlyAvailableGraphDatabase> exceptSet = new HashSet<>( asList( except ) );

            return members.values().stream().map( proxy -> proxy.get( DEFAULT_TIMEOUT_SECONDS ) )
                    .filter( db -> !exceptSet.contains( db ) ).collect( Collectors.toList());
        }

        public Iterable<ObservedClusterMembers> getArbiters()
        {
            return arbiters;
        }

        public boolean isArbiter( ClusterMember clusterMember )
        {
            for ( ObservedClusterMembers arbiter : arbiters )
            {
                if ( arbiter.getCurrentMember().getInstanceId().equals( clusterMember.getInstanceId() ) )
                {
                    return true;
                }
            }
            return false;
        }

        public boolean isAvailable( ClusterMember clusterMember )
        {
            if ( isArbiter( clusterMember ) )
            {
                return clusterMember.isAlive();
            }
            else
            {
                return clusterMember.isAlive() && !clusterMember.getHARole().equals(
                        HighAvailabilityModeSwitcher.UNKNOWN );
            }
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
            HighlyAvailableGraphDatabase db = members.get( serverId ).get( DEFAULT_TIMEOUT_SECONDS );
            if ( db == null )
            {
                throw new IllegalStateException( "Db " + serverId + " not found at the moment in " + name +
                                                 stateToString( this ) );
            }
            return db;
        }

        /**
         * Shuts down a member of this cluster. A {@link RepairKit} is returned
         * which is able to restore the instance (i.e. start it again). This method
         * does not return until the rest of the cluster sees the member as not available.
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
            await( entireClusterSeesMemberAsNotAvailable( db ) );
            return wrap( new StartDatabaseAgainKit( this, serverId ) );
        }

        private void assertMember( HighlyAvailableGraphDatabase db )
        {
            for ( HighlyAvailableGraphDatabaseProxy highlyAvailableGraphDatabaseProxy : members.values() )
            {
                if ( highlyAvailableGraphDatabaseProxy.get( DEFAULT_TIMEOUT_SECONDS ).equals( db ) )
                {
                    return;
                }
            }
            throw new IllegalArgumentException( "Db " + db + " not a member of this cluster " + name +
                                                stateToString( this ) );
        }

        /**
         * Fails a member of this cluster by making it not receive/respond to heartbeats.
         * A {@link RepairKit} is returned which is able to repair the instance  (i.e start the network) again. This
         * method does not return until the rest of the cluster sees the member as not available.
         *
         * @param db the {@link HighlyAvailableGraphDatabase} to fail.
         * @return a {@link RepairKit} which can repair the failure.
         */
        public RepairKit fail( HighlyAvailableGraphDatabase db ) throws Throwable
        {
            return fail( db, NetworkFlag.values() );
        }

        /**
         * Fails a member of this cluster by making it either not respond to heartbeats, or not receive them, or both.
         * A {@link RepairKit} is returned which is able to repair the instance  (i.e start the network) again.
         * This method does not return until the rest of the cluster sees the member as not available.
         *
         * @param db the {@link HighlyAvailableGraphDatabase} to fail.
         * @param flags which part of networking to fail (IN/OUT/BOTH)
         * @return a {@link RepairKit} which can repair the failure.
         * @throws IllegalArgumentException if the given db isn't a member of this cluster.
         */
        public RepairKit fail( HighlyAvailableGraphDatabase db, NetworkFlag... flags ) throws Throwable
        {
            return fail( db, true, flags );
        }

        /**
         * WARNING: beware of hacks.
         * <p>
         * Fails a member of this cluster by making it either not respond to heartbeats, or not receive them, or both.
         * A {@link RepairKit} is returned which is able to repair the instance  (i.e start the network) again.
         * This method optionally does not return until the rest of the cluster sees the member as not available.
         *
         * @param db the {@link HighlyAvailableGraphDatabase} to fail.
         * @param waitUntilDown if true, will not return until rest of cluster reports member as not available
         * @param flags which part of networking to fail (IN/OUT)
         * @return a {@link RepairKit} which can repair the failure.
         * @throws IllegalArgumentException if the given db isn't a member of this cluster.
         */
        public RepairKit fail( HighlyAvailableGraphDatabase db, boolean waitUntilDown, NetworkFlag... flags )
                throws Throwable
        {
            assertMember( db );

            NetworkReceiver networkReceiver = db.getDependencyResolver().resolveDependency( NetworkReceiver.class );
            NetworkSender networkSender = db.getDependencyResolver().resolveDependency( NetworkSender.class );

            if ( contains( flags, NetworkFlag.IN ) )
            {
                networkReceiver.setPaused( true );
            }
            if ( contains( flags, NetworkFlag.OUT ) )
            {
                networkSender.setPaused( true );
            }

            if ( waitUntilDown )
            {
                await( entireClusterSeesMemberAsNotAvailable( db ) );
            }
            return wrap( new StartNetworkAgainKit( db, networkReceiver, networkSender, flags ) );
        }

        private RepairKit wrap( final RepairKit actual )
        {
            pendingRepairs.add( actual );
            return () -> {
                try
                {
                    return actual.repair();
                }
                finally
                {
                    pendingRepairs.remove( actual );
                }
            };
        }

        private void startMember( InstanceId serverId ) throws URISyntaxException, IOException
        {
            Cluster.Member member = spec.getMembers().get( serverId.toIntegerIndex() - 1 );
            StringBuilder initialHosts = new StringBuilder();
            for ( int i = 0; i < spec.getMembers().size(); i++ )
            {
                if ( i > 0 )
                {
                    initialHosts.append( "," );
                }
                // the host might be 0.0.0.0:PORT, or :PORT, if so, replace with a valid address.
                URI uri = new URI( "cluster://" + spec.getMembers().get( i ).getHost() );
                if ( uri.getHost() == null || uri.getHost().isEmpty() || uri.getHost().equals("0.0.0.0") )
                {
                    initialHosts.append( localAddress ).append( ":" ).append( uri.getPort() );
                }
                else
                {
                    initialHosts.append( uri.getHost() ).append( ":" ).append( uri.getPort() );
                }
            }
            File parent = new File( root, name );
            URI clusterUri = new URI( "cluster://" + member.getHost() );
            if ( member.isFullHaMember() )
            {
                int clusterPort = clusterUri.getPort();
                int haPort = findFreePort( HA_MIN_PORT, HA_MAX_PORT, takenHaPorts );
                takenHaPorts.add( haPort );
                File storeDir = new File( parent, "server" + serverId );
                if ( storeDirInitializer != null )
                {
                    storeDirInitializer.initializeStoreDir( serverId.toIntegerIndex(), storeDir );
                }
                GraphDatabaseBuilder builder = dbFactory.newEmbeddedDatabaseBuilder( storeDir.getAbsoluteFile() );
                builder.setConfig( ClusterSettings.cluster_name, name );
                builder.setConfig( ClusterSettings.initial_hosts, initialHosts.toString() );
                builder.setConfig( ClusterSettings.server_id, serverId + "" );
                builder.setConfig( ClusterSettings.cluster_server, "0.0.0.0:" + clusterPort );
                builder.setConfig( HaSettings.ha_server, clusterUri.getHost() + ":" + haPort );
                builder.setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE );
                for ( Map.Entry<String,IntFunction<String>> conf : commonConfig.entrySet() )
                {
                    builder.setConfig( conf.getKey(), conf.getValue().apply( serverId.toIntegerIndex() ) );
                }

                final HighlyAvailableGraphDatabaseProxy graphDatabase =
                        new HighlyAvailableGraphDatabaseProxy( builder );

                members.put( serverId, graphDatabase );

                life.add( new LifecycleAdapter()
                {
                    @Override
                    public void stop() throws Throwable
                    {
                        graphDatabase.get( DEFAULT_TIMEOUT_SECONDS ).shutdown();
                    }
                } );
            }
            else
            {
                Map<String,String> config = MapUtil.stringMap(
                        ClusterSettings.cluster_name.name(), name,
                        ClusterSettings.initial_hosts.name(), initialHosts.toString(),
                        ClusterSettings.server_id.name(), serverId + "",
                        ClusterSettings.cluster_server.name(), "0.0.0.0:" + clusterUri.getPort() );
                Config config1 = new Config( config, GraphDatabaseFacadeFactory.Configuration.class,
                        GraphDatabaseSettings.class );

                LifeSupport clusterClientLife = new LifeSupport();
                NullLogService logService = NullLogService.getInstance();
                ClusterClientModule clusterClientModule = new ClusterClientModule( clusterClientLife,
                        new Dependencies(), new Monitors(), config1, logService,
                        new NotElectableElectionCredentialsProvider() );

                arbiters.add( new ObservedClusterMembers(  logService.getInternalLogProvider(),
                        clusterClientModule.clusterClient, clusterClientModule.clusterClient, new ClusterMemberEvents()
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
                }, clusterClientModule.clusterClient.getServerId() ) );

                life.add( new FutureLifecycleAdapter<>( clusterClientLife ) );
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
                if ( predicate.test( this ) )
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
            return member.getDependencyResolver().resolveDependency( Config.class ).get( ClusterSettings.server_id );
        }

        public File getStoreDir( HighlyAvailableGraphDatabase member )
        {
            assertMember( member );
            return member.getStoreDirectory();
        }

        public void sync( HighlyAvailableGraphDatabase... except ) throws InterruptedException
        {
            Set<HighlyAvailableGraphDatabase> exceptSet = new HashSet<>( asList( except ) );
            for ( HighlyAvailableGraphDatabase db : getAllMembers() )
            {
                if ( !exceptSet.contains( db ) )
                {
                    UpdatePuller updatePuller = db.getDependencyResolver().resolveDependency( UpdatePuller.class );
                    try
                    {
                        if ( db.isAvailable( 60000 ) ) // wait for 1 min for db to become available
                        {
                            updatePuller.pullUpdates();
                        }
                    }
                    catch ( Exception e )
                    {
                        throw new IllegalStateException( stateToString( this ), e );
                    }
                }
            }
        }

        public void force( HighlyAvailableGraphDatabase... except )
        {
            Set<HighlyAvailableGraphDatabase> exceptSet = new HashSet<>( asList( except ) );
            for ( HighlyAvailableGraphDatabase db : getAllMembers() )
            {
                if ( !exceptSet.contains( db ) )
                {
                    IOLimiter limiter = IOLimiter.unlimited();
                    db.getDependencyResolver().resolveDependency( StorageEngine.class ).flushAndForce( limiter );
                }
            }
        }

        public void info( String message )
        {
            for ( HighlyAvailableGraphDatabase db : getAllMembers() )
            {
                LogService logService = db.getDependencyResolver().resolveDependency( LogService.class );
                Log messagesLog = logService.getInternalLog( HighlyAvailableGraphDatabase.class );
                messagesLog.info( message );
            }
        }

        public void applyOnAll( Function<GraphDatabaseService,Void> function )
        {
            for ( HighlyAvailableGraphDatabase db : getAllMembers() )
            {
                function.apply( db );
            }
        }

        /**
         * Repairs all {@link RepairKit} that haven't already been repaired.
         *
         * @throws Throwable if any repair throws.
         */
        public void repairAll() throws Throwable
        {
            for ( RepairKit repair : pendingRepairs )
            {
                repair.repair();
            }
            pendingRepairs.clear();
        }
    }

    private class StartNetworkAgainKit implements RepairKit
    {
        private final HighlyAvailableGraphDatabase db;
        private final NetworkReceiver networkReceiver;
        private final NetworkSender networkSender;
        private final NetworkFlag[] flags;

        StartNetworkAgainKit( HighlyAvailableGraphDatabase db,
                NetworkReceiver networkReceiver,
                NetworkSender networkSender,
                NetworkFlag... flags )
        {
            this.db = db;
            this.networkReceiver = networkReceiver;
            this.networkSender = networkSender;
            this.flags = flags;
        }

        @Override
        public HighlyAvailableGraphDatabase repair() throws Throwable
        {
            if ( contains( flags, NetworkFlag.OUT ) )
            {
                networkSender.setPaused( false );
            }
            if ( contains( flags, NetworkFlag.IN ) )
            {
                networkReceiver.setPaused( false );
            }

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
