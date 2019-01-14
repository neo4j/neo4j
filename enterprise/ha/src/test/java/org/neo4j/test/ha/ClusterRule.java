/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.test.ha;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.neo4j.cluster.client.Cluster;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.kernel.impl.ha.ClusterManager.Builder;
import org.neo4j.kernel.impl.ha.ClusterManager.ClusterBuilder;
import org.neo4j.kernel.impl.ha.ClusterManager.ManagedCluster;
import org.neo4j.kernel.impl.ha.ClusterManager.StoreDirInitializer;
import org.neo4j.kernel.impl.util.Listener;
import org.neo4j.test.rule.TestDirectory;

import static org.neo4j.cluster.ClusterSettings.default_timeout;
import static org.neo4j.cluster.ClusterSettings.join_timeout;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.store_internal_log_level;
import static org.neo4j.kernel.ha.HaSettings.tx_push_factor;
import static org.neo4j.kernel.impl.ha.ClusterManager.allSeesAllAsAvailable;

/**
 * Starts, manages and in the end shuts down an HA cluster as a JUnit {@code Rule} or {@link ClassRule}.
 * Basically this is {@link ClusterManager} in a JUnit {@link Rule} packaging.
 */
public class ClusterRule extends ExternalResource implements ClusterBuilder<ClusterRule>
{
    private static final StoreDirInitializer defaultStoreDirInitializer =
            ( serverId, storeDir ) ->
            {
                File[] files = storeDir.listFiles();
                if ( files != null )
                {
                    for ( File file : files )
                    {
                        FileUtils.deleteRecursively( file );
                    }
                }
            };

    private ClusterManager.Builder clusterManagerBuilder;
    private ClusterManager clusterManager;
    private File storeDirectory;

    private final TestDirectory testDirectory;
    private ManagedCluster cluster;

    public ClusterRule()
    {
        this.testDirectory = TestDirectory.testDirectory();
        this.clusterManagerBuilder = new ClusterManager.Builder()
                .withSharedSetting( store_internal_log_level, "DEBUG" )
                .withSharedSetting( default_timeout, "1s" )
                .withSharedSetting( tx_push_factor, "0" )
                .withSharedSetting( pagecache_memory, "8m" )
                .withSharedSetting( join_timeout, "60s" )
                .withAvailabilityChecks( allSeesAllAsAvailable() )
                .withStoreDirInitializer( defaultStoreDirInitializer );
    }

    @Override
    public ClusterRule withRootDirectory( File root )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClusterRule withSeedDir( final File seedDir )
    {
        return set( clusterManagerBuilder.withSeedDir( seedDir ) );
    }

    @Override
    public ClusterRule withStoreDirInitializer( StoreDirInitializer initializer )
    {
        return set( clusterManagerBuilder.withStoreDirInitializer( initializer ) );
    }

    @Override
    public ClusterRule withDbFactory( HighlyAvailableGraphDatabaseFactory dbFactory )
    {
        return set( clusterManagerBuilder.withDbFactory( dbFactory ) );
    }

    @Override
    public ClusterRule withCluster( Supplier<Cluster> supplier )
    {
        return set( clusterManagerBuilder.withCluster( supplier ) );
    }

    @Override
    public ClusterRule withInstanceConfig( Map<String,IntFunction<String>> commonConfig )
    {
        return set( clusterManagerBuilder.withInstanceConfig( commonConfig ) );
    }

    @Override
    public ClusterRule withBoltEnabled()
    {
        return set( clusterManagerBuilder.withBoltEnabled() );
    }

    @Override
    public ClusterRule withInstanceSetting( Setting<?> setting, IntFunction<String> valueFunction )
    {
        return set( clusterManagerBuilder.withInstanceSetting( setting, valueFunction ) );
    }

    @Override
    public ClusterRule withSharedConfig( Map<String,String> commonConfig )
    {
        return set( clusterManagerBuilder.withSharedConfig( commonConfig ) );
    }

    @Override
    public ClusterRule withSharedSetting( Setting<?> setting, String value )
    {
        return set( clusterManagerBuilder.withSharedSetting( setting, value ) );
    }

    @Override
    public ClusterRule withInitialDataset( Listener<GraphDatabaseService> transactor )
    {
        return set( clusterManagerBuilder.withInitialDataset( transactor ) );
    }

    @SafeVarargs
    @Override
    public final ClusterRule withAvailabilityChecks( Predicate<ManagedCluster>... checks )
    {
        return set( clusterManagerBuilder.withAvailabilityChecks( checks ) );
    }

    @Override
    public final ClusterRule withConsistencyCheckAfterwards()
    {
        return set( clusterManagerBuilder.withConsistencyCheckAfterwards() );
    }

    @Override
    public ClusterRule withFirstInstanceId( int firstInstanceId )
    {
        return set( clusterManagerBuilder.withFirstInstanceId( firstInstanceId ) );
    }

    private ClusterRule set( Builder builder )
    {
        clusterManagerBuilder = builder;
        return this;
    }

    /**
     * Starts cluster with the configuration provided at instantiation time. This method will not return until the
     * cluster is up and all members report each other as available.
     */
    public ClusterManager.ManagedCluster startCluster()
    {
        if ( cluster == null )
        {
            if ( clusterManager == null )
            {
                clusterManager = clusterManagerBuilder.withRootDirectory( storeDirectory ).build();
            }

            try
            {
                clusterManager.start();
            }
            catch ( Throwable throwable )
            {
                throw new RuntimeException( throwable );
            }
            cluster = clusterManager.getCluster();
        }
        cluster.await( allSeesAllAsAvailable() );
        return cluster;
    }

    @Override
    public Statement apply( final Statement base, final Description description )
    {
        Statement testMethod = new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                // If this is used as class rule then getMethodName() returns null, so use
                // getClassName() instead.
                String name = description.getMethodName() != null ?
                        description.getMethodName() : description.getClassName();
                storeDirectory = testDirectory.directory( name );
                base.evaluate();
            }
        };

        Statement testMethodWithBeforeAndAfter = super.apply( testMethod, description );

        return testDirectory.apply( testMethodWithBeforeAndAfter, description );
    }

    @Override
    protected void after()
    {
        shutdownCluster();
    }

    public void shutdownCluster()
    {
        if ( clusterManager != null )
        {
            clusterManager.safeShutdown();
            cluster = null;
        }
    }

    public File directory( String name )
    {
        return testDirectory.directory( name );
    }

    public File cleanDirectory( String name ) throws IOException
    {
        return testDirectory.cleanDirectory( name );
    }

    /**
     * Adapter for providing a static config value into a setting where per-instances dynamic config values
     * are supplied.
     *
     * @param value static config value.
     * @return this {@link ClusterRule} instance, for builder convenience.
     */
    public static IntFunction<String> constant( String value )
    {
        return ClusterManager.constant( value );
    }

    /**
     * Dynamic configuration value, of sorts. Can be used as input to {@link #withInstanceConfig(Map)}.
     * Some configuration values are a function of server id of the cluster member and this is a utility
     * for creating such dynamic configuration values.
     *
     * @param oneBasedServerId value onto which one-based server id is added. So for example
     * a value of 10 would have cluster member with server id 2 that config value set to 12.
     */
    public static IntFunction<String> intBase( final int oneBasedServerId )
    {
        return serverId -> String.valueOf( oneBasedServerId + serverId );
    }

    /**
     * Dynamic configuration value, of sorts. Can be used as input to {@link #withInstanceConfig(Map)}.
     * Some configuration values are a function of server id of the cluster member and this is a utility
     * for creating such dynamic configuration values.
     *
     * @param prefix string prefix for these config values.
     * @param oneBasedServerId value onto which one-based server id is added. So for example
     * a value of 10 would have cluster member with server id 2 that config value set to 12.
     * @return a string which has a prefix and an integer part, where the integer part is a function of
     * server id of the cluster member. Can be used to set config values like a host, where arguments could look
     * something like: {@code prefix: "localhost:" oneBasedServerId: 5000}.
     */
    public static IntFunction<String> stringWithIntBase( final String prefix, final int oneBasedServerId )
    {
        return serverId -> prefix + (oneBasedServerId + serverId);
    }
}
