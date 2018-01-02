/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.enterprise;

import org.apache.commons.lang3.SystemUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.AccessibleObject;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.cluster.client.ClusterClientModule;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.cluster.protocol.cluster.ClusterListener.Adapter;
import org.neo4j.cluster.protocol.election.ServerIdElectionCredentialsProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.enterprise.functional.DumpPortListenerOnNettyBindFailure;
import org.neo4j.test.InputStreamAwaiter;
import org.neo4j.test.ProcessStreamHandler;
import org.neo4j.test.TargetDirectory;

import static java.lang.Runtime.getRuntime;
import static java.lang.String.format;
import static java.lang.System.getProperty;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.neo4j.cluster.ClusterSettings.cluster_server;
import static org.neo4j.cluster.ClusterSettings.initial_hosts;
import static org.neo4j.cluster.ClusterSettings.server_id;
import static org.neo4j.helpers.collection.MapUtil.store;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.server.enterprise.StandaloneClusterClientTestProxy.START_SIGNAL;
import static org.neo4j.test.StreamConsumer.IGNORE_FAILURES;

public class StandaloneClusterClientIT
{
    @Test
    public void canJoinWithExplicitInitialHosts() throws Exception
    {
        startAndAssertJoined( 5003,
                // Config file
                stringMap(),
                // Arguments
                stringMap(
                        initial_hosts.name(), ":5001",
                        server_id.name(), "3" )
        );
    }

    @Test
    public void willFailJoinIfIncorrectInitialHostsSet() throws Exception
    {
        assumeFalse( "Cannot kill processes on windows.", SystemUtils.IS_OS_WINDOWS );
        startAndAssertJoined( SHOULD_NOT_JOIN,
                // Config file
                stringMap(),
                // Arguments
                stringMap(
                        initial_hosts.name(), ":5011",
                        server_id.name(), "3" )
        );
    }

    @Test
    public void canJoinWithInitialHostsInConfigFile() throws Exception
    {
        startAndAssertJoined( 5003,
                // Config file
                stringMap( initial_hosts.name(), ":5001" ),
                // Arguments
                stringMap( server_id.name(), "3" ) );
    }

    @Test
    public void willFailJoinIfIncorrectInitialHostsSetInConfigFile() throws Exception
    {
        assumeFalse( "Cannot kill processes on windows.", SystemUtils.IS_OS_WINDOWS );
        startAndAssertJoined( SHOULD_NOT_JOIN,
                // Config file
                stringMap( initial_hosts.name(), ":5011" ),
                // Arguments
                stringMap( server_id.name(), "3" ) );
    }

    @Test
    public void canOverrideInitialHostsConfigFromConfigFile() throws Exception
    {
        startAndAssertJoined( 5003,
                // Config file
                stringMap( initial_hosts.name(), ":5011" ),
                // Arguments
                stringMap(
                        initial_hosts.name(), ":5001",
                        server_id.name(), "3" )
        );
    }

    @Test
    public void canSetSpecificPort() throws Exception
    {
        startAndAssertJoined( 5010,
                // Config file
                stringMap(),
                // Arguments
                stringMap(
                        initial_hosts.name(), ":5001",
                        server_id.name(), "3",
                        cluster_server.name(), ":5010" )
        );
    }

    @Test
    public void usesPortRangeFromConfigFile() throws Exception
    {
        startAndAssertJoined( 5012,
                // Config file
                stringMap(
                        initial_hosts.name(), ":5001",
                        cluster_server.name(), ":5012-5020" ),
                // Arguments
                stringMap( server_id.name(), "3" )
        );
    }

    // === Everything else ===

    private static Integer SHOULD_NOT_JOIN = null;

    @Rule
    public TestRule dumpPorts = new DumpPortListenerOnNettyBindFailure();
    @Rule
    public TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( getClass() );

    private File directory;
    private LifeSupport life;
    private ClusterClient[] clients;

    @Before
    public void before() throws Exception
    {
        directory = testDirectory.directory( "temp" );
        life = new LifeSupport();
        life.start(); // So that the clients get started as they are added
        clients = new ClusterClient[2];
        for ( int i = 1; i <= clients.length; i++ )
        {
            Map<String, String> config = stringMap();
            config.put( cluster_server.name(), ":" + (5000 + i) );
            config.put( server_id.name(), "" + i );
            config.put( initial_hosts.name(), ":5001" );

            LifeSupport moduleLife = new LifeSupport();
            ClusterClientModule clusterClientModule = new ClusterClientModule( moduleLife, new Dependencies(),
                    new Monitors(), new Config(config),  NullLogService.getInstance(),
                    new ServerIdElectionCredentialsProvider() );

            final ClusterClient client = clusterClientModule.clusterClient;
            final CountDownLatch latch = new CountDownLatch( 1 );
            client.addClusterListener( new ClusterListener.Adapter()
            {
                @Override
                public void enteredCluster( ClusterConfiguration configuration )
                {
                    latch.countDown();
                    client.removeClusterListener( this );
                }
            } );
            life.add( moduleLife );
            clients[i - 1] = client;
            assertTrue( "Didn't join the cluster", latch.await( 20, SECONDS ) );
        }
    }

    @After
    public void after() throws Exception
    {
        life.shutdown();
    }

    private File configFile( Map<String, String> config ) throws IOException
    {
        File dbConfigFile = new File( directory, "config-file" );
        store( config, dbConfigFile );
        File serverConfigFile = new File( directory, "server-file" );
        store( stringMap( Configurator.DB_TUNING_PROPERTY_FILE_KEY, dbConfigFile.getAbsolutePath() ),
                serverConfigFile );
        return serverConfigFile;
    }

    private void startAndAssertJoined( Integer expectedAssignedPort, Map<String, String> configInConfigFile,
                                       Map<String, String> config ) throws Exception
    {
        File configFile = configFile( configInConfigFile );
        CountDownLatch latch = new CountDownLatch( 1 );
        AtomicInteger port = new AtomicInteger();
        clients[0].addClusterListener( joinAwaitingListener( latch, port ) );

        boolean clientStarted = startStandaloneClusterClient( configFile, config, latch );
        if ( expectedAssignedPort == null )
        {
            assertFalse( format( "Should not be able to start cluster client given config file:%s " +
                    "and arguments:%s", configInConfigFile, config ), clientStarted );
        }
        else
        {
            assertTrue( format( "Should be able to start client client given config file:%s " +
                    "and arguments:%s", configInConfigFile, config ), clientStarted );
            assertEquals( expectedAssignedPort.intValue(), port.get() );
        }
    }

    private Adapter joinAwaitingListener( final CountDownLatch latch, final AtomicInteger port )
    {
        return new ClusterListener.Adapter()
        {
            @Override
            public void joinedCluster( InstanceId member, URI memberUri )
            {
                port.set( memberUri.getPort() );
                latch.countDown();
                clients[0].removeClusterListener( this );
            }
        };
    }

    private boolean startStandaloneClusterClient( File configFile, Map<String, String> config, CountDownLatch latch )
            throws Exception
    {
        Process process = null;
        ProcessStreamHandler handler = null;
        try
        {
            process = startStandaloneClusterClientProcess( configFile, config );
            new InputStreamAwaiter( process.getInputStream() ).awaitLine( START_SIGNAL, 20, SECONDS );
            handler = new ProcessStreamHandler( process, false, "", IGNORE_FAILURES );
            handler.launch();

            // Latch is triggered when this cluster client we just spawned joins the cluster,
            // or rather when the first client sees it as joined. If the latch awaiting times out it
            // (most likely) means that this cluster client couldn't be started. The reason for not
            // being able to start is assumed in this test to be that the specified port already is in use.
            return latch.await( 10, SECONDS );
        }
        finally
        {
            if ( process != null )
            {
                kill( process );
                process.waitFor();
            }
            if ( handler != null )
            {
                handler.done();
            }
        }
    }

    private Process startStandaloneClusterClientProcess( File configFile, Map<String, String> config )
            throws Exception
    {
        List<String> args = new ArrayList<String>( asList( "java", "-cp", getProperty( "java.class.path" ),
                "-Dneo4j.home=" + directory.getAbsolutePath() ) );
        if ( configFile != null )
        {
            args.add( "-D" + Configurator.NEO_SERVER_CONFIG_FILE_KEY + "=" + configFile.getAbsolutePath() );
        }
        args.add( StandaloneClusterClientTestProxy.class.getName() );

        for ( Map.Entry<String, String> entry : config.entrySet() )
        {
            args.add( "-" + entry.getKey() + "=" + entry.getValue() );
        }
        return getRuntime().exec( args.toArray( new String[args.size()] ) );
    }

    private static void kill( Process process )
            throws NoSuchFieldException, IllegalAccessException, IOException, InterruptedException
    {
        if ( SystemUtils.IS_OS_WINDOWS )
        {
            process.destroy();
        }
        else
        {
            int pid = ((Number) accessible( process.getClass().getDeclaredField( "pid" ) ).get( process )).intValue();
            new ProcessBuilder( "kill", "-9", "" + pid ).start().waitFor();
        }
    }

    private static <T extends AccessibleObject> T accessible( T obj )
    {
        obj.setAccessible( true );
        return obj;
    }
}
