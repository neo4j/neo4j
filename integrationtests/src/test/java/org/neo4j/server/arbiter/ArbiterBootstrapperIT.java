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
package org.neo4j.server.arbiter;

import org.apache.commons.lang3.SystemUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.AccessibleObject;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Resource;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.cluster.client.ClusterClientModule;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.cluster.protocol.cluster.ClusterListener.Adapter;
import org.neo4j.cluster.protocol.election.ServerIdElectionCredentialsProvider;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.server.ServerCommandLineArgs;
import org.neo4j.test.InputStreamAwaiter;
import org.neo4j.test.ProcessStreamHandler;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.Runtime.getRuntime;
import static java.lang.String.format;
import static java.lang.System.getProperty;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.cluster.ClusterSettings.cluster_server;
import static org.neo4j.cluster.ClusterSettings.initial_hosts;
import static org.neo4j.cluster.ClusterSettings.server_id;
import static org.neo4j.helpers.collection.MapUtil.store;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.test.StreamConsumer.IGNORE_FAILURES;

@ExtendWith( {SuppressOutputExtension.class, TestDirectoryExtension.class} )
public class ArbiterBootstrapperIT
{
    @Resource
    public SuppressOutput suppressOutput;
    @Resource
    public TestDirectory testDirectory;

    private static Integer SHOULD_NOT_JOIN;
    private File directory;
    private LifeSupport life;
    private ClusterClient[] clients;

    @BeforeEach
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
                    new Monitors(), Config.defaults( config ), NullLogService.getInstance(),
                    new ServerIdElectionCredentialsProvider() );

            ClusterClient client = clusterClientModule.clusterClient;
            CountDownLatch latch = new CountDownLatch( 1 );
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
            assertTrue( latch.await( 20, SECONDS ), "Didn't join the cluster" );
        }
    }

    @AfterEach
    public void after()
    {
        life.shutdown();
    }

    @Test
    public void canJoinWithExplicitInitialHosts() throws Exception
    {
        startAndAssertJoined( 5003,
                stringMap(
                        initial_hosts.name(), ":5001",
                        server_id.name(), "3" )
        );
    }

    @Test
    @DisabledOnOs( OS.WINDOWS )
    public void willFailJoinIfIncorrectInitialHostsSet() throws Exception
    {
        startAndAssertJoined( SHOULD_NOT_JOIN,
                stringMap(
                        initial_hosts.name(), ":5011",
                        server_id.name(), "3" )
        );
    }

    @Test
    public void canSetSpecificPort() throws Exception
    {
        startAndAssertJoined( 5010,
                stringMap(
                        initial_hosts.name(), ":5001",
                        server_id.name(), "3",
                        cluster_server.name(), ":5010" )
        );
    }

    @Test
    public void usesPortRange() throws Exception
    {
        startAndAssertJoined( 5012,
                stringMap(
                        initial_hosts.name(), ":5001",
                        cluster_server.name(), ":5012-5020",
                        server_id.name(), "3" )
        );
    }

    private File writeConfig( Map<String, String> config ) throws IOException
    {
        config.put( GraphDatabaseSettings.logs_directory.name(), directory.getPath() );
        File configFile = new File( directory, Config.DEFAULT_CONFIG_FILE_NAME );
        store( config, configFile );
        return directory;
    }

    private void startAndAssertJoined( Integer expectedAssignedPort, Map<String, String> config ) throws Exception
    {
        File configDir = writeConfig( config );
        CountDownLatch latch = new CountDownLatch( 1 );
        AtomicInteger port = new AtomicInteger();
        clients[0].addClusterListener( joinAwaitingListener( latch, port ) );

        boolean arbiterStarted = startArbiter( configDir, latch );
        if ( expectedAssignedPort == null )
        {
            assertFalse( arbiterStarted, format( "Should not be able to start arbiter given config file:%s", config ) );
        }
        else
        {
            assertTrue( arbiterStarted, format( "Should be able to start arbiter given config file:%s", config ) );
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

    private boolean startArbiter( File configDir, CountDownLatch latch ) throws Exception
    {
        Process process = null;
        ProcessStreamHandler handler = null;
        try
        {
            process = startArbiterProcess( configDir );
            new InputStreamAwaiter( process.getInputStream() ).awaitLine( ArbiterBootstrapperTestProxy.START_SIGNAL, 20, SECONDS );
            handler = new ProcessStreamHandler( process, false, "", IGNORE_FAILURES );
            handler.launch();

            // Latch is triggered when the arbiter we just spawned joins the cluster,
            // or rather when the first client sees it as joined. If the latch awaiting times out it
            // (most likely) means that the arbiter couldn't be started. The reason for not
            // being able to start is assumed in this test to be that the specified port already is in use.
            return latch.await( 10, SECONDS );
        }
        finally
        {
            if ( process != null )
            {
                // Tell it to leave the cluster and shut down now
                try ( OutputStream inputToOtherProcess = process.getOutputStream() )
                {
                    inputToOtherProcess.write( 0 );
                    inputToOtherProcess.flush();
                }
                if ( !process.waitFor( 10, SECONDS ) )
                {
                    kill( process );
                }
            }
            if ( handler != null )
            {
                handler.done();
            }
        }
    }

    private Process startArbiterProcess( File configDir ) throws Exception
    {
        List<String> args = new ArrayList<>( asList( "java", "-cp", getProperty( "java.class.path" ) ) );
        args.add( ArbiterBootstrapperTestProxy.class.getName() );
        if ( configDir != null )
        {
            args.add( String.format( "--%s=%s", ServerCommandLineArgs.CONFIG_DIR_ARG, configDir ) );
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
