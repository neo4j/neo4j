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
package org.neo4j.server.enterprise;

import static java.lang.Runtime.getRuntime;
import static java.lang.System.getProperty;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.cluster.ClusterSettings.cluster_server;
import static org.neo4j.cluster.ClusterSettings.initial_hosts;
import static org.neo4j.cluster.ClusterSettings.server_id;
import static org.neo4j.cluster.client.ClusterClient.adapt;
import static org.neo4j.helpers.collection.MapUtil.store;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.cluster.protocol.election.ServerIdElectionCredentialsProvider;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.test.ProcessStreamHandler;
import org.neo4j.test.TargetDirectory;

public class StandaloneClusterClientIT
{
    // === The tests ===
    
    @Test
    public void canJoinWithExplicitInitialHosts() throws Exception
    {
        startAndAssertJoined( 5003, null, stringMap( initial_hosts.name(), ":5001", server_id.name(), "3" ) );
    }

    @Test
    @Ignore("Currently will not properly kill the spawned process, makes every other test fail")
    public void willFailJoinIfIncorrectInitialHostsSet() throws Exception
    {
        startAndAssertJoined( null, null, stringMap( initial_hosts.name(), ":5011", server_id.name(), "3" ) );
    }

    @Test
    public void canJoinWithInitialHostsInConfigFile() throws Exception
    {
        startAndAssertJoined( 5003, configFile( initial_hosts.name(), ":5001" ), stringMap(server_id.name(), "3") );
    }

    @Test
    @Ignore("Currently will not properly kill the spawned process, makes every other test fail")
    public void willFailJoinIfIncorrectInitialHostsSetInConfigFile() throws Exception
    {
        startAndAssertJoined( null, configFile( initial_hosts.name(), ":5011" ), stringMap( server_id.name(), "3") );
    }

    @Test
    public void canOverrideInitialHostsConfigFromConfigFile() throws Exception
    {
        startAndAssertJoined( 5003, configFile( initial_hosts.name(), ":5011" ),
                stringMap( initial_hosts.name(), ":5001", server_id.name(), "3" ) );
    }
    
    @Test
    public void canSetSpecificPort() throws Exception
    {
        startAndAssertJoined( 5010, null, stringMap(
                initial_hosts.name(), ":5001",
                server_id.name(), "3",
                cluster_server.name(), ":5010" ) );
    }
    
    @Test
    public void usesPortRangeFromConfigFile() throws Exception
    {
        startAndAssertJoined( 5012, configFile(
                initial_hosts.name(), ":5001",
                cluster_server.name(), ":5012-5020" ), stringMap( server_id.name(), "3" ) );
    }
    
    // === Everything else ===
    
    private File directory = TargetDirectory.forTest( getClass() ).directory( "temp", true );
    private LifeSupport life;
    private ClusterClient[] clients;
    
    @Before
    public void before() throws Exception
    {
        life = new LifeSupport();
        life.start(); // So that the clients get started as they are added
        clients = new ClusterClient[2];
        for ( int i = 1; i <= clients.length; i++ )
        {
            Map<String, String> config = stringMap();
            config.put( cluster_server.name(), ":" + (5000+i) );
            config.put( server_id.name(), "" + i );
            config.put( initial_hosts.name(), ":5001" );
            Logging logging = new Logging()
            {
                @Override
                public StringLogger getLogger( Class loggingClass )
                {
                    return StringLogger.SYSTEM;
                }
            };
            final ClusterClient client = new ClusterClient( adapt( new Config( config ) ), logging, new ServerIdElectionCredentialsProvider() );
            final CountDownLatch latch = new CountDownLatch( 1 );
            client.addClusterListener( new ClusterListener.Adapter()
            {
                @Override
                public void enteredCluster( ClusterConfiguration configuration )
                {
                    latch.countDown();
                    client.removeClusterListener( this );
                };
            } );
            clients[i-1] = life.add( client );
            assertTrue( "Didn't join the cluster", latch.await( 2, SECONDS ) );
        }
    }

    @After
    public void after() throws Exception
    {
        life.shutdown();
    }
    
    private File configFile( Object... settingsAndValues ) throws IOException
    {
        File directory = TargetDirectory.forTest( getClass() ).directory( "temp", true );
        File dbConfigFile = new File( directory, "config-file" );
        store( MapUtil.<String, String>genericMap( settingsAndValues ), dbConfigFile );
        File serverConfigFile = new File( directory, "server-file" );
        store( stringMap( Configurator.DB_TUNING_PROPERTY_FILE_KEY, dbConfigFile.getAbsolutePath() ), serverConfigFile );
        return serverConfigFile;
    }

    private void startAndAssertJoined( Integer expectedPort, File configFile, Map<String, String> config ) throws Exception
    {
        final CountDownLatch latch = new CountDownLatch( 1 );
        final AtomicInteger port = new AtomicInteger();
        clients[0].addClusterListener( new ClusterListener.Adapter()
        {
            @Override
            public void joinedCluster( InstanceId member, URI memberUri )
            {
                port.set( memberUri.getPort() );
                latch.countDown();
                clients[0].removeClusterListener( this );
            }
        } );
        List<String> args = new ArrayList<String>( asList( "java", "-cp", getProperty( "java.class.path" ) ) );
        args.add( "-Dneo4j.home=" + directory.getAbsolutePath() );
        if ( configFile != null )
        {
            args.add( "-D" + Configurator.NEO_SERVER_CONFIG_FILE_KEY + "=" + configFile.getAbsolutePath() );
        }
        args.add( StandaloneClusterClient.class.getName() );

        for ( Map.Entry<String, String> entry : config.entrySet() )
        {
            args.add( "-" + entry.getKey() + "=" + entry.getValue() );
        }
        Process process = null;
        ProcessStreamHandler handler = null;
        try
        {
            process = getRuntime().exec( args.toArray( new String[0] ) );
            handler = new ProcessStreamHandler( process, false );
            handler.launch();
            boolean awaitOutcome = latch.await( 5, SECONDS );
            if ( expectedPort == null )
            {
                assertFalse( awaitOutcome );
            }
            else
            {
                assertTrue( awaitOutcome );
                assertEquals( expectedPort.intValue(), port.get() );
            }
        }
        finally
        {
            process.destroy();
            process.waitFor();
            handler.done();
        }
    }
}
