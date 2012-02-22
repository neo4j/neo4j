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

import java.io.File;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Ignore;
import org.neo4j.com.Client;
import org.neo4j.com.Protocol;
import org.neo4j.helpers.Format;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.ConfigProxy;
import org.neo4j.kernel.HaConfig;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.AbstractBroker;
import org.neo4j.kernel.ha.Broker;
import org.neo4j.kernel.ha.FakeMasterBroker;
import org.neo4j.kernel.ha.FakeSlaveBroker;
import org.neo4j.kernel.ha.MasterClient;
import org.neo4j.kernel.ha.zookeeper.ZooClient;
import org.neo4j.kernel.ha.zookeeper.ZooKeeperException;
import org.neo4j.management.HighAvailability;
import org.neo4j.test.subprocess.SubProcess;

import slavetest.Job;

@Ignore
public class StandaloneDatabase
{
    private final Controller process;

    public static StandaloneDatabase withDefaultBroker( String testMethodName, File path,
            int machineId, final LocalhostZooKeeperCluster zooKeeper, String haServer,
            String[] extraArgs )
    {
        List<String> args = new ArrayList<String>();
        args.add( HaConfig.CONFIG_KEY_SERVER );
        args.add( haServer );
        args.add( HaConfig.CONFIG_KEY_COORDINATORS );
        args.add( zooKeeper.getConnectionString() );
        args.addAll( asList( extraArgs ) );

        return new StandaloneDatabase( testMethodName, new Bootstrap( path, machineId,//
                args.toArray( new String[args.size()] ) )
        {
            @Override
            HighlyAvailableGraphDatabase start( String storeDir, Map<String, String> config )
            {
                config = removeDashes( config );
                HighlyAvailableGraphDatabase db = new HighlyAvailableGraphDatabase( storeDir,
                        config );
                System.out.println( "Started HA db (w/ zoo keeper)" );
                return db;
            }
        } )
        {
            @Override
            IllegalStateException format( StartupFailureException e )
            {
                return e.format( zooKeeper );
            }
        };
    }

    protected static Map<String, String> removeDashes( Map<String, String> config )
    {
        Map<String, String> result = new HashMap<String, String>();
        for ( Map.Entry<String, String> entry : config.entrySet() )
        {
            String key = entry.getKey();
            key = key.startsWith( "-" ) ? key.substring( 1 ) : key;
            result.put( key, entry.getValue() );
        }
        return result;
    }

    public static StandaloneDatabase withFakeBroker( String testMethodName, final File path,
            int machineId, final int masterId, String[] extraArgs )
    {
        StandaloneDatabase standalone = new StandaloneDatabase( testMethodName, new Bootstrap(
                path, machineId, extraArgs )
        {
            @Override
            HighlyAvailableGraphDatabase start( String storeDir, final Map<String, String> config )
            {
                HighlyAvailableGraphDatabase haGraphDb = new HighlyAvailableGraphDatabase( storeDir, config )
                {
                    @Override
                    protected Broker createBroker()
                    {
                        if ( getMachineId() == masterId )
                        {
                            ZooClient.Configuration zooConfig = ConfigProxy.config( removeDashes( config ), ZooClient.Configuration.class );
                            AbstractBroker.Configuration brokerConfig = ConfigProxy.config( removeDashes( config ), AbstractBroker.Configuration.class );
                            return new FakeMasterBroker( brokerConfig, zooConfig );
                        }
                        else
                        {
                            return new FakeSlaveBroker( new MasterClient( "localhost",
                                    Protocol.PORT, getMessageLog(), storeIdGetter, Client.ConnectionLostHandler.NO_ACTION, Client.DEFAULT_READ_RESPONSE_TIMEOUT_SECONDS,
                                    Client.DEFAULT_READ_RESPONSE_TIMEOUT_SECONDS,
                                    Client.DEFAULT_MAX_NUMBER_OF_CONCURRENT_CHANNELS_PER_CLIENT ),
                                    masterId, ConfigProxy.config( MapUtil.stringMap( "ha.server_id", Integer.toString( getMachineId() ) ), AbstractBroker.Configuration.class ));
                        }
                    }
                };
//=======
//                    broker = new FakeSlaveBroker( new MasterClient( "localhost",
//                                            Protocol.PORT,
//                                            placeHolderGraphDb,
//                                            null,
//                                            Client.DEFAULT_READ_RESPONSE_TIMEOUT_SECONDS,
//                            Client.DEFAULT_READ_RESPONSE_TIMEOUT_SECONDS,
//                            Client.DEFAULT_MAX_NUMBER_OF_CONCURRENT_CHANNELS_PER_CLIENT ),
//                            masterId, machineId, placeHolderGraphDb );
//                }
//                config = removeDashes( config );
//                HighlyAvailableGraphDatabase db = new HighlyAvailableGraphDatabase( new HAGraphDb( storeDir, config,
//                                        AbstractHaTest.wrapBrokerAndSetPlaceHolderDb(
//                                                placeHolderGraphDb, broker ),
//                                        new FakeClusterClient( broker ) ) );
//                placeHolderGraphDb.setDb( db );
//>>>>>>> master
                System.out.println( "Started HA db (w/o zoo keeper)" );
                return haGraphDb;
            }
        } );
        return standalone;
    }

    private StandaloneDatabase( String testMethodName, Bootstrap bootstrap )
    {
        process = new HaDbProcess( testMethodName ).start( bootstrap );
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + process;
    }

    public void awaitStarted()
    {
        try
        {
            process.awaitStarted();
        }
        catch ( StartupFailureException e )
        {
            throw format( e );
        }
    }

    public <T> T executeJob( Job<T> job ) throws Exception
    {
        try
        {
            return process.executeJob( job );
        }
        catch ( StartupFailureException e )
        {
            throw format( e );
        }
    }

    public int getMachineId()
    {
        try
        {
            return process.getMachineId();
        }
        catch ( StartupFailureException e )
        {
            throw format( e );
        }
    }

    public void pullUpdates()
    {
        try
        {
            process.pullUpdates();
        }
        catch ( StartupFailureException e )
        {
            throw format( e );
        }
    }

    IllegalStateException format( StartupFailureException e )
    {
    	return e.format();
    }

    public void shutdown()
    {
        SubProcess.stop( process );
    }

    public void kill()
    {
        SubProcess.kill( process );
    }

    // <IMPLEMENTATION>

    public interface Controller
    {
        void pullUpdates() throws StartupFailureException;

        void awaitStarted() throws StartupFailureException;

        int getMachineId() throws StartupFailureException;

        <T> T executeJob( Job<T> job ) throws Exception;
    }

    public static class StartupFailureException extends Exception
    {
        private final long timestamp;

        StartupFailureException( Throwable cause )
        {
            super( cause );
            timestamp = new Date().getTime();
        }

        public IllegalStateException format()
        {
            return new IllegalStateException( message(), getCause() );
        }

        private String message()
        {
            return "database failed to start @ " + Format.time( timestamp );
        }

        IllegalStateException format( LocalhostZooKeeperCluster zooKeeper )
        {
            Throwable cause = getCause();
            String message = message();
            if ( cause instanceof ZooKeeperException )
            {
                message += ". ZooKeeper status: " + zooKeeper.getStatus();
            }
            return new IllegalStateException( message, cause );
        }
    }

    public static abstract class Bootstrap implements Serializable
    {
        private final String[] config;
        private final File storeDir;
        final int machineId;

        private Bootstrap( File storeDir, int machineId, String... config )
        {
            this.storeDir = storeDir;
            this.machineId = machineId;
            this.config = config;
        }

        final HighlyAvailableGraphDatabase start()
        {
            Map<String, String> params = new HashMap<String, String>();
            params.put( HaConfig.CONFIG_KEY_SERVER_ID, Integer.toString( machineId ) );
            for ( int i = 0; i < config.length; i += 2 )
            {
                params.put( config[i], config[i + 1] );
            }
            return start( storeDir.getAbsolutePath(), params );
        }

        abstract HighlyAvailableGraphDatabase start( String storeDir, Map<String, String> config );
    }

    private static abstract class DatabaseReference
    {
        abstract HighlyAvailableGraphDatabase graph() throws StartupFailureException;

        void shutdown()
        {
        }
    }

    private static class HaDbProcess extends SubProcess<Controller, Bootstrap> implements
            Controller
    {
        private transient volatile DatabaseReference db = null;
        private final String testMethodName;

        private HaDbProcess( String testMethodName )
        {
            this.testMethodName = testMethodName;
        }

        private HighlyAvailableGraphDatabase db() throws StartupFailureException
        {
            DatabaseReference ref = db;
            if ( ref == null ) throw new IllegalStateException( "database has not been started" );
            return ref.graph();
        }

        private synchronized void db( final HighlyAvailableGraphDatabase graph )
        {
            if ( db != null && graph != null )
            {
                graph.shutdown();
                throw new IllegalStateException( "database has already been started" );
            }
            db = new DatabaseReference()
            {
                @Override
                HighlyAvailableGraphDatabase graph()
                {
                    if ( graph == null )
                        throw new IllegalStateException( "database has been shut down" );
                    return graph;
                }

                @Override
                void shutdown()
                {
                    if ( graph == null )
                    {
                        System.out.println( "database has already been shut down" );
                    }
                    else
                    {
                        graph.shutdown();
                    }
                }
            };
        }

        private synchronized Throwable db( final Throwable cause )
        {
            db = new DatabaseReference()
            {
                @Override
                HighlyAvailableGraphDatabase graph() throws StartupFailureException
                {
                    throw new StartupFailureException( cause );
                }
            };
            return cause;
        }

        @Override
        public String toString()
        {
            return testMethodName;
        }

        @Override
        protected synchronized void startup( Bootstrap bootstrap ) throws Throwable
        {
            System.setOut( new TimestampStream( System.out ) );
            System.setErr( new TimestampStream( System.err ) );
            if ( db != null ) throw new IllegalStateException( "already started" );
            System.out.println( "About to start" );
            try
            {
                db( bootstrap.start() );
            }
            catch ( Throwable exception )
            {
                System.out.println( "Startup failed: " + exception );
                throw db( exception );
            }
        }

        @Override
        protected synchronized void shutdown( boolean normal )
        {
            DatabaseReference ref = db;
            if ( ref == null )
            {
                System.out.println( "Shutdown attempted before completion of startup" );
                throw new IllegalStateException( "database has not been started" );
            }
            System.out.println( "Shutdown started" );
            try
            {
                ref.shutdown();
            }
            finally
            {
                db( (HighlyAvailableGraphDatabase) null );
            }
            System.out.println( "Shutdown completed" );
            super.shutdown( normal );
        }

        public void awaitStarted() throws StartupFailureException
        {
            boolean interrupted = false;
            while ( this.db == null )
            {
                try
                {
                    Thread.sleep( 100 );
                }
                catch ( InterruptedException e )
                {
                    interrupted = true;
                    Thread.interrupted();
                }
            }
            if ( interrupted ) Thread.currentThread().interrupt();
            db();
        }

        public <T> T executeJob( Job<T> job ) throws Exception
        {
            HighlyAvailableGraphDatabase database = db();
            System.out.println( "Executing job " + job );
            T result = job.execute( database );
            System.out.println( "Job " + job + " executed" );
            return result;
        }

        public int getMachineId() throws StartupFailureException
        {
            return Integer.parseInt( db().getSingleManagementBean( HighAvailability.class ).getMachineId() );
        }

        public void pullUpdates() throws StartupFailureException
        {
            HighlyAvailableGraphDatabase database = db();
            System.out.println( "pullUpdates" );
            database.pullUpdates();
        }
    }

    private static class TimestampStream extends PrintStream
    {
        TimestampStream( PrintStream out )
        {
            super( out );
        }

        @Override
        public void println( String string )
        {
            super.println( "[" + Format.time( new Date() ) + "] " + string );
        }
    }
}
