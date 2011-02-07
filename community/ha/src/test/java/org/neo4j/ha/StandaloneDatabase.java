/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.ha;

import java.io.File;
import java.io.PrintStream;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Ignore;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.AbstractBroker;
import org.neo4j.kernel.ha.Broker;
import org.neo4j.kernel.ha.CommunicationProtocol;
import org.neo4j.kernel.ha.FakeMasterBroker;
import org.neo4j.kernel.ha.FakeSlaveBroker;
import org.neo4j.kernel.ha.MasterClient;
import org.neo4j.kernel.ha.zookeeper.ZooKeeperException;
import org.neo4j.management.HighAvailability;
import org.neo4j.test.SubProcess;

import slavetest.Job;

@Ignore
public class StandaloneDatabase
{
    private final Controller process;

    public static StandaloneDatabase withDefaultBroker( String testMethodName, File path,
            int machineId, final LocalhostZooKeeperCluster zooKeeper, String haServer,
            String[] extraArgs )
    {
        return new StandaloneDatabase( testMethodName, new Bootstrap( path, machineId,//
                HighlyAvailableGraphDatabase.CONFIG_KEY_HA_SERVER, haServer,//
                HighlyAvailableGraphDatabase.CONFIG_KEY_HA_ZOO_KEEPER_SERVERS,
                zooKeeper.getConnectionString() )
        {
            @Override
            HighlyAvailableGraphDatabase start( String storeDir, Map<String, String> config )
            {
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

    public static StandaloneDatabase withFakeBroker( String testMethodName, File path,
            int machineId, final int masterId, String[] extraArgs )
    {
        StandaloneDatabase standalone = new StandaloneDatabase( testMethodName, new Bootstrap(
                path, machineId )
        {
            @Override
            HighlyAvailableGraphDatabase start( String storeDir, Map<String, String> config )
            {
                final Broker broker;
                if ( machineId == masterId )
                {
                    broker = new FakeMasterBroker( machineId, storeDir );
                }
                else
                {
                    broker = new FakeSlaveBroker( new MasterClient( "localhost",
                            CommunicationProtocol.PORT, storeDir ), masterId, machineId, storeDir );
                }
                HighlyAvailableGraphDatabase db = new HighlyAvailableGraphDatabase( storeDir,
                        config, AbstractBroker.wrapSingleBroker( broker ) );
                System.out.println( "Started HA db (w/o zoo keeper)" );
                return db;
            }
        } );
        standalone.awaitStarted();
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
			return new IllegalStateException( message() , getCause() );
		}

		private String message()
		{
			return "database failed to start @ " + TimestampStream.format( new Date( timestamp ) );
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
            params.put( HighlyAvailableGraphDatabase.CONFIG_KEY_HA_MACHINE_ID,
                    Integer.toString( machineId ) );
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
        protected synchronized void shutdown()
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
            super.shutdown();
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
            return Integer.parseInt( db().getManagementBean( HighAvailability.class ).getMachineId() );
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
        static ThreadLocal<DateFormat> timestamp = new ThreadLocal<DateFormat>()
        {
            @Override
            protected DateFormat initialValue()
            {
                return new SimpleDateFormat( "[HH:mm:ss:SS] " );
            }
        };

        static String format( Date date )
        {
            return timestamp.get().format( date );
        }

        TimestampStream( PrintStream out )
        {
            super( out );
        }

        @Override
        public void println( String string )
        {
            super.println( format( new Date() ) + string );
        }
    }
}
