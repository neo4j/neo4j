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
import org.neo4j.management.HighAvailability;
import org.neo4j.test.SubProcess;

import slavetest.Job;

@Ignore
public class StandaloneDatabase
{
    private final Controller process;

    public static StandaloneDatabase withDefaultBroker( String testMethodName, File path,
            int machineId, String zooKeeperConnection, String haServer, String[] extraArgs )
    {
        return new StandaloneDatabase( testMethodName, new Bootstrap( path, machineId,//
                HighlyAvailableGraphDatabase.CONFIG_KEY_HA_SERVER, haServer,//
                HighlyAvailableGraphDatabase.CONFIG_KEY_HA_ZOO_KEEPER_SERVERS, zooKeeperConnection )
        {
            @Override
            HighlyAvailableGraphDatabase start( String storeDir, Map<String, String> config )
            {
                HighlyAvailableGraphDatabase db = new HighlyAvailableGraphDatabase( storeDir,
                        config );
                System.out.println( "Started HA db (w/ zoo keeper)" );
                return db;
            }
        } );
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
        process.awaitStarted();
    }

    public <T> T executeJob( Job<T> job ) throws Exception
    {
        return process.executeJob( job );
    }

    public int getMachineId()
    {
        return process.getMachineId();
    }

    public void pullUpdates()
    {
        process.pullUpdates();
    }

    public void shutdown()
    {
        SubProcess.stop( process );
    }

    // <IMPLEMENTATION>

    public interface Controller
    {
        void pullUpdates();

        void awaitStarted();

        int getMachineId();

        <T> T executeJob( Job<T> job ) throws Exception;
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

    private static class HaDbProcess extends SubProcess<Controller, Bootstrap> implements
            Controller
    {
        private static class DatabaseReference
        {
            final HighlyAvailableGraphDatabase graph;

            DatabaseReference( HighlyAvailableGraphDatabase graph )
            {
                this.graph = graph;
            }
        }

        private transient volatile DatabaseReference db = null;
        private final String testMethodName;

        private HaDbProcess( String testMethodName )
        {
            this.testMethodName = testMethodName;
        }

        private HighlyAvailableGraphDatabase db()
        {
            DatabaseReference ref = db;
            if ( ref == null ) throw new IllegalStateException( "database has not been started" );
            if ( ref.graph == null )
                throw new IllegalStateException( "database has been shut down" );
            return ref.graph;
        }

        private synchronized boolean db( HighlyAvailableGraphDatabase graph )
        {
            if ( db != null && graph != null )
            {
                return false;
            }
            db = new DatabaseReference( graph );
            return true;
        }

        @Override
        public String toString()
        {
            return testMethodName;
        }

        @Override
        protected void startup( Bootstrap bootstrap )
        {
            System.setOut( new TimestampStream( System.out ) );
            System.setErr( new TimestampStream( System.err ) );
            if ( db != null ) throw new IllegalStateException( "already started" );
            System.out.println( "About to start" );
            HighlyAvailableGraphDatabase graph = null;
            try
            {
                graph = bootstrap.start();
            }
            finally
            {
                if ( !db( graph ) && graph != null )
                {
                    graph.shutdown();
                    throw new IllegalStateException( "already started" );
                }
            }
        }

        @Override
        protected void shutdown()
        {
            System.out.println( "Shutdown started" );
            DatabaseReference ref = db;
            try
            {
                if ( ref.graph != null ) ref.graph.shutdown();
            }
            finally
            {
                db( null );
            }
            System.out.println( "Shutdown completed" );
            super.shutdown();
        }

        public void awaitStarted()
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
        }

        public <T> T executeJob( Job<T> job ) throws Exception
        {
            HighlyAvailableGraphDatabase database = db();
            System.out.println( "Executing job " + job );
            T result = job.execute( database );
            System.out.println( "Job " + job + " executed" );
            return result;
        }

        public int getMachineId()
        {
            return Integer.parseInt( db().getManagementBean( HighAvailability.class ).getMachineId() );
        }

        public void pullUpdates()
        {
            HighlyAvailableGraphDatabase database = db();
            System.out.println( "pullUpdates" );
            database.pullUpdates();
        }
    }

    private static class TimestampStream extends PrintStream
    {
        ThreadLocal<DateFormat> timestamp = new ThreadLocal<DateFormat>()
        {
            @Override
            protected DateFormat initialValue()
            {
                return new SimpleDateFormat( "[HH:mm:ss:SS] " );
            }
        };

        TimestampStream( PrintStream out )
        {
            super( out );
        }

        @Override
        public void println( String string )
        {
            super.println( timestamp.get().format( new Date() ) + string );
        }
    }
}
