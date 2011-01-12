/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package slavetest;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.neo4j.helpers.Args;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.AbstractBroker;
import org.neo4j.kernel.ha.CommunicationProtocol;
import org.neo4j.kernel.ha.FakeMasterBroker;
import org.neo4j.kernel.ha.FakeSlaveBroker;
import org.neo4j.kernel.ha.Master;
import org.neo4j.kernel.ha.MasterClient;
import org.neo4j.shell.impl.RmiLocation;

public class StandaloneDb extends UnicastRemoteObject implements StandaloneDbCom
{
    private final PrintStream out;
    private final String storeDir;
    private final HighlyAvailableGraphDatabase db;
    private final RmiLocation location;
    private volatile boolean shutdown;
    private final int machineId;

    public StandaloneDb( Args args, RmiLocation location ) throws Exception
    {
        super();
        
        storeDir = args.get( "path", null );
        File storeDirFile = new File( storeDir );
        storeDirFile.mkdirs();
        out = new PrintStream( new File( storeDirFile, "output" ) )
        {
            public void println(String x)
            {
                super.println( new SimpleDateFormat( "HH:mm:ss:SS" ).format( new Date() ) + ": " + x );
            }
        };
        System.setOut( out );
        System.setErr( out );
        try
        {
            int tempMachineId;
            System.out.println( "About to start" );
            
            HighlyAvailableGraphDatabase haDb = null;
            System.out.println( args.asMap().toString() );
            if ( args.has( HighlyAvailableGraphDatabase.CONFIG_KEY_HA_MACHINE_ID ) )
            {
                tempMachineId = args.getNumber(
                        HighlyAvailableGraphDatabase.CONFIG_KEY_HA_MACHINE_ID, null ).intValue();
                Map<String, String> config = MapUtil.stringMap(
                        HighlyAvailableGraphDatabase.CONFIG_KEY_HA_MACHINE_ID, "" + tempMachineId,
                        HighlyAvailableGraphDatabase.CONFIG_KEY_HA_ZOO_KEEPER_SERVERS,
                        args.get( HighlyAvailableGraphDatabase.CONFIG_KEY_HA_ZOO_KEEPER_SERVERS, null ),
                        HighlyAvailableGraphDatabase.CONFIG_KEY_HA_SERVER,
                        args.get( HighlyAvailableGraphDatabase.CONFIG_KEY_HA_SERVER, null ),
                        "index", args.get( "index", null ) );
                haDb = new HighlyAvailableGraphDatabase( storeDir, config );
                System.out.println( "Started HA db (w/ zoo keeper)" );
            }
            else
            {
                boolean isMaster = args.getBoolean( "master", false ).booleanValue();
                tempMachineId = args.getNumber( "id", null ).intValue();
                Number masterId = args.getNumber( "master-id", null );
                Master master = new MasterClient( "localhost", CommunicationProtocol.PORT, storeDir );
                AbstractBroker broker = isMaster ? new FakeMasterBroker( tempMachineId, storeDir ) :
                        new FakeSlaveBroker( master, masterId.intValue(), tempMachineId, storeDir );
                haDb = new HighlyAvailableGraphDatabase( storeDir, MapUtil.stringMap(
                        HighlyAvailableGraphDatabase.CONFIG_KEY_HA_MACHINE_ID, "" + tempMachineId,
                        "index", args.get( "index", null ) ),
                        AbstractBroker.wrapSingleBroker( broker ) );
                System.out.println( "Started HA db (w/o zoo keeper)" );
            }
            this.location = location;
            this.location.ensureRegistryCreated();
            this.location.bind( this );
            this.machineId = tempMachineId;
            this.db = haDb;
            System.out.println( "RMI object bound" );
        }
        catch ( Exception e )
        {
            println( "Exception", e );
            throw e;
        }
    }
    
    private void println( String string, Throwable t )
    {
        System.out.println( string );
        t.printStackTrace();
    }
    
    public static void main( String[] args ) throws Exception
    {
        Args arguments = new Args( args );
        RmiLocation location = RmiLocation.location( "localhost",
                arguments.getNumber( "port", null ).intValue(), "interface" );
        StandaloneDb db = new StandaloneDb( arguments, location );
        db.waitForShutdown();
    }
    
    private void waitForShutdown()
    {
        while ( !shutdown )
        {
            try
            {
                Thread.sleep( 100 );
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
            }
        }
    }

    public void initiateShutdown() throws RemoteException
    {
        System.out.println( "Shutdown initiated" );
        this.location.unbind( this );
        this.db.shutdown();
        this.shutdown = true;
        System.out.println( "Shutdown done" );
        try
        {
            new File( new File( storeDir ), "shutdown" ).createNewFile();
        }
        catch ( IOException e )
        {
            println( "Couldn't create file, damn it", e );
        }
        new Thread()
        {
            public void run()
            {
                try
                {
                    Thread.sleep( 50 );
                }
                catch ( InterruptedException e )
                {
                    Thread.interrupted();
                }
                System.exit( 0 );
            }
        }.start();
    }
    
    public <T> T executeJob( Job<T> job ) throws RemoteException
    {
        System.out.println( "Executing job " + job );
        T result = job.execute( this.db );
        System.out.println( "Job " + job + " executed" );
        return result;
    }

    public void pullUpdates()
    {
        System.out.println( "pullUpdates" );
        db.pullUpdates();
    }
    
    public int getMachineId()
    {
        return this.machineId;
    }
    
    public void awaitStarted()
    {
        while ( this.db == null )
        {
            try
            {
                Thread.sleep( 100 );
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
            }
        }
    }
}
