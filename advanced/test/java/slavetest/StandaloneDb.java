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
import org.neo4j.kernel.EmbeddedGraphDatabase;
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
        out = new PrintStream( new File( new File( storeDir ), "output" ) )
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
                new EmbeddedGraphDatabase( storeDir ).shutdown();
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
                Master master = new MasterClient( "localhost", CommunicationProtocol.PORT );
                AbstractBroker broker = isMaster ? new FakeMasterBroker( tempMachineId ) :
                        new FakeSlaveBroker( master, masterId.intValue(), tempMachineId );
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
