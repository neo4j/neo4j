package slavetest;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import org.neo4j.helpers.Args;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.AbstractBroker;
import org.neo4j.kernel.ha.FakeMasterBroker;
import org.neo4j.kernel.ha.FakeSlaveBroker;
import org.neo4j.shell.impl.RmiLocation;

public class StandaloneDb extends UnicastRemoteObject implements StandaloneDbCom
{
    private final PrintStream out;
    private final String storeDir;
    private final HighlyAvailableGraphDatabase db;
    private final RmiLocation location;
    private volatile boolean shutdown;

    public StandaloneDb( Args args, RmiLocation location ) throws Exception
    {
        super();
        
        storeDir = args.get( "path", null );
        out = new PrintStream( new File( new File( storeDir ), "output" ) );
        System.setOut( out );
        try
        {
            println( "About to start" );
            boolean isMaster = args.getBoolean( "master", false ).booleanValue();
            AbstractBroker broker = isMaster ? new FakeMasterBroker() : new FakeSlaveBroker(
                    args.getNumber( "id", null ).intValue() );
            this.db = new HighlyAvailableGraphDatabase( storeDir, new HashMap<String, String>(),
                    broker );
            println( "Started HA db" );
            broker.setDb( this.db );
            this.location = location;
            this.location.ensureRegistryCreated();
            this.location.bind( this );
            println( "RMI object bound" );
        }
        catch ( Exception e )
        {
            println( "Exception", e );
            throw e;
        }
    }
    
    private void println( String string, Throwable t )
    {
        println( string );
        t.printStackTrace( out );
    }
    
    private void println( String string )
    {
        out.println( new SimpleDateFormat( "HH:mm:ss:SS" ).format( new Date() ) +
                ": " + string );
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
                Thread.sleep( 1000 );
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
            }
        }
    }

    public void initiateShutdown() throws RemoteException
    {
        println( "Shutdown initiated" );
        this.location.unbind( this );
        this.db.shutdown();
        this.shutdown = true;
        println( "Shutdown done" );
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
                    Thread.sleep( 500 );
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
        println( "Executing job " + job );
        T result = job.execute( this.db );
        println( "Job " + job + " executed" );
        return result;
    }

    public void pullUpdates()
    {
        println( "pullUpdates" );
        db.pullUpdates();
    }
}
