package slavetest;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;

import org.neo4j.helpers.Args;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.shell.impl.RmiLocation;

public class StandaloneDb extends UnicastRemoteObject implements StandaloneDbCom
{
    private final HighlyAvailableGraphDatabase db;
    private final RmiLocation location;
    private volatile boolean shutdown;

    public StandaloneDb( Args args, RmiLocation location ) throws RemoteException
    {
        super();
        
        boolean isMaster = args.getBoolean( "master", null ).booleanValue();
        String storeDir = args.get( "path", null );
        AbstractBroker broker = isMaster ? new FakeMasterBroker() : new FakeSlaveBroker(
                args.getNumber( "id", null ).intValue() );
        this.db = new HighlyAvailableGraphDatabase( storeDir, new HashMap<String, String>(),
                broker );
        broker.setDb( this.db );
        this.location = location;
        this.location.ensureRegistryCreated();
        this.location.bind( this );
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

    public void shutdown() throws RemoteException
    {
        this.location.unbind( this );
        this.shutdown = true;
    }
    
    public <T> T executeJob( Job<T> job )
    {
        return job.execute( this.db );
    }

    public void pullUpdates()
    {
        db.pullUpdates();
    }
}
