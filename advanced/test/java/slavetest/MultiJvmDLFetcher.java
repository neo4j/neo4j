package slavetest;

import java.io.Serializable;
import java.rmi.RemoteException;

import org.neo4j.shell.impl.RmiLocation;

public class MultiJvmDLFetcher implements Fetcher<DoubleLatch>, Serializable
{
    public MultiJvmDLFetcher() throws RemoteException
    {
        MultiJvmDoubleLatch latch = new MultiJvmDoubleLatch();
        RmiLocation location = location();
        location.ensureRegistryCreated();
        location.bind( latch );
    }
    
    private RmiLocation location()
    {
        return RmiLocation.location( "localhost", 8054, "latch" );
    }
    
    public DoubleLatch fetch()
    {
        try
        {
            return (DoubleLatch) location().getBoundObject();
        }
        catch ( RemoteException e )
        {
            throw new RuntimeException( e );
        }
    }

    public void close()
    {
        // TODO
    }
}
