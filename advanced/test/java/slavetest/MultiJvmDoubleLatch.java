package slavetest;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.CountDownLatch;

public class MultiJvmDoubleLatch extends UnicastRemoteObject implements DoubleLatch
{
    private transient final CountDownLatch first = new CountDownLatch( 1 );
    private transient final CountDownLatch second = new CountDownLatch( 1 );
    
    protected MultiJvmDoubleLatch() throws RemoteException
    {
        super();
    }

    public void countDownFirst() throws RemoteException
    {
        first.countDown();
    }

    public void awaitFirst() throws RemoteException
    {
        await( first );
    }

    public void countDownSecond() throws RemoteException
    {
        second.countDown();
    }

    public void awaitSecond() throws RemoteException
    {
        await( second );
    }

    private void await( CountDownLatch latch )
    {
        try
        {
            latch.await();
        }
        catch ( InterruptedException e )
        {
            Thread.interrupted();
            e.printStackTrace();
        }
    }
}
