package slavetest;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface StandaloneDbCom extends Remote
{
    <T> T executeJob( Job<T> job ) throws RemoteException;
    
    void pullUpdates() throws RemoteException;
    
    void shutdown() throws RemoteException;
}
