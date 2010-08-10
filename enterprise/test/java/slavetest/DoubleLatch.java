package slavetest;

import java.rmi.Remote;
import java.rmi.RemoteException;

interface DoubleLatch extends Remote
{
    void countDownFirst() throws RemoteException;
    
    void awaitFirst() throws RemoteException;
    
    void countDownSecond() throws RemoteException;
    
    void awaitSecond() throws RemoteException;
}