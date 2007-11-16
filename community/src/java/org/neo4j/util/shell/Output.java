package org.neo4j.util.shell;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface Output extends Appendable, Remote
{
	void print( Serializable object ) throws RemoteException;
	
	void println() throws RemoteException;
	
	void println( Serializable object ) throws RemoteException;
}
