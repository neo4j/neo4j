package org.neo4j.util.shell;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ShellServer extends Remote
{
	String getName() throws RemoteException;
	
	/**
	 * @return a String with some result, "e" means exit
	 */
	String interpretLine( String line, Session session, Output out )
		throws ShellException, RemoteException;
 
	App findApp( String command ) throws RemoteException;
	
	String welcome() throws RemoteException;
	
	void setProperty( String key, Serializable value ) throws RemoteException;
	
	Serializable getProperty( String key ) throws RemoteException;
	
	void shutdown() throws RemoteException;
	
	void makeRemotelyAvailable( int port, String name ) throws RemoteException;
}
