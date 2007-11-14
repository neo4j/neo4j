package org.neo4j.util.shell;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ShellServer extends Remote
{
	String getName() throws RemoteException;
	
	/**
	 * @return a String with some result, if it contains "e" it means exit
	 */
	String interpretLine( String line, Session session, Output out )
		throws ShellException, RemoteException;
	
	/**
	 * Typical use is the PS1 environment variable, where the server converts
	 * "neo-sh[\W]$ " into
	 * "neo-sh[23]$ " (where 23 is the current node id).
	 * @param key the variable key.
	 * @param value the variable value.
	 * @param session the client session to get necessary values from to
	 * help the interpretation.
	 * @return the interpreted value.
	 */
	Serializable interpretVariable( String key, Serializable value,
		Session session ) throws RemoteException;
 
	String welcome() throws RemoteException;
	
	void setProperty( String key, Serializable value ) throws RemoteException;
	
	Serializable getProperty( String key ) throws RemoteException;
	
	void shutdown() throws RemoteException;
	
	void makeRemotelyAvailable( int port, String name ) throws RemoteException;
}
