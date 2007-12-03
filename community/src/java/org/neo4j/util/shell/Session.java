package org.neo4j.util.shell;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * A session (or environment) for a shell client.
 */
public interface Session extends Remote
{
	/**
	 * Sets a session value.
	 * @param key the session key.
	 * @param value the value.
	 * @throws RemoteException RMI error.
	 */
	void set( String key, Serializable value ) throws RemoteException;
	
	/**
	 * @param key the key to get the session value for.
	 * @return the value for the {@code key}.
	 * @throws RemoteException RMI error.
	 */
	Serializable get( String key ) throws RemoteException;
	
	/**
	 * Removes a value from the session.
	 * @param key the session key to remove.
	 * @return the removed value, or null if none.
	 * @throws RemoteException RMI error.
	 */
	Serializable remove( String key ) throws RemoteException;
	
	/**
	 * @return all the available session keys.
	 * @throws RemoteException RMI error.
	 */
	String[] keys() throws RemoteException;
}
