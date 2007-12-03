package org.neo4j.util.shell;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * A shell server which clients can connect to and send requests
 * (command lines).
 */
public interface ShellServer extends Remote
{
	/**
	 * @return the name of this server.
	 * @throws RemoteException RMI error.
	 */
	String getName() throws RemoteException;
	
	/**
	 * Receives a command line (probably from a {@link ShellClient}) and reacts
	 * to it.
	 * @param line the command line to react to.
	 * @param session the client session (environment).
	 * @param out where output should go (like System.out).
	 * @return some result from the execution, it's up to the client to
	 * interpret the result, if any. F.ex. "e" could mean that the client
	 * should exit, in response to a request "exit". 
	 * @throws ShellException if there was an error in the
	 * interpretation/execution of the command line.
	 * @throws RemoteException RMI error.
	 */
	String interpretLine( String line, Session session, Output out )
		throws ShellException, RemoteException;
	
	/**
	 * Interprets a variable from a client session and returns the
	 * interpreted result.
	 * @param key the variable key.
	 * @param value the variable value.
	 * @param session the client session to get necessary values from to
	 * help the interpretation.
	 * @return the interpreted value.
	 * @throws RemoteException RMI error.
	 */
	Serializable interpretVariable( String key, Serializable value,
		Session session ) throws RemoteException;
 
	/**
	 * @return a nice welcome for a client. Typically a client connects and
	 * asks for a greeting message to display to the user.
	 * @throws RemoteException RMI error.
	 */
	String welcome() throws RemoteException;
	
	/**
	 * Sets a server property, typically used for setting default values which
	 * clients can read at startup, but can be used for anything.
	 * @param key the property key.
	 * @param value the property value.
	 * @throws RemoteException RMI error.
	 */
	void setProperty( String key, Serializable value ) throws RemoteException;
	
	/**
	 * @param key the property key.
	 * @return the server property value for {@code key}.
	 * @throws RemoteException RMI error.
	 */
	Serializable getProperty( String key ) throws RemoteException;
	
	/**
	 * Shuts down the server.
	 * @throws RemoteException RMI error.
	 */
	void shutdown() throws RemoteException;
	
	/**
	 * Makes this server available for clients to connect to via RMI.
	 * @param port the RMI port.
	 * @param name the RMI name.
	 * @throws RemoteException RMI error.
	 */
	void makeRemotelyAvailable( int port, String name ) throws RemoteException;
}
