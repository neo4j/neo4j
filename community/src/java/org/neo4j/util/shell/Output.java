package org.neo4j.util.shell;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * An interface for printing output, like System.out, The implementation can
 * be via RMI or locally. 
 */
public interface Output extends Appendable, Remote
{
	/**
	 * Prints a line to the output.
	 * @param object the object to print (the string representation of it).
	 * @throws RemoteException RMI error.
	 */
	void print( Serializable object ) throws RemoteException;
	
	/**
	 * Prints a new line to the output.
	 * @param object the object to print (the string representation of it).
	 * @throws RemoteException RMI error.
	 */
	void println() throws RemoteException;
	
	/**
	 * Prints a line with new line to the output.
	 * @param object the object to print (the string representation of it).
	 * @throws RemoteException RMI error.
	 */
	void println( Serializable object ) throws RemoteException;
}
