package org.neo4j.util.shell;

import java.rmi.RemoteException;

/**
 * A {@link ShellServer} with the addition of executing apps.
 */
public interface AppShellServer extends ShellServer
{
	/**
	 * Finds and returns an {@link App} implementation with a given name.
	 * @param name the name of the app.
	 * @return an {@link App} instance for {@code name}.
	 * @throws RemoteException if an RMI exception occurs.
	 */
	App findApp( String name ) throws RemoteException;
}
