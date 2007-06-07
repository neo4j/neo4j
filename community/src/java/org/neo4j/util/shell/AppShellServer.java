package org.neo4j.util.shell;

import java.rmi.RemoteException;

public interface AppShellServer extends ShellServer
{
	App findApp( String name ) throws RemoteException;
}
