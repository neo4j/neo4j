package org.neo4j.kernel;

import java.rmi.RemoteException;

import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.kernel.apps.ReadOnlyGraphDatabaseApp;

public class Pullupdates extends ReadOnlyGraphDatabaseApp
{
    @Override
    protected String exec( AppCommandParser parser, Session session, Output out )
            throws ShellException, RemoteException
    {
        ((HighlyAvailableGraphDatabase) getServer().getDb()).pullUpdates();
        return null;
    }
}
