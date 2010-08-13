package org.neo4j.kernel;

import java.rmi.RemoteException;

import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.kernel.apps.GraphDatabaseApp;

public class Pullupdates extends GraphDatabaseApp
{
    @Override
    protected String exec( AppCommandParser parser, Session session, Output out )
            throws ShellException, RemoteException
    {
        ((HighlyAvailableGraphDatabase) getServer().getDb()).pullUpdates();
        return null;
    }
}
