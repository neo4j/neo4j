package org.neo4j.index.impl;

import java.rmi.RemoteException;

import org.neo4j.helpers.Service;
import org.neo4j.shell.App;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.kernel.apps.GraphDatabaseApp;

@Service.Implementation( App.class )
public class Ix extends GraphDatabaseApp
{
    @Override
    public String getDescription()
    {
        return "Utilize modern indexes (the IndexProvider API) from the shell.";
    }

    @Override
    protected String exec( AppCommandParser parser, Session session, Output out )
            throws ShellException, RemoteException
    {
        out.println( "The ix command is not fully implemented yet." );
        return null;
    }
}
