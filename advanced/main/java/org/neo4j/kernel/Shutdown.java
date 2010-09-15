package org.neo4j.kernel;

import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.kernel.apps.ReadOnlyGraphDatabaseApp;

public class Shutdown extends ReadOnlyGraphDatabaseApp
{
    @Override
    protected String exec( AppCommandParser parser, Session session, Output out ) throws Exception
    {
        getServer().getDb().shutdown();
        return null;
    }
}
