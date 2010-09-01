package org.neo4j.shell.kernel.apps;

import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;

public abstract class ReadOnlyGraphDatabaseApp extends GraphDatabaseApp
{
    @Override
    public String execute( AppCommandParser parser, Session session, Output out ) throws Exception
    {
        return this.exec( parser, session, out );
    }
}
