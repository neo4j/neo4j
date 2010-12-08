package org.neo4j.server.webadmin.rest;

import org.neo4j.server.database.Database;
import org.neo4j.server.webadmin.console.GremlinSession;
import org.neo4j.server.webadmin.console.ScriptSession;

import javax.servlet.http.HttpSession;

public class SessionFactoryImpl implements SessionFactory
{
    private HttpSession httpSession;

    public SessionFactoryImpl(HttpSession httpSession)
    {
        this.httpSession = httpSession;
    }

    @Override
    public ScriptSession createSession( String engineName, Database database )
    {
        Object session = httpSession.getAttribute( "consoleSession" );
        if ( session == null )
        {
            session = new GremlinSession( database );
            httpSession.setAttribute( "consoleSession", session );
        }
        return (ScriptSession) session;
    }
}
