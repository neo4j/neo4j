/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.shell.impl;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.neo4j.helpers.Service;
import org.neo4j.shell.App;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.AppShellServer;
import org.neo4j.shell.Continuation;
import org.neo4j.shell.Output;
import org.neo4j.shell.Response;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.TabCompletion;
import org.neo4j.shell.TextUtil;

/**
 * A common implementation of an {@link AppShellServer}. The server can be given
 * one or more java packages f.ex. "org.neo4j.shell.apps" where some
 * common apps exist. All classes in those packages which implements the
 * {@link App} interface will be available to execute.
 */
public abstract class AbstractAppServer extends SimpleAppServer
	implements AppShellServer
{
    private final Map<String, App> apps = new TreeMap<>();

    public AbstractAppServer()
            throws RemoteException
    {
        this( true );
    }

	/**
	 * Constructs a new server.
	 * @throws RemoteException if there's an RMI error.
	 */
    public AbstractAppServer( boolean addFromServiceLoading )
        throws RemoteException
    {
        super();
        if ( addFromServiceLoading )
        {
            for ( App app : Service.load( App.class ) )
            {
                addApp( app );
            }
        }
    }

    protected void addApp( App app )
    {
        apps.put( app.getName(), app );
        try
        {
            ( (AbstractApp) app ).setServer( this );
        }
        catch ( Exception e )
        {
            // TODO It's OK, or is it?
        }
    }

    @Override
    public App findApp( String command )
	{
        return apps.get( command );
	}

	@Override
	public void shutdown() throws RemoteException
	{
	    for ( App app : this.apps.values() )
	    {
	        app.shutdown();
	    }

	    super.shutdown();
	}

	@Override
	public Response interpretLine( Serializable clientId, String line, Output out )
		throws ShellException
	{
        Session session = getClientSession( clientId );
		if ( line == null || line.trim().length() == 0 )
        {
            return new Response( getPrompt( session ), Continuation.INPUT_COMPLETE );
        }

        try
        {
            Continuation commandResult = null;
            for ( String command : line.split( Pattern.quote( "&&" ) ) )
            {
                command = TextUtil.removeSpaces( command );
                command = replaceAlias( command, session );
                AppCommandParser parser = new AppCommandParser( this, command );
                commandResult = parser.app().execute( parser, session, out );
            }
            return new Response( getPrompt( session ), commandResult );
        }
        catch ( Exception e )
        {
            throw wrapException( e );
        }
	}

    protected String replaceAlias( String line, Session session )
    {
	    boolean changed = true;
	    Set<String> appNames = new HashSet<>();
	    while ( changed )
	    {
	        changed = false;
    	    String appName = AppCommandParser.parseOutAppName( line );
            String alias = session.getAlias( appName );
    	    if ( alias != null && appNames.add( alias ) )
    	    {
    	        changed = true;
    	        line = alias + line.substring( appName.length() );
    	    }
	    }
	    return line;
    }

    @Override
	public String[] getAllAvailableCommands()
	{
		return apps.keySet().toArray( new String[apps.size()] );
	}

    @Override
    public TabCompletion tabComplete( Serializable clientID, String partOfLine )
            throws ShellException, RemoteException
    {
        // TODO We can't assume it's an AppShellServer, can we?
        try
        {
            AppCommandParser parser = new AppCommandParser( this, partOfLine );
            App app = parser.app();
            List<String> appCandidates = app.completionCandidates( partOfLine, getClientSession( clientID ) );
            appCandidates = quote( appCandidates );
            if ( appCandidates.size() == 1 )
            {
                appCandidates.set( 0, appCandidates.get( 0 ) + " " );
            }
            int cursor = partOfLine.length() - TextUtil.lastWordOrQuoteOf( partOfLine, true ).length();
            return new TabCompletion( appCandidates, cursor );
        }
        catch ( Exception e )
        {
            throw wrapException( e );
        }
    }

    protected ShellException wrapException( Exception e )
    {
        return ShellException.wrapCause( e );
    }

    private static List<String> quote( List<String> candidates )
    {
        List<String> result = new ArrayList<>();
        for ( String candidate : candidates )
        {
            candidate = candidate.replaceAll( " ", "\\\\ " );
            result.add( candidate );
        }
        return result;
    }
}
