package org.neo4j.impl.shell.apps;

import java.rmi.RemoteException;
import org.neo4j.impl.shell.NeoApp;
import org.neo4j.util.shell.CommandParser;
import org.neo4j.util.shell.Output;
import org.neo4j.util.shell.Session;
import org.neo4j.util.shell.ShellException;

public class Env extends NeoApp
{
	@Override
	public String getDescription()
	{
		return "Lists all environment variables";
	}
	
	@Override
	protected String exec( CommandParser parser, Session session, Output out )
		throws ShellException, RemoteException
	{
		for ( String key : session.keys() )
		{
			out.println( key + "=" + session.get( key ) );
		}
		return null;
	}
}
