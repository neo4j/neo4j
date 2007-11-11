package org.neo4j.util.shell.apps;

import java.io.Serializable;
import java.rmi.RemoteException;

import org.neo4j.util.shell.AbstractApp;
import org.neo4j.util.shell.AppCommandParser;
import org.neo4j.util.shell.Output;
import org.neo4j.util.shell.Session;
import org.neo4j.util.shell.ShellException;

public class Env extends AbstractApp
{
	@Override
	public String getDescription()
	{
		return "Lists all environment variables";
	}
	
	public String execute( AppCommandParser parser, Session session,
		Output out ) throws ShellException
	{
		try
		{
			for ( String key : session.keys() )
			{
				Serializable value = session.get( key );
				out.println( key + "=" + ( value == null ? "" : value ) );
			}
			return null;
		}
		catch ( RemoteException e )
		{
			throw new ShellException( e );
		}
	}
}
