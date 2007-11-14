package org.neo4j.util.shell.apps;

import org.neo4j.util.shell.AbstractApp;
import org.neo4j.util.shell.AppCommandParser;
import org.neo4j.util.shell.Output;
import org.neo4j.util.shell.Session;
import org.neo4j.util.shell.ShellException;

public class Export extends AbstractApp
{
	@Override
	public String getDescription()
	{
		return "Sets an environment variable. Usage: export <key>=<value>";
	}

	public String execute( AppCommandParser parser, Session session,
		Output out ) throws ShellException
	{
		StringBuffer buffer = new StringBuffer();
		for ( String string : parser.arguments() )
		{
			buffer.append( string );
		}
		
		String string = buffer.toString();
		int index = string.indexOf( '=' );
		if ( index == -1 )
		{
			throw new ShellException( "Invalid format <key>=<value>" );
		}
		
		String key = string.substring( 0, index );
		String value = string.substring( index + 1 );
		if ( value == null || value.trim().length() == 0 )
		{
			safeRemove( session, key );
		}
		else
		{
			safeSet( session, key, value );
		}
		return null;
	}
}
