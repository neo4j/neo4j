package org.neo4j.impl.shell.apps;

import org.neo4j.impl.shell.NeoApp;
import org.neo4j.util.shell.CommandParser;
import org.neo4j.util.shell.Output;
import org.neo4j.util.shell.Session;
import org.neo4j.util.shell.ShellException;

public class Rm extends NeoApp
{
	@Override
	public String getDescription()
	{
		return "Removes a property";
	}

	@Override
	protected String exec( CommandParser parser, Session session, Output out )
		throws ShellException
	{
		String key = parser.arguments().get( 0 );
		this.getCurrentNode( session ).removeProperty( key );
		return null;
	}
}
