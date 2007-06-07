package org.neo4j.impl.shell.apps;

import java.rmi.RemoteException;
import java.util.List;
import org.neo4j.api.core.Node;
import org.neo4j.impl.shell.NeoApp;
import org.neo4j.util.shell.AppCommandParser;
import org.neo4j.util.shell.Output;
import org.neo4j.util.shell.Session;
import org.neo4j.util.shell.ShellException;

public class Pwd extends NeoApp
{
	@Override
	public String getDescription()
	{
		return "Prints path to current node";
	}

	@Override
	protected String exec( AppCommandParser parser, Session session, Output out )
		throws ShellException, RemoteException
	{
		Node currentNode = this.getCurrentNode( session );
		out.println( "Current node is " +
			getDisplayNameForNode( currentNode ) );
		
		String path = stringifyPath( Cd.readPaths( session ) );
		if ( path.length() > 0 )
		{
			out.println( path );
		}
		return null;
	}
	
	private String stringifyPath( List<Long> pathIds )
	{
		if ( pathIds.isEmpty() )
		{
			return "";
		}			
		StringBuilder path = new StringBuilder();
		for ( Long id : pathIds )
		{
			path.append( getDisplayNameForNode( id ) ).append( "-->" );			
		}
		return path.append( getDisplayNameForCurrentNode() ).toString();
	}
}
