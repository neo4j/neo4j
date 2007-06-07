package org.neo4j.impl.shell.apps;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.impl.core.NodeManager;
import org.neo4j.impl.shell.NeoApp;
import org.neo4j.util.shell.AppCommandParser;
import org.neo4j.util.shell.OptionValueType;
import org.neo4j.util.shell.Output;
import org.neo4j.util.shell.Session;
import org.neo4j.util.shell.ShellException;

public class Cd extends NeoApp
{
	public static final String WORKING_DIR_KEY = "WORKING_DIR";
	
	public Cd()
	{
		this.addValueType( "a", new OptionContext( OptionValueType.NONE,
			"Absolute id, doesn't need to be connected to current node" ) );
	}
	
	@Override
	public String getDescription()
	{
		return "Changes the current node. Usage: cd <node-id>";
	}
	
	@Override
	protected String exec( AppCommandParser parser, Session session, Output out )
		throws ShellException, RemoteException
	{
		List<Long> paths = readPaths( session );
		
		Node currentNode = this.getCurrentNode( session );
		Node newNode = null;
		if ( parser.arguments().isEmpty() )
		{
			newNode = NodeManager.getManager().getReferenceNode();
			paths.clear();
		}
		else
		{
			String arg = parser.arguments().get( 0 );
			long newId = currentNode.getId();
			if ( arg.equals( ".." )  )
			{
				if ( paths.size() > 0 )
				{
					newId = paths.remove( paths.size() - 1 );
				}
			}
			else if ( arg.equals( "." ) )
			{
			}
			else
			{
				newId = Long.parseLong( arg );
				if ( newId == currentNode.getId() )
				{
					throw new ShellException( "Can't cd to the current node" );
				}
				boolean absolute = parser.options().containsKey( "a" );
				if ( !absolute && !this.nodeIsConnected( currentNode, newId ) )
				{
					throw new ShellException( "Node " + newId +
						" isn't connected to the current node" );
				}
				paths.add( currentNode.getId() );
			}
			newNode = this.getNodeById( newId );
		}
		
		this.setCurrentNode( session, newNode );
		session.set( WORKING_DIR_KEY, this.makePath( paths ) );
		return null;
	}
	
	private boolean nodeIsConnected( Node currentNode, long newId )
	{
		for ( Relationship rel : currentNode.getRelationships() )
		{
			if ( rel.getOtherNode( currentNode ).getId() == newId )
			{
				return true;
			}
		}
		return false;
	}

	public static List<Long> readPaths( Session session ) throws RemoteException
	{
		List<Long> list = new ArrayList<Long>();
		String path = ( String ) session.get( WORKING_DIR_KEY );
		if ( path != null && path.trim().length() > 0 )
		{
			for ( String id : path.split( "," ) )
			{
				list.add( new Long( id ) );
			}
		}
		return list;
	}
	
	private String makePath( List<Long> paths )
	{
		StringBuffer buffer = new StringBuffer();
		for ( Long id : paths )
		{
			if ( buffer.length() > 0 )
			{
				buffer.append( "," );
			}
			buffer.append( id );
		}
		return buffer.length() > 0 ? buffer.toString() : null;
	}
}
