package org.neo4j.impl.shell;

import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;
import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.Transaction;
import org.neo4j.impl.core.NodeManager;
import org.neo4j.util.shell.AbstractApp;
import org.neo4j.util.shell.CommandParser;
import org.neo4j.util.shell.Output;
import org.neo4j.util.shell.Session;
import org.neo4j.util.shell.ShellException;

/**
 * Completely server-side
 */
public abstract class NeoApp extends AbstractApp
{
	private static final String NODE_KEY = "CURRENT_NODE";
	
	private static Map<String, RelationshipType> relTypes;
	
	protected Node getCurrentNode( Session session )
	{
		Number id = ( Number ) this.safeGet( session, NODE_KEY );
		Node node = null;
		if ( id == null )
		{
			node = NodeManager.getManager().getReferenceNode();
			this.setCurrentNode( session, node );
		}
		else
		{
			node = NodeManager.getManager().getNodeById( id.intValue() );
		}
		return node;
	}
	
	protected void setCurrentNode( Session session, Node node )
	{
		this.safeSet( session, NODE_KEY, node.getId() );
	}
	
	protected RelationshipType[] getAllRelationshipTypes()
	{
		this.ensureRelTypesInitialized();
		return relTypes.values().toArray(
			new RelationshipType[ relTypes.size() ] );
	}
	
	private NeoShellServer getNeoServer()
	{
		return ( NeoShellServer ) this.getServer();
	}
	
	private void ensureRelTypesInitialized()
	{
		if ( relTypes == null )
		{
			relTypes = new HashMap<String, RelationshipType>();
			Class<? extends RelationshipType> cls =
				this.getNeoServer().getRelationshipTypeClass();
			for ( RelationshipType type : cls.getEnumConstants() )
			{
				relTypes.put( type.toString().toLowerCase(), type );
			}
		}
	}
	
	protected RelationshipType getRelationshipType( String name )
	{
		this.ensureRelTypesInitialized();
		RelationshipType result = relTypes.get( name.toLowerCase() );
		if ( result == null )
		{
			throw new RuntimeException( "No relationship type '" + name +
				"' found" );
		}
		return result;
	}
	
	protected Direction getDirection( String direction ) throws ShellException
	{
		return this.getDirection( direction, Direction.OUTGOING );
	}

	protected Direction getDirection( String direction,
		Direction defaultDirection ) throws ShellException
	{
		if ( direction == null )
		{
			return defaultDirection;
		}
		
		Direction result = null;
		try
		{
			result = Direction.valueOf( direction.toUpperCase() );
		}
		catch ( Exception e )
		{
			if ( direction.equalsIgnoreCase( "o" ) )
			{
				result = Direction.OUTGOING;
			}
			else if ( direction.equalsIgnoreCase( "i" ) )
			{
				result = Direction.INCOMING;
			}
		}
		
		if ( result == null )
		{
			throw new ShellException( "Unknown direction " + direction +
				" (may be OUTGOING, INCOMING, o, i)" );
		}
		return result;
	}
	
	protected Node getNodeById( long id ) throws ShellException
	{
		return this.getNeoServer().getNeo().getNodeById( id );
	}

	public final String execute( CommandParser parser, Session session,
		Output out ) throws ShellException
	{
		Transaction tx = Transaction.begin();
		try
		{
			String result = this.exec( parser, session, out );
			tx.success();
			return result;
		}
		catch ( RemoteException e )
		{
			throw new ShellException( e );
		}
		finally
		{
			tx.finish();
		}
	}
	
	protected String directionAlternatives()
	{
		return "[OUTGOING], INCOMING, o, i";
	}
	
	protected abstract String exec( CommandParser parser, Session session,
		Output out ) throws ShellException, RemoteException;
	
	
	protected String getDisplayNameForCurrentNode()
	{
		return "(me)";
	}

	public static String getDisplayNameForNode( Node node )
	{
		return node != null
			? getDisplayNameForNode( node.getId() )
			: getDisplayNameForNode( (Long) null );
	}
	
	public static String getDisplayNameForNode( Long nodeId )
	{
		return "(" + nodeId + ")";
	}
}
