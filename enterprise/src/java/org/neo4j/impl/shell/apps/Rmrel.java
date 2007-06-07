package org.neo4j.impl.shell.apps;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.ReturnableEvaluator;
import org.neo4j.api.core.StopEvaluator;
import org.neo4j.api.core.Traverser;
import org.neo4j.api.core.Traverser.Order;
import org.neo4j.impl.core.NodeManager;
import org.neo4j.impl.shell.NeoApp;
import org.neo4j.util.shell.AppCommandParser;
import org.neo4j.util.shell.OptionValueType;
import org.neo4j.util.shell.Output;
import org.neo4j.util.shell.Session;
import org.neo4j.util.shell.ShellException;

public class Rmrel extends NeoApp
{
	public Rmrel()
	{
		this.addValueType( "r", new OptionContext( OptionValueType.MUST,
			"The relationship id." ) );
		this.addValueType( "d", new OptionContext( OptionValueType.NONE,
			"Must be supplied if the affected other node gets decoupled " +
			"after this operation so that it gets deleted." ) );
	}
	
	@Override
	public String getDescription()
	{
		return "Removes a relationship";
	}

	@Override
	protected String exec( AppCommandParser parser, Session session, Output out )
		throws ShellException, RemoteException
	{
		if ( parser.options().get( "r" ) == null )
		{
			throw new ShellException(
				"Must supply relationship id (-r <id>) to delete" );
		}
		
		Node currentNode = this.getCurrentNode( session );
		Relationship rel = findRel( currentNode, Long.parseLong(
			parser.options().get( "r" ) ) );
		rel.delete();
		if ( !currentNode.getRelationships().iterator().hasNext() )
		{
			throw new ShellException( "It would result in the current node " +
				currentNode + " to be decoupled (no relationships left)" );
		}
		Node otherNode = rel.getOtherNode( currentNode );
		if ( !otherNode.getRelationships().iterator().hasNext() )
		{
			boolean deleteOtherNodeWhenEmpty =
				parser.options().containsKey( "d" );
			if ( !deleteOtherNodeWhenEmpty )
			{
				throw new ShellException( "Since the node " + 
					getDisplayNameForNode( otherNode ) +
					" would be decoupled after this, you must supply the" +
					" -d (for delete-when-decoupled) so that it may be " +
					"removed" ); 
			}
			otherNode.delete();
		}
		else
		{
			 if ( !this.hasPathToRefNode( otherNode ) )
			 {
				 throw new ShellException( "It would result in " + otherNode +
					 " to be recursively decoupled with the reference node" );
			 }
			 if ( !this.hasPathToRefNode( currentNode ) )
			 {
				 throw new ShellException( "It would result in " + currentNode +
					 " to be recursively decoupled with the reference node" );
			 }
		}
		return null;
	}

	private Relationship findRel( Node currentNode, long relId )
		throws ShellException
	{
		for ( Relationship rel : currentNode.getRelationships() )
		{
			if ( rel.getId() == relId )
			{
				return rel;
			}
		}
		throw new ShellException( "No relationship " + relId +
			" connected to " + currentNode );
	}
	
	private boolean hasPathToRefNode( Node node )
	{
		List<Object> filterList = new ArrayList<Object>(); 
		for ( RelationshipType rel : this.getAllRelationshipTypes() )
		{
			filterList.add( rel );
			filterList.add( Direction.BOTH );
		}
		
		Node refNode = NodeManager.getManager().getReferenceNode();
		Traverser traverser = node.traverse( Order.DEPTH_FIRST,
			StopEvaluator.END_OF_NETWORK, ReturnableEvaluator.ALL,
			filterList.toArray() );
		for ( Node testNode : traverser )
		{
			if ( refNode.equals( testNode ) )
			{
				return true;
			}
		}
		return false;
	}
}
