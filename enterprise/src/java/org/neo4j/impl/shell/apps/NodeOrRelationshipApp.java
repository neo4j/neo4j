package org.neo4j.impl.shell.apps;

import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.impl.shell.NeoApp;
import org.neo4j.util.shell.AppCommandParser;
import org.neo4j.util.shell.OptionValueType;
import org.neo4j.util.shell.ShellException;

abstract class NodeOrRelationshipApp extends NeoApp
{
	public NodeOrRelationshipApp()
	{
		this.addValueType( "e", new OptionContext( OptionValueType.MUST,
			"Temporarily select a connected relationship to do the " +
			"operation on" ) );
	}
	
	protected NodeOrRelationship getNodeOrRelationship( Node node,
		AppCommandParser parser ) throws ShellException
	{
		if ( parser.options().containsKey( "e" ) )
		{
			long relId = Long.parseLong( parser.options().get( "e" ) );
			Relationship rel = findRelationshipOnNode( node, relId );
			if ( rel == null )
			{
				throw new ShellException( "No relationship " + relId +
					" connected to the current node" );
			}
			return NodeOrRelationship.wrap( rel );
		}
		return NodeOrRelationship.wrap( node );
	}
	
	protected Relationship findRelationshipOnNode( Node node, long id )
	{
		for ( Relationship rel : node.getRelationships() )
		{
			if ( rel.getId() == id )
			{
				return rel;
			}
		}
		return null;
	}
}
