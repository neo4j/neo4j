package org.neo4j.impl.shell.apps;

import java.rmi.RemoteException;
import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.impl.shell.NeoApp;
import org.neo4j.util.shell.AppCommandParser;
import org.neo4j.util.shell.OptionValueType;
import org.neo4j.util.shell.Output;
import org.neo4j.util.shell.Session;
import org.neo4j.util.shell.ShellException;

public class Ls extends NeoApp
{
	public Ls()
	{
		this.addValueType( "d", new OptionContext( OptionValueType.MUST,
			"Direction filter for relationships: " +
			this.directionAlternatives() ) );
		this.addValueType( "v", new OptionContext( OptionValueType.NONE,
			"Verbose mode" ) );
		this.addValueType( "q", new OptionContext( OptionValueType.NONE,
			"Quiet mode" ) );
		this.addValueType( "p", new OptionContext( OptionValueType.NONE,
			"Lists properties" ) );
		this.addValueType( "r", new OptionContext( OptionValueType.NONE,
			"Lists relationships" ) );
	}
	
	@Override
	public String getDescription()
	{
		return "Lists the current node. Optionally supply node id for " +
			"listing a certain node: ls <node-id>";
	}
	
	@Override
	protected String exec( AppCommandParser parser, Session session, Output out )
		throws ShellException, RemoteException
	{
		boolean verbose = parser.options().containsKey( "v" );
		boolean displayValues = verbose || !parser.options().containsKey( "q" );
		boolean displayProperties =
			verbose || parser.options().containsKey( "p" );
		boolean displayRelationships =
			verbose || parser.options().containsKey( "r" );
		if ( !displayProperties && !displayRelationships )
		{
			displayProperties = true;
			displayRelationships = true;
		}
		
		Node node = null;
		if ( parser.arguments().isEmpty() )
		{
			node = this.getCurrentNode( session );
		}
		else
		{
			node = this.getNodeById(
				Long.parseLong( parser.arguments().get( 0 ) ) );
		}
		
		this.displayProperties( node, out, displayProperties, displayValues,
			verbose );
		this.displayRelationships( parser, node, out, displayRelationships );
		return null;
	}
	
	private void displayProperties( Node node, Output out,
		boolean displayProperties, boolean displayValues, boolean verbose)
		throws ShellException, RemoteException
	{
		if ( !displayProperties )
		{
			return;
		}
		int longestKey = this.findLongestKey( node );
		for ( String key : node.getPropertyKeys() )
		{
			out.print( "*" + key );
			if ( displayValues )
			{
				this.printMany( out, " ", longestKey - key.length() + 1 );
				Object value = node.getProperty( key );
				out.print( "=[" + value + "]" );
				if ( verbose )
				{
					out.print( " (" + this.getNiceType( value ) + ")" );
				}
			}
			out.println( "" );
		}
	}
	
	private void displayRelationships( AppCommandParser parser, Node node,
		Output out, boolean displayRelationships ) throws ShellException,
		RemoteException
	{
		if ( !displayRelationships )
		{
			return;
		}
		String directionFilter = parser.options().get( "d" );
		Direction direction = this.getDirection( directionFilter );
		boolean displayOutgoing = directionFilter == null ||
			direction == Direction.OUTGOING;
		boolean displayIncoming = directionFilter == null ||
			direction == Direction.INCOMING;
		if ( displayOutgoing )
		{
			for ( Relationship rel :
				node.getRelationships( Direction.OUTGOING ) )
			{
				out.println(
					getDisplayNameForCurrentNode() +
					" --[" + rel.getType() + ", " + rel.getId() + "]--> " +
					getDisplayNameForNode( rel.getEndNode() ) );
			}
		}
		if ( displayIncoming )
		{
			for ( Relationship rel :
				node.getRelationships( Direction.INCOMING ) )
			{
				out.println(
					getDisplayNameForCurrentNode() +
					" <--[" + rel.getType() + ", " + rel.getId() + "]-- " +
					getDisplayNameForNode( rel.getStartNode() ) );
			}
		}
	}
	
	private String getNiceType( Object value )
	{
		String cls = value.getClass().getName();
		return cls.substring(
			String.class.getPackage().getName().length() + 1 );
	}
	
	private int findLongestKey( Node node )
	{
		int length = 0;
		for ( String key : node.getPropertyKeys() )
		{
			if ( key.length() > length )
			{
				length = key.length();
			}
		}
		return length;
	}
}
