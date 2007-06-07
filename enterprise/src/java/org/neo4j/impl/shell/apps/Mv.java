package org.neo4j.impl.shell.apps;

import org.neo4j.api.core.Node;
import org.neo4j.impl.shell.NeoApp;
import org.neo4j.util.shell.AppCommandParser;
import org.neo4j.util.shell.OptionValueType;
import org.neo4j.util.shell.Output;
import org.neo4j.util.shell.Session;
import org.neo4j.util.shell.ShellException;

public class Mv extends NeoApp
{
	public Mv()
	{
		this.addValueType( "o", new OptionContext( OptionValueType.NONE,
			"To override if the key already exists" ) );
	}
	
	@Override
	public String getDescription()
	{
		return "Renames a property. Usage: mv <key> <new-key>";
	}

	@Override
	protected String exec( AppCommandParser parser, Session session, Output out )
		throws ShellException
	{
		if ( parser.arguments().size() != 2 )
		{
			throw new ShellException(
				"Must supply <from-key> <to-key> arguments" );
		}
		String fromKey = parser.arguments().get( 0 );
		String toKey = parser.arguments().get( 1 );
		boolean mayOverwrite = parser.options().containsKey( "o" );
		Node currentNode = this.getCurrentNode( session );
		if ( !currentNode.hasProperty( fromKey ) )
		{
			throw new ShellException( "Property '" + fromKey +
				"' doesn't exist" );
		}
		if ( currentNode.hasProperty( toKey ) )
		{
			if ( !mayOverwrite )
			{
				throw new ShellException( "Property '" + toKey +
					"' already exists, supply -o flag to overwrite" );
			}
			else
			{
				currentNode.removeProperty( toKey );
			}
		}
		
		Object value = currentNode.removeProperty( fromKey );
		currentNode.setProperty( toKey, value );
		return null;
	}
}
