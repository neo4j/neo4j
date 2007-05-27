package org.neo4j.impl.shell.apps;

import org.neo4j.impl.shell.NeoApp;
import org.neo4j.util.shell.CommandParser;
import org.neo4j.util.shell.OptionValueType;
import org.neo4j.util.shell.Output;
import org.neo4j.util.shell.Session;
import org.neo4j.util.shell.ShellException;

public class Set extends NeoApp
{
	public Set()
	{
		this.addValueType( "t", new OptionContext( OptionValueType.MUST,
			"Value type, [String], Integer, Long, Byte a.s.o." ) );
	}

	@Override
	public String getDescription()
	{
		return "Sets a property. Usage: set <key> <value>";
	}

	@Override
	protected String exec( CommandParser parser, Session session, Output out )
		throws ShellException
	{
		String key = parser.arguments().get( 0 );
		
		Class type = this.getValueType( parser );
		Object value = null;
		try
		{
			value = type.getConstructor( String.class ).newInstance(
				parser.arguments().get( 1 ) );
		}
		catch ( Exception e )
		{
			throw new ShellException( e );
		}
		
		this.getCurrentNode( session ).setProperty( key, value );
		return null;
	}
	
	private Class getValueType( CommandParser parser ) throws ShellException
	{
		String type = parser.options().containsKey( "t" ) ?
			parser.options().get( "t" ) : String.class.getName();
		Class cls = null;
		try
		{
			cls = Class.forName( type );
		}
		catch ( ClassNotFoundException e )
		{
			// Ok
		}
		
		try
		{
			cls = Class.forName( String.class.getPackage().getName() +
				"." + type );
		}
		catch ( ClassNotFoundException e )
		{
			// Ok
		}
		
		if ( cls == null )
		{
			throw new ShellException( "Invalid value type '" + type + "'" );
		}
		return cls;
	}
}
