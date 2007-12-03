package org.neo4j.util.shell;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Common implementation of an {@link App}.
 */
public abstract class AbstractApp implements App
{
	private Map<String, OptionContext> typeExceptions =
		new HashMap<String, OptionContext>();
	private AppShellServer server;
	
	public String getName()
	{
		return this.getClass().getSimpleName().toLowerCase();
	}

	public OptionValueType getOptionValueType( String option )
	{
		OptionContext context = this.typeExceptions.get( option );
		return context == null ? OptionValueType.NONE : context.getType();
	}

	protected void addValueType( String option, OptionContext context )
	{
		this.typeExceptions.put( option, context );
	}
	
	public String[] getAvailableOptions()
	{
		String[] result = this.typeExceptions.keySet().toArray(
			new String[ this.typeExceptions.size() ] );
		Arrays.sort( result );
		return result;
	}

	public void setServer( AppShellServer server )
	{
		this.server = server;
	}
	
	protected AppShellServer getServer()
	{
		return this.server;
	}
	
	public String getDescription()
	{
		return null;
	}
	
	public String getDescription( String option )
	{
		OptionContext context = this.typeExceptions.get( option );
		return context == null ? null : context.getDescription();
	}

	protected void printMany( Output out, String string, int count )
		throws RemoteException
	{
		for ( int i = 0; i < count; i++ )
		{
			out.print( string );
		}
	}

	protected static Serializable safeGet( Session session, String key )
	{
		try
		{
			return session.get( key );
		}
		catch ( RemoteException e )
		{
			throw new RuntimeException( e );
		}
	}
	
	protected static void safeSet( Session session, String key,
		Serializable value )
	{
		try
		{
			session.set( key, value );
		}
		catch ( RemoteException e )
		{
			throw new RuntimeException( e );
		}
	}

	protected static Serializable safeRemove( Session session, String key )
	{
		try
		{
			return session.remove( key );
		}
		catch ( RemoteException e )
		{
			throw new RuntimeException( e );
		}
	}
	
	/**
	 * Groups an {@link OptionValueType} and a description.
	 */
	public static class OptionContext
	{
		private OptionValueType type;
		private String description;
		
		/**
		 * @param type the type for the option.
		 * @param description the description of the option.
		 */
		public OptionContext( OptionValueType type, String description )
		{
			this.type = type;
			this.description = description;
		}
		
		/**
		 * @return the option value type.
		 */
		public OptionValueType getType()
		{
			return this.type;
		}
		
		/**
		 * @return the description.
		 */
		public String getDescription()
		{
			return this.description;
		}
	}
}
