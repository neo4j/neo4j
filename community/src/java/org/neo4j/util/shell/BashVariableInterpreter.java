package org.neo4j.util.shell;

import java.rmi.RemoteException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Can replace the prompt string (PS1) with common Bash variable interpretation,
 * f.ex. "\h [\t] \W $ " would result in "shell [10:05:30] 1243 $" 
 */
public class BashVariableInterpreter
{
	private static final Map<String, Replacer> REPLACERS =
		new HashMap<String, Replacer>();
	static
	{
		REPLACERS.put( "d", new DateReplacer( "EEE MMM dd" ) );
		REPLACERS.put( "h", new HostReplacer() );
		REPLACERS.put( "H", new HostReplacer() );
		REPLACERS.put( "s", new HostReplacer() );
		REPLACERS.put( "t", new DateReplacer( "HH:mm:ss" ) );
		REPLACERS.put( "T", new DateReplacer( "KK:mm:ss" ) );
		REPLACERS.put( "@", new DateReplacer( "KK:mm aa" ) );
		REPLACERS.put( "A", new DateReplacer( "HH:mm" ) );
		REPLACERS.put( "u", new StaticReplacer( "user" ) );
		REPLACERS.put( "v", new StaticReplacer( "1.0-b6" ) );
		REPLACERS.put( "V", new StaticReplacer( "1.0-b6" ) );
	}
	
	/**
	 * Adds a customized replacer for a certain variable.
	 * @param key the variable key, f.ex. "t".
	 * @param replacer the replacer which gives a replacement for the variable.
	 */
	public void addReplacer( String key, Replacer replacer )
	{
		REPLACERS.put( key, replacer );
	}
	
	/**
	 * Interprets a string with variables in it and replaces those variables
	 * with values from replacers, see {@link Replacer}.
	 * @param string the string to interpret.
	 * @param server the server which runs the interpretation.
	 * @param session the session (or environment) of the interpretation.
	 * @return the interpreted string.
	 */
	public String interpret( String string, ShellServer server,
		Session session )
	{
		for ( String key : REPLACERS.keySet() )
		{
			Replacer replacer = REPLACERS.get( key );
			String value = replacer.getReplacement( server, session );
			string = string.replaceAll( "\\\\" + key, value );
		}
		return string;
	}
	
	/**
	 * A replacer which can return a string to replace a variable.
	 */
	public static interface Replacer
	{
		/**
		 * Returns a string to replace something else.
		 * @param server the server which runs the interpretation.
		 * @param session the environment of the interpretation.
		 * @return the replacement.
		 */
		String getReplacement( ShellServer server, Session session );
	}
	
	/**
	 * A {@link Replacer} which gets instantiated with a string representing the
	 * replacement, which means that the value returned from
	 * {@link #getReplacement(ShellServer, Session)} is always the same.
	 */
	public static class StaticReplacer implements Replacer
	{
		private String value;
		
		/**
		 * @param value the value to return from
		 * {@link #getReplacement(ShellServer, Session)}.
		 */
		public StaticReplacer( String value )
		{
			this.value = value;
		}
		
		public String getReplacement( ShellServer server, Session session )
		{
			return this.value;
		}
	}
	
	/**
	 * A {@link Replacer} which returns a date string in a certain format.
	 */
	public static class DateReplacer implements Replacer
	{
		private DateFormat format;
	
		/**
		 * @param format the date format, see {@link SimpleDateFormat}.
		 */
		public DateReplacer( String format )
		{
			this.format = new SimpleDateFormat( format );
		}
		
		public String getReplacement( ShellServer server, Session session )
		{
			return format.format( new Date() );
		}
	}
	
	/**
	 * Returns the name of the server (or "host").
	 */
	public static class HostReplacer implements Replacer
	{
		public String getReplacement( ShellServer server, Session session )
		{
			try
			{
				return server.getName();
			}
			catch ( RemoteException e )
			{
				return "";
			}
		}
	}
}
