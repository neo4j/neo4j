package org.neo4j.util.shell;

import java.rmi.RemoteException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

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
	
	public void addReplacer( String key, Replacer replacer )
	{
		REPLACERS.put( key, replacer );
	}
	
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
	
	public static interface Replacer
	{
		String getReplacement( ShellServer server, Session session );
	}
	
	public static class StaticReplacer implements Replacer
	{
		private String value;
		
		public StaticReplacer( String value )
		{
			this.value = value;
		}
		
		public String getReplacement( ShellServer server, Session session )
		{
			return this.value;
		}
	}
	
	public static class DateReplacer implements Replacer
	{
		private DateFormat format;
		
		public DateReplacer( String format )
		{
			this.format = new SimpleDateFormat( format );
		}
		
		public String getReplacement( ShellServer server, Session session )
		{
			return format.format( new Date() );
		}
	}
	
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
