package org.neo4j.util.shell;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Completely server-side
 */
public class AppCommandParser
{
	private AppShellServer ui;
	private String line;
	private String appName;
	private App app;
	private Map<String, String> options = new HashMap<String, String>();
	private List<String> arguments = new ArrayList<String>();
	
	public AppCommandParser( AppShellServer ui, String line )
		throws ShellException
	{
		this.ui = ui;
		if ( line != null )
		{
			line = line.trim();
		}
		this.line = line;
		this.parse();
	}
	
	private void parse() throws ShellException
	{
		if ( this.line == null || this.line.trim().length() == 0 )
		{
			return;
		}
		
		this.parseApp();
		this.parseParameters();
	}
	
	private void parseApp() throws ShellException
	{
		int index = findNextWhiteSpace( this.line, 0 );
		this.appName = index == -1 ?
			this.line : this.line.substring( 0, index );
		try
		{
			this.app = this.ui.findApp( this.appName );
		}
		catch ( Exception e )
		{
			throw new ShellException( e );
		}
		if ( this.app == null )
		{
			throw new ShellException(
				"Unknown command '" + this.appName + "'" );
		}
	}
	
	private void parseParameters() throws ShellException
	{
		String rest = this.line.substring( this.appName.length() ).trim();
		String[] parsed = tokenizeStringWithQuotes( rest, false );
		for ( int i = 0; i < parsed.length; i++ )
		{
			String string = parsed[ i ];
			if ( string.startsWith( "--" ) )
			{
				// It is one long name
				String name = string.substring( 2 );
				i = this.fetchArguments( parsed, i, name );
			}
			else if ( this.isOption( string ) )
			{
				String options = string.substring( 1 );
				for ( int o = 0; o < options.length(); o++ )
				{
					String name = String.valueOf( options.charAt( o ) );
					i = this.fetchArguments( parsed, i, name );
				}
			}
			else
			{
				this.arguments.add( string );
			}
		}
	}
	
	private boolean isOption( String string )
	{
		return string.startsWith( "-" );
	}
	
	private int fetchArguments( String[] parsed, int whereAreWe,
		String optionName ) throws ShellException
	{
		String value = null;
		OptionValueType type = this.app.getOptionValueType( optionName );
		if ( type == OptionValueType.MUST )
		{
			whereAreWe++;
			String message = "Value required for '" + optionName + "'";
			this.assertHasIndex( parsed, whereAreWe, message );
			value = parsed[ whereAreWe ];
			if ( this.isOption( value ) )
			{
				throw new ShellException( message );
			}
		}
		else if ( type == OptionValueType.MAY )
		{
			if ( this.hasIndex( parsed, whereAreWe + 1 ) &&
				!this.isOption( parsed[ whereAreWe + 1 ] ) )
			{
				whereAreWe++;
				value = parsed[ whereAreWe ];
			}
		}
		this.options.put( optionName, value );
		return whereAreWe;
	}
	
	private boolean hasIndex( String[] array, int index )
	{
		return index >= 0 && index < array.length;
	}
	
	private void assertHasIndex( String[] array, int index, String message )
		throws ShellException
	{
		if ( !this.hasIndex( array, index ) )
		{
			throw new ShellException( message );
		}
	}
	
	private static int findNextWhiteSpace( String line, int fromIndex )
	{
		int index = line.indexOf( ' ', fromIndex );
		return index == -1 ? line.indexOf( '\t', fromIndex ) : index;
	}
	
	public String getAppName()
	{
		return this.appName;
	}
	
	public App app()
	{
		return this.app;
	}
	
	public Map<String, String> options()
	{
		return this.options;
	}
	
	public List<String> arguments()
	{
		return this.arguments;
	}
	
	public String getLine()
	{
		return this.line;
	}
	
	public String getLineWithoutCommand()
	{
		return this.line.substring( this.appName.length() ).trim();
	}

	public static String[] tokenizeStringWithQuotes( String string )
	{
		return tokenizeStringWithQuotes( string, true );
	}

	public static String[] tokenizeStringWithQuotes( String string,
		boolean trim )
	{
		if ( trim )
		{
			string = string.trim();
		}
		ArrayList<String> result = new ArrayList<String>();
		string = string.trim();
		boolean inside = string.startsWith( "\"" );
		StringTokenizer quoteTokenizer = new StringTokenizer( string, "\"" );
		while ( quoteTokenizer.hasMoreTokens() )
		{
			String token = quoteTokenizer.nextToken();
			if ( trim )
			{
				token = token.trim();
			}
			if ( token.length() == 0 )
			{
				// Skip it
			}
			else if ( inside )
			{
				// Don't split
				result.add( token );
			}
			else
			{
				// Split
				StringTokenizer spaceTokenizer =
					new StringTokenizer( token, " " );
				while ( spaceTokenizer.hasMoreTokens() )
				{
					String spaceToken = spaceTokenizer.nextToken();
					if ( trim )
					{
						spaceToken = spaceToken.trim();
					}
					result.add( spaceToken );
				}
			}
			inside = !inside;
		}
		return result.toArray( new String[ result.size() ] );
	}
}
