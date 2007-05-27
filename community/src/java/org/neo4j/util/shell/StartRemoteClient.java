package org.neo4j.util.shell;

public class StartRemoteClient
{
	public static void main( String[] args )
	{
		try
		{
			printGreeting( args );
			ShellServer server = ShellLobby.getInstance().findRemoteServer(
				getPort( args ), getShellName( args ) );
			ShellLobby.getInstance().startClient( server );
		}
		catch ( Exception e )
		{
			System.err.println( "Can't start client shell: " + e );
		}
	}
	
	private static void printGreeting( String[] args )
	{
		if ( args.length == 0 )
		{
			System.out.println( "NOTE: No port or RMI name specified, using " +
				"default port " + AbstractServer.DEFAULT_PORT + " and name '" +
				AbstractServer.DEFAULT_NAME + "'." );
		}
	}
	
	private static int getPort( String[] args )
	{
		try
		{
			return args[ 0 ] != null ? Integer.parseInt( args [ 0 ] ) :
				AbstractServer.DEFAULT_PORT;
		}
		catch ( ArrayIndexOutOfBoundsException e )
		// Intentionally let NumberFormat propagate out to user
		{
			return AbstractServer.DEFAULT_PORT;
		}
	}
	
	private static String getShellName( String[] args )
	{
		try
		{
			return args[ 1 ] != null ? args [ 1 ] : AbstractServer.DEFAULT_NAME;
		}
		catch ( ArrayIndexOutOfBoundsException e )
		{
			return AbstractServer.DEFAULT_NAME;
		}
	}
}
