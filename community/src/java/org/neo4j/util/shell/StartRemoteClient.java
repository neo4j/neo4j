package org.neo4j.util.shell;

public class StartRemoteClient
{
	public static void main( String[] args ) throws Exception
	{
		ShellServer server = ShellLobby.getInstance().findRemoteServer(
			getPort( args ), getShellName( args ) );
		ShellLobby.getInstance().startClient( server );
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
