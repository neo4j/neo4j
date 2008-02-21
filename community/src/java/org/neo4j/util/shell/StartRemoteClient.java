package org.neo4j.util.shell;

/**
 * Convenience main class for starting a client which connects to a remote
 * server. Which port/name to connect to may be specified as arguments.
 */
public class StartRemoteClient
{
	/**
	 * Starts a client and connects to a remote server.
	 * @param args may contain RMI port/name to the server.
	 */
	public static void main( String[] args )
	{
		boolean started = false;
		try
		{
			printGreeting( args );
			if ( args.length == 0 || args.length >= 2 )
			{
				ShellLobby.startClient( getPort( args ), getShellName( args ) );
				started = true;
			}
		}
		catch ( Exception e )
		{
			System.err.println( "Can't start client shell: " + e );
		}
		
		try
		{
			if ( !started && args.length == 1 )
			{
				tryStartLocalServerAndClient( args );
				started = true;
			}
		}
		catch ( Exception e )
		{
			System.err.println( "Can't start client with local neo service: " +
				e );
		}
	}
	
	private static String getNeoDirectory( String[] args )
	{
		return args[ 0 ];
	}
	
	private static boolean tryStartLocalServerAndClient( String[] args )
		throws Exception
	{
		String neoDirectory = getNeoDirectory( args );
		final LocalNeoShellServer server =
			new LocalNeoShellServer( neoDirectory );
		server.makeRemotelyAvailable( AbstractServer.DEFAULT_PORT,
			AbstractServer.DEFAULT_NAME );
		Runtime.getRuntime().addShutdownHook( new Thread()
		{
			@Override
			public void run()
			{
				server.shutdown();
			}
		} );
		
		try
		{
			System.out.println( "NOTE: Using local neo server at '" +
				neoDirectory + "'" );
			ShellLobby.startClient( server );
		}
		catch ( RuntimeException e )
		{
			server.shutdown();
			throw e;
		}
		server.shutdown();
		return true;
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
