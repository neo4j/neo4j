package org.neo4j.util.shell;

/**
 * A convenience class for creating servers clients as well as finding remote
 * servers.
 */
public abstract class ShellLobby
{
	/**
	 * To get rid of the RemoteException, uses a constructor without arguments.
	 * @param cls the class of the server to instantiate.
	 * @throws ShellException if the object couldn't be instantiated.
	 * @return a new shell server.
	 */
	public static ShellServer newServer( Class<? extends ShellServer> cls )
		throws ShellException
	{
		try
		{
			return cls.newInstance();
		}
		catch ( Exception e )
		{
			throw new RuntimeException( e );
		}
	}

	/**
	 * Creates a client and "starts" it, i.e. grabs the console prompt.
	 * @param server the server (in the same JVM) which the client will
	 * communicate with.
	 * @return the new shell client.
	 */
	public static ShellClient startClient( ShellServer server )
	{
		ShellClient client = new SameJvmClient( server );
		client.grabPrompt();
		return client;
	}
	
	/**
	 * Creates a client and "starts" it, i.e. grabs the console prompt.
	 * It will try to find a remote server on "localhost".
	 * @param port the RMI port.
	 * @param name the RMI name.
	 * @throws ShellException if no server was found at the RMI location.
	 * @return the new shell client.
	 */
	public static ShellClient startClient( int port, String name )
		throws ShellException
	{
		return startClient( RmiLocation.location( "localhost", port, name ) );
	}

	/**
	 * Creates a client and "starts" it, i.e. grabs the console prompt.
	 * It will try to find a remote server specified by {@code serverLocation}.
	 * @param serverLocation the RMI location of the server to connect to.
	 * @throws ShellException if no server was found at the RMI location.
	 * @return the new shell client.
	 */
	public static ShellClient startClient( RmiLocation serverLocation )
		throws ShellException
	{
		ShellClient client = new RemoteClient( serverLocation );
		client.grabPrompt();
		return client;
	}
}
