package org.neo4j.util.shell;

/**
 * Represents a shell client which communicates with a {@link ShellServer}.
 * A client is very thin, it just grabs a command line from the user and sends
 * it to the server, letting everything happen server-side.
 */
public interface ShellClient
{
	/**
	 * Grabs the console prompt.
	 */
	void grabPrompt();
	
	/**
	 * Reads the next line from the user console.
	 * @return the next command line from the user.
	 */
	String readLine();
	
	/**
	 * @return the session (or environment) for this client.
	 */
	Session session();
	
	/**
	 * @return the server to communicate with.
	 */
	ShellServer getServer();
	
	/**
	 * @return the output instance where output will be passed to.
	 */
	Output getOutput();
}
