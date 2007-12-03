package org.neo4j.util.shell;

/**
 * An implementation of {@link ShellClient} optimized to use with a server
 * in the same JVM.
 */
public class SameJvmClient extends AbstractClient
{
	private Output out = new SystemOutput();
	private ShellServer server;
	private Session session = new SameJvmSession();
	
	/**
	 * @param server the server to communicate with.
	 */
	public SameJvmClient( ShellServer server )
	{
		this.server = server;
	}
	
	public Output getOutput()
	{
		return this.out;
	}

	public ShellServer getServer()
	{
		return this.server;
	}

	public Session session()
	{
		return this.session;
	}
}
