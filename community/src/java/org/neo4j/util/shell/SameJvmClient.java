package org.neo4j.util.shell;

public class SameJvmClient extends AbstractClient
{
	private Output out = new SystemOutput();
	private ShellServer server;
	private Session session = new SameJvmSession();
	
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
