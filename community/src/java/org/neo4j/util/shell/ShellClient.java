package org.neo4j.util.shell;

public interface ShellClient
{
	void grabPrompt();
	
	String readLine();
	
	Session session();
	
	ShellServer getServer();
	
	Output getOutput();
}
