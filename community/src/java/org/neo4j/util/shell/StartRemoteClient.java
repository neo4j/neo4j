package org.neo4j.util.shell;

public class StartRemoteClient
{
	public static void main( String[] args ) throws Exception
	{
		// For now, just check localhost, default port/name
		ShellServer server = ShellLobby.getInstance().findRemoteServer(
			AbstractServer.DEFAULT_PORT, AbstractServer.DEFAULT_NAME );
		ShellLobby.getInstance().startClient( server );
	}
}
