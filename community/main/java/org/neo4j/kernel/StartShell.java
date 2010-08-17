package org.neo4j.kernel;

import org.neo4j.shell.ShellClient;
import org.neo4j.shell.ShellLobby;
import org.neo4j.shell.impl.AbstractServer;

public class StartShell
{
    public static void main( String[] args ) throws Exception
    {
        ShellClient client = ShellLobby.newClient( AbstractServer.DEFAULT_PORT, AbstractServer.DEFAULT_NAME );
//        StartClient.setSessionVariablesFromArgs( client, new Args( args ) );
        client.grabPrompt();
    }
}
