package org.neo4j.util.shell;

import java.util.Map;

public class StartClient extends AbstractStarter
{
    public static void main( String[] args )
    {
        printUsage();
        
        Map<String, String> argMap = parseArgs( args );
        String path = argMap.get( StartLocalClient.ARG_PATH );
        String port = argMap.get( StartRemoteClient.ARG_PORT );
        String name = argMap.get( StartRemoteClient.ARG_NAME );
        
        if ( path != null && ( port != null || name != null ) )
        {
            System.err.println( "You have supplied both " +
                StartLocalClient.ARG_PATH + " as well as " +
                StartRemoteClient.ARG_PORT + "/" +
                StartRemoteClient.ARG_NAME + ". " +
                "You should either supply only " + StartLocalClient.ARG_PATH +
                " or " + StartRemoteClient.ARG_PORT + "/" +
                StartRemoteClient.ARG_NAME + " so that either a local or " +
                "remote shell client can be started" );
            return;
        }
        // Local
        else if ( path != null )
        {
            StartLocalClient.main( args );
        }
        // Remote
        else
        {
            StartRemoteClient.main( args );
        }
    }

    private static void printUsage()
    {
        int port = AbstractServer.DEFAULT_PORT;
        String name = AbstractServer.DEFAULT_NAME;
        String pathArg = StartLocalClient.ARG_PATH;
        String portArg = StartRemoteClient.ARG_PORT;
        String nameArg = StartRemoteClient.ARG_NAME;
        System.out.println(
            "Example arguments for remote:\n" +
                "\t-" + portArg + " " + port + "\n" +
                "\t-" + portArg + " " + port +
                    " -" + nameArg + " " + name + "\n" +
                "\t...or no arguments\n" +
            "Example arguments for local:\n" +
                "\t-" + pathArg + " /path/to/neo"
        );
    }
}
