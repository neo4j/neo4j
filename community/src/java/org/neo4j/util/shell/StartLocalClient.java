package org.neo4j.util.shell;

public class StartLocalClient
{
    public static void main( String[] args )
    {
        if ( args.length == 0 )
        {
            System.out.println( "ERROR: To start a local neo service and a" +
                "shell client on top of that you need to supply a path to a " +
                "neo store or just a new path where a new neo store will " +
                "be created if it doesn't exist" );
            return;
        }
        
        String neoDbPath = args[ 0 ];
        try
        {
            tryStartLocalServerAndClient( neoDbPath );
        }
        catch ( Exception e )
        {
            System.err.println( "Can't start client with local neo service: " +
                e );
        }
    }

    private static void tryStartLocalServerAndClient( String neoDbPath )
        throws Exception
    {
        final LocalNeoShellServer server =
            new LocalNeoShellServer( neoDbPath );
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                server.shutdown();
            }
        } );
        
        System.out.println( "NOTE: Using local neo service at '" +
            neoDbPath + "'" );
        new SameJvmClient( server ).grabPrompt();
        server.shutdown();
    }
}
