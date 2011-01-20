package org.neo4j.server;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.lang.reflect.Array;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.neo4j.ha.LocalhostZooKeeperCluster;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Triplet;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.management.HighAvailability;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.database.DatabaseMode;
import org.neo4j.test.SubProcess;
import org.neo4j.test.TargetDirectory;

public final class ServerCluster
{
    private static final Random random = new Random();
    private final Triplet<ServerManager, URI, File>[] servers;

    public ServerCluster( TargetDirectory targetDir, LocalhostZooKeeperCluster keeperCluster,
            Pair<Integer, Integer>... ports )
    {
        // @SuppressWarnings( { "unchecked", "hiding" } )
        // Pair<ServerManager, File>[] servers = new Pair[ports.length];
        this.servers = new Triplet[ports.length];
        SubProcess<ServerManager, String> process = new ServerProcess();
        try
        {
            for ( int i = 0; i < ports.length; i++ )
            {
                Pair<String, File> config = config( targetDir, keeperCluster, i, ports[i] );
                servers[i] = awaitStartup( Pair.of( process.start( config.first() ), config.other() ) )[0];
            }
        }
        catch ( Throwable e )
        {
            // shutdownAndCleanUp( servers );
            for ( Pair<URI, File> UGLY_CODE_DUE_TO_ISSUES_IN_HA : shutdown(
                    (Class<Pair<URI, File>>) (Class) Pair.class, servers ) )
            {
                if ( UGLY_CODE_DUE_TO_ISSUES_IN_HA != null )
                    TargetDirectory.recursiveDelete( UGLY_CODE_DUE_TO_ISSUES_IN_HA.other() );
            }
            if ( e instanceof Error ) throw (Error) e;
            if ( e instanceof RuntimeException ) throw (RuntimeException) e;
            throw new RuntimeException( "Cluster startup failed", e );
        }
        // this.servers = awaitStartup( servers );
        System.out.println( "Started " + this );
    }

    @Override
    public String toString()
    {
        StringBuilder result = new StringBuilder( "ServerCluster[" );
        String prefix = "";
        for ( Triplet<ServerManager, URI, File> server : servers )
        {
            if ( server == null )
            {
                result.append( "--" );
            }
            else
            {
                result.append( prefix ).append( server.second() );
                if ( server.first().isMaster() ) result.append( " is master" );
            }
            prefix = ", ";
        }
        return result.append( "]" ).toString();
    }

    public URI getRandomServerUri( URI... exclude )
    {
        Set<URI> excluded;
        if ( exclude == null || exclude.length == 0 )
        {
            excluded = Collections.emptySet();
        }
        else
        {
            excluded = new HashSet<URI>( Arrays.asList( exclude ) );
        }
        List<URI> candidates = new ArrayList<URI>();
        for ( Triplet<ServerManager, URI, File> server : servers )
        {
            if ( server != null && !excluded.contains( server.second() ) )
                candidates.add( server.second() );
        }
        if ( candidates.isEmpty() ) throw new IllegalStateException( "No servers available" );
        return candidates.get( random.nextInt( candidates.size() ) );
    }

    public void kill( URI server )
    {
        for ( int i = 0; i < servers.length; i++ )
        {
            if ( server.equals( servers[i].second() ) )
            {
                SubProcess.kill( servers[i].first() );
                servers[i] = null;
                return;
            }
        }
        throw new IllegalArgumentException( "No such server: " + server );
    }

    public void updateAll()
    {
        for ( Triplet<ServerManager, URI, File> server : servers )
        {
            server.first().update();
        }
    }

    public void shutdown()
    {
        shutdown( null, servers );
    }

    @SuppressWarnings( "unchecked" )
    private static Pair<String, File> config( TargetDirectory targetDir,
            LocalhostZooKeeperCluster keeperCluster, int id, Pair<Integer, Integer> ports )
    {
        File serverDir = targetDir.directory( "server-" + ports.other(), true );
        File serverConfig = new File( serverDir, "server.cfg" );
        File dbConfig = new File( serverDir, "neo4j.cfg" );
        File dbDir = new File( serverDir, "graph-database" );

        // Server configuration
        config( serverConfig,//
                Pair.of( Configurator.DB_MODE_KEY, DatabaseMode.HA.name() ),//
                Pair.of( Configurator.DATABASE_LOCATION_PROPERTY_KEY, dbDir.getAbsolutePath() ),//
                Pair.of( Configurator.WEBSERVER_PORT_PROPERTY_KEY, ports.other().toString() ),//
                Pair.of( Configurator.DB_TUNING_PROPERTY_FILE_KEY, dbConfig.getAbsolutePath() ) );

        // Kernel (and HA) configuration
        config( dbConfig, //
                Pair.of( HighlyAvailableGraphDatabase.CONFIG_KEY_HA_SERVER, "localhost:"
                                                                            + ports.first() ),//
                Pair.of( HighlyAvailableGraphDatabase.CONFIG_KEY_HA_MACHINE_ID,
                        Integer.toString( id ) ),//
                Pair.of( HighlyAvailableGraphDatabase.CONFIG_KEY_HA_ZOO_KEEPER_SERVERS,
                        keeperCluster.getConnectionString() ) );

        return Pair.of( serverConfig.getAbsolutePath(), serverDir );
    }

    private static void config( File configFile, Pair<String, String>... config )
    {
        String[] content = new String[config.length];
        for ( int i = 0; i < config.length; i++ )
        {
            content[i] = config[i].first() + "=" + config[i].other();
        }
        write( configFile, content );
    }

    private static void write( File target, String... content )
    {
        PrintStream writer;
        try
        {
            writer = new PrintStream( target );
        }
        catch ( FileNotFoundException e )
        {
            throw new RuntimeException( e );
        }
        try
        {
            for ( String line : content )
            {
                writer.println( line );
            }
        }
        finally
        {
            writer.close();
        }
    }

    private static void shutdownAndCleanUp( Pair<ServerManager, File>[] servers )
    {
        for ( File store : shutdown( File.class, servers ) )
        {
            if ( store != null ) TargetDirectory.recursiveDelete( store );
        }
    }

    private static <T> T[] shutdown( Class<T> type, Pair<ServerManager, T>[] servers )
    {
        @SuppressWarnings( "unchecked" ) T[] result = ( type != null ) ? (T[]) Array.newInstance(
                type, servers.length ) : null;
        for ( int i = 0; i < servers.length; i++ )
        {
            if ( servers[i] != null )
            {
                SubProcess.stop( servers[i].first() );
                if ( result != null ) result[i] = servers[i].other();
            }
        }
        return result;
    }

    private static Triplet<ServerManager, URI, File>[] awaitStartup(
            Pair<ServerManager, File>... managers )
    {
        @SuppressWarnings( "unchecked" ) Triplet<ServerManager, URI, File>[] result = new Triplet[managers.length];
        for ( int i = 0; i < result.length; i++ )
        {
            Pair<ServerManager, File> manager = managers[i];
            result[i] = Triplet.of( manager.first(), manager.first().awaitStartup(), manager.other() );
        }
        return result;
    }

    public interface ServerManager
    {
        URI awaitStartup();

        void update();

        boolean isMaster();
    }

    private static class ServerProcess extends SubProcess<ServerManager, String> implements
            ServerManager
    {
        private transient BootStrapper bootstrap = null;
        private transient volatile Integer startupStatus = null;

        @Override
        protected void startup( String configFilePath )
        {
            System.out.println( "configFilePath=" + configFilePath );
            System.setProperty( Configurator.NEO_SERVER_CONFIG_FILE_KEY, configFilePath );
            this.bootstrap = new BootStrapper();
            this.startupStatus = this.bootstrap.start();
        }

        @Override
        protected void shutdown()
        {
            if ( this.bootstrap != null ) this.bootstrap.stop();
            super.shutdown();
        }

        private HighAvailability ha()
        {
            return graphDb().getManagementBean( HighAvailability.class );
        }

        private AbstractGraphDatabase graphDb()
        {
            return this.bootstrap.getServer().getDatabase().graph;
        }

        @Override
        public URI awaitStartup()
        {
            try
            {
                while ( startupStatus == null )
                    Thread.sleep( 10 );
            }
            catch ( InterruptedException ex )
            {
                throw new RuntimeException( "Interrupted during startup", ex );
            }
            if ( startupStatus.equals( BootStrapper.GRAPH_DATABASE_STARTUP_ERROR_CODE ) )
                throw new RuntimeException( "Database startup failure" );
            if ( startupStatus.equals( BootStrapper.WEB_SERVER_STARTUP_ERROR_CODE ) )
                throw new RuntimeException( "Server startup failure" );
            return bootstrap.getServer().restApiUri();
        }

        @Override
        public boolean isMaster()
        {
            return ha().isMaster();
        }

        @Override
        public void update()
        {
            System.out.println( ha().update() );
        }
    }
}
