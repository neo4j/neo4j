package org.neo4j.server;

import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.ha.LocalhostZooKeeperCluster;
import org.neo4j.helpers.Pair;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.test.TargetDirectory;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;

public class HaServerFunctionalTest
{
    private static final int[] ZOOKEEPER_PORTS = { 2181, 2182 };
    @SuppressWarnings( "unchecked" )
    private static final Pair<Integer/*ha port*/, Integer/*web port*/>[] SERVER_PORTS = new Pair[] {
            Pair.of( 6001, 7474 ), Pair.of( 6002, 7475 ) };
    private static final TargetDirectory dir = TargetDirectory.forTest( HaServerFunctionalTest.class );
    private static LocalhostZooKeeperCluster zk;

    @BeforeClass
    public static void startZooKeeper()
    {
        zk = new LocalhostZooKeeperCluster( dir, ZOOKEEPER_PORTS );
    }

    @AfterClass
    public static void stopZooKeeper()
    {
        if ( zk != null ) zk.shutdown();
        zk = null;
        dir.cleanup();
    }

    private ServerCluster cluster;

    @After
    public void stopServer()
    {
        if ( cluster != null ) cluster.shutdown();
        cluster = null;
    }

    @Test
    public void canStartUpServerCluster() throws Exception
    {
        cluster = new ServerCluster( dir, zk, SERVER_PORTS );
    }

    @Test
    public void canWriteToOneServerInTheClusterAndReadFromAnother() throws Exception
    {
        cluster = new ServerCluster( dir, zk, SERVER_PORTS );
        URI base = cluster.getRandomServerUri();
        put( property( node( base, 0 ), "message" ), "hello world" );
        cluster.updateAll();
        base = cluster.getRandomServerUri( base );
        assertEquals( "hello world", get( property( node( base, 0 ), "message" ) ) );
    }

    @Test
    public void canWriteToOneServerInTheClusterThenReadFromAnotherAfterShuttingDownTheWriteServer()
            throws Exception
    {
        cluster = new ServerCluster( dir, zk, SERVER_PORTS );
        URI base = cluster.getRandomServerUri();
        put( property( node( base, 0 ), "message" ), "hello world" );
        cluster.updateAll();
        cluster.kill( base );
        base = cluster.getRandomServerUri();
        assertEquals( "hello world", get( property( node( base, 0 ), "message" ) ) );
    }

    private static URI node( URI base, int id )
    {
        return URI.create( base + "node/" + id );
    }

    private static URI property( URI entity, String key )
    {
        return URI.create( entity + "/properties/" + key );
    }

    private static Object get( URI property )
    {
        ClientResponse response = Client.create().resource( property ).accept(
                MediaType.APPLICATION_JSON_TYPE ).type( MediaType.APPLICATION_JSON_TYPE ).get(
                ClientResponse.class );
        try
        {
            if ( 200 == response.getStatus() )
            {
                return JsonHelper.jsonToSingleValue( response.getEntity( String.class ) );
            }
            else
            {
                Map<String, Object> data = JsonHelper.jsonToMap( response.getEntity( String.class ) );
                throw new RuntimeException( data.get( "message" ).toString() );
            }
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    private static void put( URI property, Object value )
    {
        Client.create().resource( property ).accept( MediaType.APPLICATION_JSON_TYPE ).type(
                MediaType.APPLICATION_JSON_TYPE ).entity( JsonHelper.createJsonFrom( value ) ).put();
    }
}
