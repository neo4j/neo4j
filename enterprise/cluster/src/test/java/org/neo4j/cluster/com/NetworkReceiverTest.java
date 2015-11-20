package org.neo4j.cluster.com;

import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.URI;

import org.neo4j.logging.LogProvider;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;


public class NetworkReceiverTest
{
    static final int PORT = 1234;

    @Test
    public void testGetURIWithWildCard() throws Exception
    {
        NetworkReceiver networkReceiver = new NetworkReceiver( mock( NetworkReceiver.Monitor.class ),
                mock( NetworkReceiver.Configuration.class ), mock( LogProvider.class ) );

        // Wildcard should not be resolved here
        final String wildCard = "0.0.0.0";
        URI uri = networkReceiver.getURI( new InetSocketAddress( wildCard, PORT ) );

        assertTrue( wildCard + " does not match Uri host: " + uri.getHost(), wildCard.equals( uri.getHost() ) );
        assertTrue( PORT == uri.getPort() );
    }

    @Test
    public void testGetURIWithLocalHost() throws Exception
    {
        NetworkReceiver networkReceiver = new NetworkReceiver( mock( NetworkReceiver.Monitor.class ),
                mock( NetworkReceiver.Configuration.class ), mock( LogProvider.class ) );

        // We should NOT do a reverse DNS lookup for hostname. It might not be routed properly.
        URI uri = networkReceiver.getURI( new InetSocketAddress( "localhost", PORT ) );

        assertTrue( "Uri host is not localhost ip: " + uri.getHost(), "127.0.0.1".equals( uri.getHost() ) );
        assertTrue( PORT == uri.getPort() );
    }
}