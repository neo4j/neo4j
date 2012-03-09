/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.ext.udc.impl;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.http.localserver.LocalTestServer;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.GraphDatabaseSPI;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

/**
 * Unit testing for the UDC kernel extension.
 * <p/>
 * The UdcExtensionImpl is loaded when a new
 * GraphDatabase is instantiated, as part of
 * {@link org.neo4j.helpers.Service#load}.
 */
public class UdcExtensionImplTest
{

    Random rnd = new Random();

    @Before
    public void resetUdcState()
    {
        UdcTimerTask.successCounts.clear();
        UdcTimerTask.failureCounts.clear();
    }

    /**
     * Sanity check to make sure a database can be created
     * and destroyed.
     *
     * @throws java.io.IOException
     */
    @Test
    public void shouldNotCrashNormalGraphdbCreation() throws IOException
    {
        GraphDatabaseService graphdb = createTempDatabase( null );
        destroy( graphdb );
    }

    /**
     * Expect the counts to be initialized.
     */
    @Test
    public void shouldLoadWhenNormalGraphdbIsCreated() throws Exception
    {
        GraphDatabaseService graphdb = createTempDatabase( null );
        // when the UDC extension successfully loads, it initializes the attempts count to 0
        assertGotSuccessWithRetry( IS_ZERO );
        destroy( graphdb );
    }

    /**
     * Expect separate counts for each graphdb.
     */
    @Test
    public void shouldLoadForEachCreatedGraphdb() throws IOException
    {
        GraphDatabaseService graphdb1 = createTempDatabase( null );
        GraphDatabaseService graphdb2 = createTempDatabase( null );
        Set<String> successCountValues = UdcTimerTask.successCounts.keySet();
        assertThat( successCountValues.size(), equalTo( 2 ) );
        assertThat( "this", is( not( "that" ) ) );
        destroy( graphdb1 );
        destroy( graphdb2 );
    }

    @Test
    public void shouldRecordFailuresWhenThereIsNoServer() throws Exception
    {
        Map<String, String> config = new HashMap<String, String>();
        config.put( UdcExtensionImpl.FIRST_DELAY_CONFIG_KEY, "100" ); // first delay must be long enough to allow class initialization to complete
        config.put( UdcExtensionImpl.UDC_HOST_ADDRESS_KEY, "127.0.0.1:1" );
        GraphDatabaseService graphdb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( "should-record-failures").setConfig( config ).newGraphDatabase();
        assertGotFailureWithRetry( IS_GREATER_THAN_ZERO );
        destroy( graphdb );
    }

    @Test
    public void shouldRecordSuccessesWhenThereIsAServer() throws Exception
    {
        // first, set up the test server
        LocalTestServer server = new LocalTestServer( null, null );
        PingerHandler handler = new PingerHandler();
        server.register( "/*", handler );
        server.start();

        final String hostname = server.getServiceHostName();
        final String serverAddress = hostname + ":" + server.getServicePort();

        Map<String, String> config = new HashMap<String, String>();
        config.put( UdcExtensionImpl.FIRST_DELAY_CONFIG_KEY, "100" );
        config.put( UdcExtensionImpl.UDC_HOST_ADDRESS_KEY, serverAddress );

        GraphDatabaseService graphdb = createTempDatabase( config );
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        assertGotFailureWithRetry( IS_ZERO );
        destroy( graphdb );
    }

    @Test
    public void shouldBeAbleToSpecifySourceWithConfig() throws Exception
    {
        // first, set up the test server
        LocalTestServer server = new LocalTestServer( null, null );
        PingerHandler handler = new PingerHandler();
        server.register( "/*", handler );
        server.start();

        final String hostname = server.getServiceHostName();
        final String serverAddress = hostname + ":" + server.getServicePort();

        Map<String, String> config = new HashMap<String, String>();
        config.put( UdcExtensionImpl.FIRST_DELAY_CONFIG_KEY, "100" );
        config.put( UdcExtensionImpl.UDC_HOST_ADDRESS_KEY, serverAddress );
        config.put( UdcExtensionImpl.UDC_SOURCE_KEY, "test" );

        GraphDatabaseService graphdb = createTempDatabase( config );
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        assertEquals( "test", handler.getQueryMap().get( "source" ) );

        destroy( graphdb );
    }

    @Test
    public void shouldBeAbleToSpecifyRegistrationIdWithConfig() throws Exception
    {
        // first, set up the test server
        LocalTestServer server = new LocalTestServer( null, null );
        PingerHandler handler = new PingerHandler();
        server.register( "/*", handler );
        server.start();

        final String hostname = server.getServiceHostName();
        final String serverAddress = hostname + ":" + server.getServicePort();

        Map<String, String> config = new HashMap<String, String>();
        config.put( UdcExtensionImpl.FIRST_DELAY_CONFIG_KEY, "100" );
        config.put( UdcExtensionImpl.UDC_HOST_ADDRESS_KEY, serverAddress );
        config.put( UdcExtensionImpl.UDC_SOURCE_KEY, "test" );
        config.put( UdcExtensionImpl.UDC_REGISTRATION_KEY, "marketoid" );

        GraphDatabaseService graphdb = createTempDatabase( config );
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        assertEquals( "marketoid", handler.getQueryMap().get( "reg" ) );

        destroy( graphdb );
    }


    @Test
    public void shouldIncludeMacAddressInConfig() throws Exception
    {
        // first, set up the test server
        LocalTestServer server = new LocalTestServer( null, null );
        PingerHandler handler = new PingerHandler();
        server.register( "/*", handler );
        server.start();

        final String hostname = server.getServiceHostName();
        final String serverAddress = hostname + ":" + server.getServicePort();

        Map<String, String> config = new HashMap<String, String>();
        config.put( UdcExtensionImpl.FIRST_DELAY_CONFIG_KEY, "100" );
        config.put( UdcExtensionImpl.UDC_HOST_ADDRESS_KEY, serverAddress );

        GraphDatabaseService graphdb = createTempDatabase( config );
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        assertNotNull(handler.getQueryMap().get("mac"));

        destroy( graphdb );
    }

    private static interface Condition<T>
    {
        boolean isTrue( T value );
    }

    private static final Condition<Integer> IS_ZERO = new Condition<Integer>()
    {
        public boolean isTrue( Integer value )
        {
            return value == 0;
        }
    };

    private static final Condition<Integer> IS_GREATER_THAN_ZERO = new Condition<Integer>()
    {
        public boolean isTrue( Integer value )
        {
            return value > 0;
        }
    };

    private void assertGotSuccessWithRetry( Condition<Integer> condition ) throws Exception
    {
        assertGotPingWithRetry( UdcTimerTask.successCounts, condition );
    }

    private void assertGotFailureWithRetry( Condition<Integer> condition ) throws Exception
    {
        assertGotPingWithRetry( UdcTimerTask.failureCounts, condition );
    }

    private void assertGotPingWithRetry( Map<String, Integer> counts, Condition<Integer> condition ) throws Exception
    {
        for ( int i = 0; i < 10; i++ )
        {
            Thread.sleep( 200 );
            Collection<Integer> countValues = counts.values();
            Integer count = countValues.iterator().next();
            if ( condition.isTrue( count ) )
            {
                return;
            }
        }
        fail();
    }

    private GraphDatabaseService createTempDatabase( Map<String, String> config ) throws IOException
    {
        String randomDbName = "tmpdb-" + rnd.nextInt();
        File possibleDirectory = new File( "target" + File.separator
                + randomDbName );
        if ( possibleDirectory.exists() )
        {
            FileUtils.deleteDirectory( possibleDirectory );
        }

        GraphDatabaseBuilder graphDatabaseBuilder = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( randomDbName );
        if ( config != null )
        {
            graphDatabaseBuilder.setConfig( config );
        }

        return graphDatabaseBuilder.newGraphDatabase();
    }

    private void destroy( GraphDatabaseService dbToDestroy ) throws IOException
    {
        dbToDestroy.shutdown();
        FileUtils.deleteDirectory( new File( ((GraphDatabaseSPI)dbToDestroy).getStoreDir() ) );
    }

}
