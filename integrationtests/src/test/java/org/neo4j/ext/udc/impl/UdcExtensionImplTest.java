/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.sun.jersey.spi.container.ContainerRequest;
import org.apache.commons.io.FileUtils;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.localserver.LocalTestServer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.ext.udc.Edition;
import org.neo4j.ext.udc.UdcConstants;
import org.neo4j.ext.udc.UdcSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.server.rest.web.CollectUserAgentFilter;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.stub;

import static org.neo4j.ext.udc.UdcConstants.CLUSTER_HASH;
import static org.neo4j.ext.udc.UdcConstants.DATABASE_MODE;
import static org.neo4j.ext.udc.UdcConstants.EDITION;
import static org.neo4j.ext.udc.UdcConstants.MAC;
import static org.neo4j.ext.udc.UdcConstants.REGISTRATION;
import static org.neo4j.ext.udc.UdcConstants.SOURCE;
import static org.neo4j.ext.udc.UdcConstants.TAGS;
import static org.neo4j.ext.udc.UdcConstants.USER_AGENTS;
import static org.neo4j.ext.udc.UdcConstants.VERSION;

/**
 * Unit testing for the UDC kernel extension.
 * <p>
 * The UdcExtensionImpl is loaded when a new
 * GraphDatabase is instantiated, as part of
 * {@link org.neo4j.helpers.Service#load}.
 */
public class UdcExtensionImplTest
{
    private static final String VersionPattern = "\\d\\.\\d+((\\.|\\-).*)?";

    @Rule
    public TargetDirectory.TestDirectory path = TargetDirectory.testDirForTest( getClass() );

    private PingerHandler handler;
    private Map<String, String> config;

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
        GraphDatabaseService graphdb = createDatabase( null );
        destroy( graphdb );
    }

    /**
     * Expect the counts to be initialized.
     */
    @Test
    public void shouldLoadWhenNormalGraphdbIsCreated() throws Exception
    {
        GraphDatabaseService graphdb = createDatabase( null );
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
        GraphDatabaseService graphdb1 = createDatabase( null );
        GraphDatabaseService graphdb2 = createDatabase( null );
        Set<String> successCountValues = UdcTimerTask.successCounts.keySet();
        assertThat( successCountValues.size(), equalTo( 2 ) );
        assertThat( "this", is( not( "that" ) ) );
        destroy( graphdb1 );
        destroy( graphdb2 );
    }

    @Test
    public void shouldRecordFailuresWhenThereIsNoServer() throws Exception
    {
        GraphDatabaseService graphdb = new TestGraphDatabaseFactory().
                newEmbeddedDatabaseBuilder( path.directory( "should-record-failures" ).getPath() ).
                loadPropertiesFromURL( getClass().getResource( "/org/neo4j/ext/udc/udc.properties" ) ).
                setConfig( UdcSettings.first_delay, "100" ).
                setConfig( UdcSettings.udc_host, "127.0.0.1:1" ).
                newGraphDatabase();
        assertGotFailureWithRetry( IS_GREATER_THAN_ZERO );
        destroy( graphdb );
    }

    @Test
    public void shouldRecordSuccessesWhenThereIsAServer() throws Exception
    {
        setupServer();
        GraphDatabaseService graphdb = createDatabase( config );
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        assertGotFailureWithRetry( IS_ZERO );
        destroy( graphdb );
    }

    @Test
    public void shouldBeAbleToSpecifySourceWithConfig() throws Exception
    {
        setupServer();

        GraphDatabaseService graphdb = createDatabase( config );
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        assertEquals( "unit-testing", handler.getQueryMap().get( SOURCE ) );

        destroy( graphdb );
    }

    @Test
    public void shouldRecordDatabaseMode() throws Exception
    {
        setupServer();

        GraphDatabaseService graphdb = createDatabase( config );
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        assertEquals( "single", handler.getQueryMap().get( DATABASE_MODE ) );

        destroy( graphdb );
    }

    @Test
    public void shouldRecordClusterName() throws Exception
    {
        setupServer();

        GraphDatabaseService graphdb = createDatabase( config );
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );

        String hashOfDefaultClusterName = "1108231321";
        assertEquals( hashOfDefaultClusterName, handler.getQueryMap().get( CLUSTER_HASH ) );

        destroy( graphdb );
    }

    private void setupServer() throws Exception
    {
        // first, set up the test server
        LocalTestServer server = new LocalTestServer( null, null );
        handler = new PingerHandler();
        server.register( "/*", handler );
        server.start();

        int servicePort = server.getServiceAddress().getPort();
        String serviceHostName = server.getServiceAddress().getHostName();
        String serverAddress = serviceHostName + ":" + servicePort;

        config = new HashMap<>();
        config.put( UdcSettings.first_delay.name(), "100" );
        config.put( UdcSettings.udc_host.name(), serverAddress );

        blockUntilServerAvailable( new URL( "http", serviceHostName, servicePort, "/" ) );
    }

    private void blockUntilServerAvailable( final URL url ) throws Exception
    {
        final CountDownLatch latch = new CountDownLatch( 1 );
        final PointerTo<Boolean> flag = new PointerTo<>( false );

        Thread t = new Thread( new Runnable()
        {
            @Override
            public void run()
            {
                while ( !flag.getValue() )
                {
                    try
                    {
                        HttpGet httpget = new HttpGet( url.toURI() );
                        httpget.addHeader( "Accept", "application/json" );
                        DefaultHttpClient client = new DefaultHttpClient();
                        client.execute( httpget );

                        // If we get here, the server's ready
                        flag.setValue( true );
                        latch.countDown();
                    }
                    catch ( Exception e )
                    {
                        throw new RuntimeException( e );
                    }
                }
            }
        } );


        t.run();

        latch.await( 1000, TimeUnit.MILLISECONDS );

        t.join();
    }

    @Test
    public void shouldNotBeAbleToSpecifyRegistrationIdWithConfig() throws Exception
    {

        setupServer();

        config.put( UdcSettings.udc_registration_key.name(), "marketoid" );

        GraphDatabaseService graphdb = createDatabase( config );
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        assertEquals( "test-reg", handler.getQueryMap().get( REGISTRATION ) );

        destroy( graphdb );
    }

    @Test
    public void shouldBeAbleToReadDefaultRegistration() throws Exception
    {
        setupServer();

        GraphDatabaseService graphdb = createDatabase( config );
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        assertEquals( "test-reg", handler.getQueryMap().get( REGISTRATION ) );

        destroy( graphdb );
    }

    @Test
    public void shouldBeAbleToDetermineTestTagFromClasspath() throws Exception
    {
        setupServer();

        GraphDatabaseService graphdb = createDatabase( config );
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        assertEquals( "test,web", handler.getQueryMap().get( TAGS ) );

        destroy( graphdb );
    }

    @Test
    public void shouldBeAbleToDetermineEditionFromClasspath() throws Exception
    {
        setupServer();
        GraphDatabaseService graphdb = createDatabase( config );
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        assertEquals( Edition.enterprise.name(), handler.getQueryMap().get( EDITION ) );

        destroy( graphdb );
    }

    @Test
    public void shouldBeAbleToDetermineUserAgent() throws Exception
    {
        makeRequestWithAgent( "test/1.0" );
        setupServer();
        GraphDatabaseService graphdb = createDatabase( config );
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        assertEquals( "test/1.0", handler.getQueryMap().get( USER_AGENTS ) );


        destroy( graphdb );
    }

    @Test
    public void shouldBeAbleToDetermineUserAgents() throws Exception
    {
        makeRequestWithAgent( "test/1.0" );
        makeRequestWithAgent( "foo/bar" );
        setupServer();
        GraphDatabaseService graphdb = createDatabase( config );
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        String userAgents = handler.getQueryMap().get( USER_AGENTS );
        assertEquals( true, userAgents.contains( "test/1.0" ) );
        assertEquals( true, userAgents.contains( "foo/bar" ) );


        destroy( graphdb );
    }

    @Test
    public void shouldUpdateTheUserAgentsPerPing() throws Exception
    {
        makeRequestWithAgent( "test/1.0" );
        setupServer();
        config.put( UdcSettings.interval.name(), "1000" );
        GraphDatabaseService graphdb = createDatabase( config );
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        String userAgents = handler.getQueryMap().get( USER_AGENTS );
        assertEquals( true, userAgents.contains( "test/1.0" ) );

        makeRequestWithAgent( "foo/bar" );

        Thread.sleep( 1000 );
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );

        userAgents = handler.getQueryMap().get( USER_AGENTS );
        assertEquals( true, userAgents.contains( "foo/bar" ) );
        assertEquals( false, userAgents.contains( "test/1.0" ) );

        destroy( graphdb );
    }

    @Test
    public void shouldIncludeMacAddressInConfig() throws Exception
    {
        setupServer();

        GraphDatabaseService graphdb = createDatabase( config );
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        assertNotNull( handler.getQueryMap().get( MAC ) );

        destroy( graphdb );
    }

    @Test
    public void shouldIncludePrefixedSystemProperties() throws Exception
    {
        setupServer();
        withSystemProperty( UdcConstants.UDC_PROPERTY_PREFIX + ".test", "udc-property", new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                withSystemProperty( "os.test", "os-property", new Callable<Void>()
                {
                    @Override
                    public Void call() throws Exception
                    {
                        GraphDatabaseService graphdb = createDatabase( config );
                        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
                        assertEquals( "udc-property", handler.getQueryMap().get( "test" ) );
                        assertEquals( "os-property", handler.getQueryMap().get( "os.test" ) );

                        destroy( graphdb );
                        return null;
                    }
                } );
                return null;
            }
        } );
    }

    @Test
    public void shouldNotIncludeDistributionForWindows() throws Exception
    {
        setupServer();
        withSystemProperty( "os.name", "Windows", new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                GraphDatabaseService graphdb = createDatabase( config );
                assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
                assertEquals( UdcConstants.UNKNOWN_DIST, handler.getQueryMap().get( "dist" ) );

                destroy( graphdb );
                return null;
            }
        } );
    }

    @Test
    public void shouldIncludeDistributionForLinux() throws Exception
    {
        if ( !System.getProperty( "os.name" ).equals( "Linux" ) )
        {
            return;
        }
        setupServer();
        GraphDatabaseService graphdb = createDatabase( config );
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );

        assertEquals( DefaultUdcInformationCollector.searchForPackageSystems(), handler.getQueryMap().get( "dist" ) );

        destroy( graphdb );
    }

    @Test
    public void shouldNotIncludeDistributionForMacOS() throws Exception
    {
        setupServer();
        withSystemProperty( "os.name", "Mac OS X", new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                GraphDatabaseService graphdb = createDatabase( config );
                assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
                assertEquals( UdcConstants.UNKNOWN_DIST, handler.getQueryMap().get( "dist" ) );

                destroy( graphdb );
                return null;
            }
        } );
    }

    @Test
    public void shouldIncludeVersionInConfig() throws Exception
    {
        setupServer();

        GraphDatabaseService graphdb = createDatabase( config );
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        String version = handler.getQueryMap().get( VERSION );
        assertTrue( version.matches( VersionPattern ) );

        destroy( graphdb );
    }

    @Test
    public void shouldReadSourceFromJar() throws Exception
    {
        setupServer();

        GraphDatabaseService graphdb = createDatabase( config );
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        String source = handler.getQueryMap().get( SOURCE );
        assertEquals( "unit-testing", source );

        destroy( graphdb );
    }

    @Test
    public void shouldOverrideSourceWithSystemProperty() throws Exception
    {
        setupServer();

        withSystemProperty( UdcSettings.udc_source.name(), "overridden", new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                GraphDatabaseService graphdb = createDatabase( config );
                assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
                String source = handler.getQueryMap().get( SOURCE );
                assertEquals( "overridden", source );
                destroy( graphdb );
                return null;
            }
        } );
    }

    @Test
    public void shouldMatchAllValidVersions() throws Exception
    {
        assertTrue( "1.8.M07".matches( VersionPattern ) );
        assertTrue( "1.8.RC1".matches( VersionPattern ) );
        assertTrue( "1.8.GA".matches( VersionPattern ) );
        assertTrue( "1.8".matches( VersionPattern ) );
        assertTrue( "1.9".matches( VersionPattern ) );
        assertTrue( "1.9-SNAPSHOT".matches( VersionPattern ) );
        assertTrue( "2.0-SNAPSHOT".matches( VersionPattern ) );
        assertTrue( "1.9.M01".matches( VersionPattern ) );
        assertTrue( "1.10".matches( VersionPattern ) );
        assertTrue( "1.10-SNAPSHOT".matches( VersionPattern ) );
        assertTrue( "1.10.M01".matches( VersionPattern ) );
    }

    @Test
    public void testUdcPropertyFileKeysMatchSettings() throws Exception
    {
        Map<String, String> config = MapUtil.load( getClass().getResourceAsStream( "/org/neo4j/ext/udc/udc" +
                ".properties" ) );
        assertEquals( "test-reg", config.get( UdcSettings.udc_registration_key.name() ) );
        assertEquals( "unit-testing", config.get( UdcSettings.udc_source.name() ) );
    }

    @Test
    public void shouldFilterPlusBuildNumbers() throws Exception
    {
        assertThat( DefaultUdcInformationCollector.filterVersionForUDC( "1.9.0-M01+00001" ),
                is( equalTo( "1.9.0-M01" ) ) );
    }

    @Test
    public void shouldNotFilterSnapshotBuildNumbers() throws Exception
    {
        assertThat( DefaultUdcInformationCollector.filterVersionForUDC( "2.0-SNAPSHOT" ),
                is( equalTo( "2.0-SNAPSHOT" ) ) );

    }

    @Test
    public void shouldNotFilterReleaseBuildNumbers() throws Exception
    {
        assertThat( DefaultUdcInformationCollector.filterVersionForUDC( "1.9" ),
                is( equalTo( "1.9" ) ) );
    }

    private static interface Condition<T>
    {
        boolean isTrue( T value );
    }

    private static final Condition<Integer> IS_ZERO = new Condition<Integer>()
    {
        @Override
        public boolean isTrue( Integer value )
        {
            return value == 0;
        }
    };

    private static final Condition<Integer> IS_GREATER_THAN_ZERO = new Condition<Integer>()
    {
        @Override
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
        for ( int i = 0; i < 50; i++ )
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

    private GraphDatabaseService createDatabase( Map<String, String> config ) throws IOException
    {
        GraphDatabaseBuilder graphDatabaseBuilder = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder();
        graphDatabaseBuilder.loadPropertiesFromURL( getClass().getResource( "/org/neo4j/ext/udc/udc.properties" ) );

        if ( config != null )
        {
            graphDatabaseBuilder.setConfig( config );
        }

        return graphDatabaseBuilder.newGraphDatabase();
    }

    private void destroy( GraphDatabaseService dbToDestroy ) throws IOException
    {
        @SuppressWarnings("deprecation") InternalAbstractGraphDatabase db = (InternalAbstractGraphDatabase) dbToDestroy;
        dbToDestroy.shutdown();
        FileUtils.deleteDirectory( new File( db.getStoreDir() ) );
    }

    private static class PointerTo<T>
    {
        private T value;

        public PointerTo( T value )
        {
            this.value = value;
        }

        public T getValue()
        {
            return value;
        }

        public void setValue( T value )
        {
            this.value = value;
        }
    }

    private ContainerRequest makeRequestWithAgent( String agent )
    {
        return CollectUserAgentFilter.instance().filter( request( agent ) );
    }

    private static ContainerRequest request( String... userAgent )
    {
        ContainerRequest request = mock( ContainerRequest.class );
        List<String> headers = Arrays.asList( userAgent );
        stub( request.getRequestHeader( "User-Agent" ) ).toReturn( headers );
        return request;
    }

    private void withSystemProperty( String name, String value, Callable<Void> block ) throws Exception
    {
        String original = System.getProperty( name );
        System.setProperty( name, value );
        try
        {
            block.call();
        }
        finally
        {
            if ( original == null )
            {
                System.clearProperty( name );
            }
            else
            {
                System.setProperty( name, original );
            }
        }
    }
}
