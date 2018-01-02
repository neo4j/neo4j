/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.ext.udc.impl;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.localserver.LocalTestServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.neo4j.concurrent.RecentK;
import org.neo4j.ext.udc.Edition;
import org.neo4j.ext.udc.UdcConstants;
import org.neo4j.ext.udc.UdcSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.test.RegexMatcher;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.udc.UsageData;
import org.neo4j.udc.UsageDataKeys;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
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
    private static final String VersionPattern = "(\\d\\.\\d+((\\.|\\-).*)?)|(dev)";

    @Rule
    public TargetDirectory.TestDirectory path = TargetDirectory.testDirForTest( getClass() );

    private PingerHandler handler;
    private Map<String, String> config;
    private GraphDatabaseService graphdb;

    @Before
    public void resetUdcState()
    {
        UdcTimerTask.successCounts.clear();
        UdcTimerTask.failureCounts.clear();
    }

    /**
     * Expect the counts to be initialized.
     */
    @Test
    public void shouldLoadWhenNormalGraphdbIsCreated() throws Exception
    {
        // When
        graphdb = createDatabase( null );

        // Then, when the UDC extension successfully loads, it initializes the attempts count to 0
        assertGotSuccessWithRetry( IS_ZERO );
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
        cleanup( graphdb1 );
        cleanup( graphdb2 );
    }

    @Test
    public void shouldRecordFailuresWhenThereIsNoServer() throws Exception
    {
        // When
        graphdb = new TestGraphDatabaseFactory().
                newEmbeddedDatabaseBuilder( path.directory( "should-record-failures" ).getPath() ).
                loadPropertiesFromURL( getClass().getResource( "/org/neo4j/ext/udc/udc.properties" ) ).
                setConfig( UdcSettings.first_delay, "100" ).
                setConfig( UdcSettings.udc_host, "127.0.0.1:1" ).
                newGraphDatabase();

        // Then
        assertGotFailureWithRetry( IS_GREATER_THAN_ZERO );
    }

    @Test
    public void shouldRecordSuccessesWhenThereIsAServer() throws Exception
    {
        // Given
        setupServer();

        // When
        graphdb = createDatabase( config );

        // Then
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        assertGotFailureWithRetry( IS_ZERO );
    }

    @Test
    public void shouldBeAbleToSpecifySourceWithConfig() throws Exception
    {
        // Given
        setupServer();

        // When
        graphdb = createDatabase( config );

        // Then
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        assertEquals( "unit-testing", handler.getQueryMap().get( SOURCE ) );
    }

    @Test
    public void shouldRecordDatabaseMode() throws Exception
    {
        // Given
        setupServer();

        // When
        graphdb = createDatabase( config );

        // Then
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        assertEquals( "single", handler.getQueryMap().get( DATABASE_MODE ) );
    }

    @Test
    public void shouldRecordClusterName() throws Exception
    {
        // Given
        setupServer();

        // When
        graphdb = createDatabase( config );

        // Then
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );

        String hashOfDefaultClusterName = "1108231321";
        assertEquals( hashOfDefaultClusterName, handler.getQueryMap().get( CLUSTER_HASH ) );
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

        assertTrue( latch.await( 1000, TimeUnit.MILLISECONDS ) );

        t.join();
    }

    @Test
    public void shouldNotBeAbleToSpecifyRegistrationIdWithConfig() throws Exception
    {
        // Given
        setupServer();
        config.put( UdcSettings.udc_registration_key.name(), "marketoid" );

        // When
        graphdb = createDatabase( config );

        // Then
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        assertEquals( "test-reg", handler.getQueryMap().get( REGISTRATION ) );
    }

    @Test
    public void shouldBeAbleToReadDefaultRegistration() throws Exception
    {
        // Given
        setupServer();

        // When
        graphdb = createDatabase( config );

        // Then
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        assertEquals( "test-reg", handler.getQueryMap().get( REGISTRATION ) );
    }

    @Test
    public void shouldBeAbleToDetermineTestTagFromClasspath() throws Exception
    {
        // Given
        setupServer();

        // When
        graphdb = createDatabase( config );

        // Then
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        assertEquals( "test,web", handler.getQueryMap().get( TAGS ) );
    }

    @Test
    public void shouldBeAbleToDetermineEditionFromClasspath() throws Exception
    {
        // Given
        setupServer();

        // When
        graphdb = createDatabase( config );

        // Then
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        assertEquals( Edition.enterprise.name(), handler.getQueryMap().get( EDITION ) );
    }

    @Test
    public void shouldBeAbleToDetermineUserAgent() throws Exception
    {
        // Given
        setupServer();
        graphdb = createDatabase( config );

        // When
        makeRequestWithAgent( "test/1.0" );

        // Then
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        assertEquals( "test/1.0", handler.getQueryMap().get( USER_AGENTS ) );
    }

    @Test
    public void shouldBeAbleToDetermineUserAgents() throws Exception
    {
        // Given
        setupServer();
        graphdb = createDatabase( config );

        // When
        makeRequestWithAgent( "test/1.0" );
        makeRequestWithAgent( "foo/bar" );

        // Then
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        String userAgents = handler.getQueryMap().get( USER_AGENTS );
        assertEquals( true, userAgents.contains( "test/1.0" ) );
        assertEquals( true, userAgents.contains( "foo/bar" ) );
    }

    @Test
    public void shouldIncludeMacAddressInConfig() throws Exception
    {
        // Given
        setupServer();

        // When
        graphdb = createDatabase( config );

        // Then
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        assertNotNull( handler.getQueryMap().get( MAC ) );
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
                        graphdb = createDatabase( config );
                        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
                        assertEquals( "udc-property", handler.getQueryMap().get( "test" ) );
                        assertEquals( "os-property", handler.getQueryMap().get( "os.test" ) );
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
                graphdb = createDatabase( config );
                assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
                assertEquals( UdcConstants.UNKNOWN_DIST, handler.getQueryMap().get( "dist" ) );
                return null;
            }
        } );
    }

    @Test
    public void shouldIncludeDistributionForLinux() throws Exception
    {
        if ( !SystemUtils.IS_OS_LINUX )
        {
            return;
        }
        setupServer();
        graphdb = createDatabase( config );
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );

        assertEquals( DefaultUdcInformationCollector.searchForPackageSystems(), handler.getQueryMap().get( "dist" ) );
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
                graphdb = createDatabase( config );
                assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
                assertEquals( UdcConstants.UNKNOWN_DIST, handler.getQueryMap().get( "dist" ) );
                return null;
            }
        } );
    }

    @Test
    public void shouldIncludeVersionInConfig() throws Exception
    {
        setupServer();

        graphdb = createDatabase( config );
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        String version = handler.getQueryMap().get( VERSION );
        assertThat( version, new RegexMatcher( Pattern.compile( VersionPattern ) ) );
    }

    @Test
    public void shouldReadSourceFromJar() throws Exception
    {
        setupServer();

        graphdb = createDatabase( config );
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        String source = handler.getQueryMap().get( SOURCE );
        assertEquals( "unit-testing", source );
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
                graphdb = createDatabase( config );
                assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
                String source = handler.getQueryMap().get( SOURCE );
                assertEquals( "overridden", source );
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

    @After
    public void cleanup() throws IOException
    {
        cleanup( graphdb );
    }

    private void cleanup( GraphDatabaseService gdb ) throws IOException
    {
        if(gdb != null)
        {
            @SuppressWarnings( "deprecation" ) GraphDatabaseAPI db = (GraphDatabaseAPI) gdb;
            gdb.shutdown();
            FileUtils.deleteDirectory( new File( db.getStoreDir() ) );
        }
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

    private void makeRequestWithAgent( String agent )
    {
        RecentK<String> clients =
                ((GraphDatabaseAPI) graphdb).getDependencyResolver().resolveDependency( UsageData.class ).get(
                        UsageDataKeys.clientNames );
        clients.add( agent );
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
