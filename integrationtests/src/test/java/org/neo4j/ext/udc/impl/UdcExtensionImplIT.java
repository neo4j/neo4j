/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.ext.udc.impl;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.localserver.LocalServerTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.concurrent.RecentK;
import org.neo4j.ext.udc.Edition;
import org.neo4j.ext.udc.UdcConstants;
import org.neo4j.ext.udc.UdcSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.mockito.matcher.RegexMatcher;
import org.neo4j.test.rule.TestDirectory;
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
public class UdcExtensionImplIT extends LocalServerTestBase
{
    private static final String VersionPattern = "(\\d\\.\\d+(([.-]).*)?)|(dev)";
    private static final Condition<Integer> IS_ZERO = value -> value == 0;
    private static final Condition<Integer> IS_GREATER_THAN_ZERO = value -> value > 0;

    @Rule
    public TestDirectory path = TestDirectory.testDirectory();

    private PingerHandler handler;
    private Map<String,String> config;
    private GraphDatabaseService graphdb;

    @Override
    @Before
    public void setUp() throws Exception
    {
        super.setUp();
        UdcTimerTask.successCounts.clear();
        UdcTimerTask.failureCounts.clear();
        handler = new PingerHandler();
        serverBootstrap.registerHandler( "/*", handler );
        HttpHost target = start();

        int servicePort = target.getPort();
        String serviceHostName = target.getHostName();
        String serverAddress = serviceHostName + ":" + servicePort;

        config = new HashMap<>();
        config.put( UdcSettings.first_delay.name(), "1000" );
        config.put( UdcSettings.udc_host.name(), serverAddress );
        config.put( OnlineBackupSettings.online_backup_enabled.name(), Settings.FALSE );

        blockUntilServerAvailable( new URL( "http", serviceHostName, servicePort, "/" ) );
    }

    @After
    public void cleanup() throws IOException
    {
        cleanup( graphdb );
    }

    /**
     * Expect the counts to be initialized.
     */
    @Test
    public void shouldLoadWhenNormalGraphdbIsCreated() throws Exception
    {
        // When
        Map<String, String> config = Collections.singletonMap( OnlineBackupSettings.online_backup_enabled.name(), Settings.FALSE );
        graphdb = createDatabase(config);

        // Then, when the UDC extension successfully loads, it initializes the attempts count to 0
        assertGotSuccessWithRetry( IS_ZERO );
    }

    /**
     * Expect separate counts for each graphdb.
     */
    @Test
    public void shouldLoadForEachCreatedGraphdb() throws IOException
    {
        Map<String, String> config = Collections.singletonMap( OnlineBackupSettings.online_backup_enabled.name(), Settings.FALSE );
        GraphDatabaseService graphdb1 = createDatabase( path.directory( "first-db" ), config );
        GraphDatabaseService graphdb2 = createDatabase( path.directory( "second-db" ), config );
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
                newEmbeddedDatabaseBuilder( path.directory( "should-record-failures" ) ).
                setConfig( UdcSettings.first_delay, "100" ).
                setConfig( UdcSettings.udc_host, "127.0.0.1:1" ).
                setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE ).
                newGraphDatabase();

        // Then
        assertGotFailureWithRetry( IS_GREATER_THAN_ZERO );
    }

    @Test
    public void shouldRecordSuccessesWhenThereIsAServer() throws Exception
    {
        // When
        graphdb = createDatabase( config );

        // Then
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        assertGotFailureWithRetry( IS_ZERO );
    }

    @Test
    public void shouldBeAbleToSpecifySourceWithConfig() throws Exception
    {
        // When
        config.put( UdcSettings.udc_source.name(), "unit-testing" );
        graphdb = createDatabase( config );

        // Then
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        assertEquals( "unit-testing", handler.getQueryMap().get( SOURCE ) );
    }

    @Test
    public void shouldRecordDatabaseMode() throws Exception
    {
        // When
        graphdb = createDatabase( config );

        // Then
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        assertEquals( "single", handler.getQueryMap().get( DATABASE_MODE ) );
    }

    @Test
    public void shouldRecordClusterName() throws Exception
    {
        // When
        graphdb = createDatabase( config );

        // Then
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );

        String hashOfDefaultClusterName = "1108231321";
        assertEquals( hashOfDefaultClusterName, handler.getQueryMap().get( CLUSTER_HASH ) );
    }

    private void blockUntilServerAvailable( final URL url ) throws Exception
    {
        final CountDownLatch latch = new CountDownLatch( 1 );
        final PointerTo<Boolean> flag = new PointerTo<>( false );

        Thread t = new Thread( () ->
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
        } );

        t.run();

        assertTrue( latch.await( 1000, TimeUnit.MILLISECONDS ) );

        t.join();
    }

    @Test
    public void shouldBeAbleToReadDefaultRegistration() throws Exception
    {
        // When
        graphdb = createDatabase( config );

        // Then
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        assertEquals( "unreg", handler.getQueryMap().get( REGISTRATION ) );
    }

    @Test
    public void shouldBeAbleToDetermineTestTagFromClasspath() throws Exception
    {
        // When
        graphdb = createDatabase( config );

        // Then
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        assertEquals( "test,web", handler.getQueryMap().get( TAGS ) );
    }

    @Test
    public void shouldBeAbleToDetermineEditionFromClasspath() throws Exception
    {
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
        // When
        graphdb = createDatabase( config );

        // Then
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        assertNotNull( handler.getQueryMap().get( MAC ) );
    }

    @Test
    public void shouldIncludePrefixedSystemProperties() throws Exception
    {
        withSystemProperty( UdcConstants.UDC_PROPERTY_PREFIX + ".test", "udc-property", () ->
        {
            withSystemProperty( "os.test", "os-property", () ->
            {
                graphdb = createDatabase( config );
                assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
                assertEquals( "udc-property", handler.getQueryMap().get( "test" ) );
                assertEquals( "os-property", handler.getQueryMap().get( "os.test" ) );
                return null;
            } );
            return null;
        } );
    }

    @Test
    public void shouldNotIncludeDistributionForWindows() throws Exception
    {
        withSystemProperty( "os.name", "Windows", () ->
        {
            graphdb = createDatabase( config );
            assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
            assertEquals( UdcConstants.UNKNOWN_DIST, handler.getQueryMap().get( "dist" ) );
            return null;
        } );
    }

    @Test
    public void shouldIncludeDistributionForLinux() throws Exception
    {
        if ( !SystemUtils.IS_OS_LINUX )
        {
            return;
        }
        graphdb = createDatabase( config );
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );

        assertEquals( DefaultUdcInformationCollector.searchForPackageSystems(), handler.getQueryMap().get( "dist" ) );
    }

    @Test
    public void shouldNotIncludeDistributionForMacOS() throws Exception
    {
        withSystemProperty( "os.name", "Mac OS X", () ->
        {
            graphdb = createDatabase( config );
            assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
            assertEquals( UdcConstants.UNKNOWN_DIST, handler.getQueryMap().get( "dist" ) );
            return null;
        } );
    }

    @Test
    public void shouldIncludeVersionInConfig() throws Exception
    {
        graphdb = createDatabase( config );
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        String version = handler.getQueryMap().get( VERSION );
        assertThat( version, new RegexMatcher( Pattern.compile( VersionPattern ) ) );
    }

    @Test
    public void shouldOverrideSourceWithSystemProperty() throws Exception
    {
        withSystemProperty( UdcSettings.udc_source.name(), "overridden", () ->
        {
            graphdb = createDatabase( path.directory( "db-with-property" ), config );
            assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
            String source = handler.getQueryMap().get( SOURCE );
            assertEquals( "overridden", source );
            return null;
        } );
    }

    @Test
    public void shouldMatchAllValidVersions()
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
    public void shouldFilterPlusBuildNumbers()
    {
        assertThat( DefaultUdcInformationCollector.filterVersionForUDC( "1.9.0-M01+00001" ),
                is( equalTo( "1.9.0-M01" ) ) );
    }

    @Test
    public void shouldNotFilterSnapshotBuildNumbers()
    {
        assertThat( DefaultUdcInformationCollector.filterVersionForUDC( "2.0-SNAPSHOT" ),
                is( equalTo( "2.0-SNAPSHOT" ) ) );

    }

    @Test
    public void shouldNotFilterReleaseBuildNumbers()
    {
        assertThat( DefaultUdcInformationCollector.filterVersionForUDC( "1.9" ), is( equalTo( "1.9" ) ) );
    }

    @Test
    public void shouldUseTheCustomConfiguration()
    {
        // Given
        config.put( UdcSettings.udc_source.name(), "my_source" );
        config.put( UdcSettings.udc_registration_key.name(), "my_key" );

        // When
        graphdb = createDatabase( config );

        // Then
        Config config = ((GraphDatabaseAPI) graphdb).getDependencyResolver().resolveDependency( Config.class );

        assertEquals( "my_source", config.get( UdcSettings.udc_source ) );
        assertEquals( "my_key", config.get( UdcSettings.udc_registration_key ) );
    }

    private interface Condition<T>
    {
        boolean isTrue( T value );
    }

    private void assertGotSuccessWithRetry( Condition<Integer> condition ) throws Exception
    {
        assertGotPingWithRetry( UdcTimerTask.successCounts, condition );
    }

    private void assertGotFailureWithRetry( Condition<Integer> condition ) throws Exception
    {
        assertGotPingWithRetry( UdcTimerTask.failureCounts, condition );
    }

    private void assertGotPingWithRetry( Map<String,Integer> counts, Condition<Integer> condition ) throws Exception
    {
        for ( int i = 0; i < 100; i++ )
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

    private GraphDatabaseService createDatabase()
    {
        return createDatabase( null, null );
    }

    private GraphDatabaseService createDatabase( Map<String,String> config )
    {
        return createDatabase( null, config );
    }

    private GraphDatabaseService createDatabase( File storeDir, Map<String,String> config )
    {
        TestEnterpriseGraphDatabaseFactory factory = new TestEnterpriseGraphDatabaseFactory();
        GraphDatabaseBuilder graphDatabaseBuilder =
                (storeDir != null) ? factory.newImpermanentDatabaseBuilder( storeDir )
                                   : factory.newImpermanentDatabaseBuilder();
        if ( config != null )
        {
            graphDatabaseBuilder.setConfig( config );
        }

        return graphDatabaseBuilder.newGraphDatabase();
    }

    private void cleanup( GraphDatabaseService gdb ) throws IOException
    {
        if ( gdb != null )
        {
            GraphDatabaseAPI db = (GraphDatabaseAPI) gdb;
            gdb.shutdown();
            FileUtils.deleteDirectory( db.getStoreDir() );
        }
    }

    private static class PointerTo<T>
    {
        private T value;

        PointerTo( T value )
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
                ((GraphDatabaseAPI) graphdb).getDependencyResolver().resolveDependency( UsageData.class )
                        .get( UsageDataKeys.clientNames );
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
