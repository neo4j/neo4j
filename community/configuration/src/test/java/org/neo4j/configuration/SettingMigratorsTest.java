/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.configuration;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.neo4j.configuration.GraphDatabaseSettings.LogQueryLevel;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpsConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.configuration.ssl.SslPolicyConfig;
import org.neo4j.configuration.ssl.SslPolicyScope;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.logging.AssertableLogProvider.inLog;

@TestDirectoryExtension
class SettingMigratorsTest
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void shouldRemoveAllowKeyGenerationFrom35ConfigFormat() throws Throwable
    {
        shouldRemoveAllowKeyGeneration( "dbms.ssl.policy.default.allow_key_generation", TRUE );
        shouldRemoveAllowKeyGeneration( "dbms.ssl.policy.default.allow_key_generation", FALSE );
    }

    @Test
    void shouldRemoveAllowKeyGeneration() throws Throwable
    {
        shouldRemoveAllowKeyGeneration( "dbms.ssl.policy.default.allow_key_generation", TRUE );
        shouldRemoveAllowKeyGeneration( "dbms.ssl.policy.default.allow_key_generation", FALSE );
    }

    @TestFactory
    Collection<DynamicTest> shouldMigrateSslPolicySettingToActualPolicyGroupName()
    {
        Collection<DynamicTest> tests = new ArrayList<>();
        Map<String,SslPolicyScope> sources = Map.of(
                "bolt.ssl_policy", SslPolicyScope.BOLT,
                "https.ssl_policy", SslPolicyScope.HTTPS,
                "dbms.backup.ssl_policy", SslPolicyScope.BACKUP,
                "causal_clustering.ssl_policy", SslPolicyScope.CLUSTER
        );
        sources.forEach( ( setting, source ) ->
        {
            tests.add( dynamicTest( String.format( "Test migration of SslPolicy for source %s", source.name() ), () ->
                    testMigrateSslPolicy( setting, SslPolicyConfig.forScope( source ) ) ) );
        } );

        return tests;
    }

    @Test
    void shouldWarnWhenUsingLegacySslPolicySettings()
    {
        Map<String,String> legacySettings = Map.of(
                "dbms.directories.certificates", "/cert/dir/",
                "unsupported.dbms.security.tls_certificate_file", "public.crt",
                "unsupported.dbms.security.tls_key_file", "private.key" );

        var config = Config.newBuilder().setRaw( legacySettings ).build();

        var logProvider = new AssertableLogProvider();
        config.setLogger( logProvider.getLog( Config.class ) );

        for ( String setting : legacySettings.keySet() )
        {
            logProvider.assertAtLeastOnce( inLog( Config.class ).warn("Use of deprecated setting %s. Legacy ssl policy is no longer supported.", setting ) );
        }

    }

    @Test
    void testDefaultDatabaseMigrator() throws IOException
    {
        File confFile = testDirectory.createFile( "test.conf" );
        Files.write( confFile.toPath(), List.of( "dbms.active_database=foo") );

        {
            Config config = Config.newBuilder()
                    .fromFile( confFile )
                    .build();
            Log log = mock( Log.class );
            config.setLogger( log );

            assertEquals( "foo", config.get( GraphDatabaseSettings.default_database ) );
            verify( log ).warn( "Use of deprecated setting %s. It is replaced by %s", "dbms.active_database", GraphDatabaseSettings.default_database.name() );
        }
        {
            Config config = Config.newBuilder()
                    .fromFile( confFile )
                    .set( GraphDatabaseSettings.default_database, "bar" )
                    .build();
            Log log = mock( Log.class );
            config.setLogger( log );

            assertEquals( "bar", config.get( GraphDatabaseSettings.default_database ) );
            verify( log ).warn( "Use of deprecated setting %s. It is replaced by %s", "dbms.active_database", GraphDatabaseSettings.default_database.name() );
        }
    }

    @Test
    void testConnectorOldFormatMigration() throws IOException
    {
        File confFile = testDirectory.createFile( "test.conf" );
        Files.write( confFile.toPath(), Arrays.asList(
                "dbms.connector.bolt.enabled=true",
                "dbms.connector.bolt.type=BOLT",
                "dbms.connector.http.enabled=true",
                "dbms.connector.https.enabled=true",
                "dbms.connector.bolt2.type=bolt",
                "dbms.connector.bolt2.listen_address=:1234" ) );

        Config config = Config.newBuilder().fromFile( confFile ).build();
        var logProvider = new AssertableLogProvider();
        config.setLogger( logProvider.getLog( Config.class ) );

        assertTrue( config.get( BoltConnector.enabled ) );
        assertTrue( config.get( HttpConnector.enabled ) );
        assertTrue( config.get( HttpsConnector.enabled ) );

        logProvider.assertAtLeastOnce( inLog( Config.class ).warn( "Use of deprecated setting %s. Type is no longer required", "dbms.connector.bolt.type" ) );
        logProvider.assertAtLeastOnce( inLog( Config.class ).warn( "Use of deprecated setting %s. No longer supports multiple connectors. Setting discarded.",
                "dbms.connector.bolt2.type" ) );
        logProvider.assertAtLeastOnce( inLog( Config.class ).warn( "Use of deprecated setting %s. No longer supports multiple connectors. Setting discarded.",
                "dbms.connector.bolt2.listen_address" ) );
    }

    @Test
    void testKillQueryVerbose() throws IOException
    {
        File confFile = testDirectory.createFile( "test.conf" );
        Files.write( confFile.toPath(), List.of( "dbms.procedures.kill_query_verbose=false" ) );

        Config config = Config.newBuilder().fromFile( confFile ).build();
        var logProvider = new AssertableLogProvider();
        config.setLogger( logProvider.getLog( Config.class ) );

        logProvider.assertAtLeastOnce( inLog( Config.class )
                .warn( "Setting %s is removed. It's no longer possible to disable verbose kill query logging.", "dbms.procedures.kill_query_verbose" ) );
    }

    @Test
    void testDefaultSchemaProvider() throws IOException
    {
        Map<String,String> migrationMap = Map.of(
                "lucene-1.0", "lucene+native-3.0",
                "lucene+native-1.0", "lucene+native-3.0",
                "lucene+native-2.0", "native-btree-1.0",
                "native-btree-1.0", "native-btree-1.0" );
        for ( String oldSchemaProvider : migrationMap.keySet() )
        {
            File confFile = testDirectory.createFile( "test.conf" );
            Files.write( confFile.toPath(), List.of( "dbms.index.default_schema_provider=" + oldSchemaProvider ) );

            Config config = Config.newBuilder().fromFile( confFile ).build();
            var logProvider = new AssertableLogProvider();
            config.setLogger( logProvider.getLog( Config.class ) );

            String expectedWarning = "Use of deprecated setting dbms.index.default_schema_provider.";
            if ( !"native-btree-1.0".equals( oldSchemaProvider ) )
            {
                expectedWarning += " Value migrated from " + oldSchemaProvider + " to " + migrationMap.get( oldSchemaProvider ) + ".";
            }
            logProvider.assertAtLeastOnce( inLog( Config.class ).warn( expectedWarning ) );
        }
    }

    @TestFactory
    Collection<DynamicTest> testConnectorAddressMigration()
    {
        Collection<DynamicTest> tests = new ArrayList<>();
        tests.add( dynamicTest( "Test bolt connector address migration",
                () -> testAddrMigration( BoltConnector.listen_address, BoltConnector.advertised_address ) ) );
        tests.add( dynamicTest( "Test http connector address migration",
                () -> testAddrMigration( HttpConnector.listen_address, HttpConnector.advertised_address ) ) );
        tests.add( dynamicTest( "Test https connector address migration",
                () -> testAddrMigration( HttpsConnector.listen_address, HttpsConnector.advertised_address ) ) );
        return tests;
    }

    @TestFactory
    Collection<DynamicTest> testQueryLogMigration()
    {
        Collection<DynamicTest> tests = new ArrayList<>();
        tests.add( dynamicTest( "Test query log migration, disabled", () -> testQueryLogMigration( false, LogQueryLevel.OFF ) ) );
        tests.add( dynamicTest( "Test query log migration, enabled", () -> testQueryLogMigration( true, LogQueryLevel.INFO ) ) );
        return tests;
    }

    private static void testQueryLogMigration( Boolean oldValue, LogQueryLevel newValue )
    {
        var setting = GraphDatabaseSettings.log_queries;
        Config config = Config.newBuilder().setRaw( Map.of( setting.name(), oldValue.toString() ) ).build();

        var logProvider = new AssertableLogProvider();
        config.setLogger( logProvider.getLog( Config.class ) );

        assertEquals( newValue, config.get( setting ) );

        String msg = "Use of deprecated setting value %s=%s. It is replaced by %s=%s";
        logProvider.assertAtLeastOnce( inLog( Config.class ).warn( msg, setting.name(), oldValue.toString(), setting.name(), newValue.name() ) );
    }

    private static void testAddrMigration( Setting<SocketAddress> listenAddr, Setting<SocketAddress> advertisedAddr )
    {
        Config config1 = Config.newBuilder().setRaw( Map.of( listenAddr.name(), "foo:111" ) ).build();
        Config config2 = Config.newBuilder().setRaw( Map.of( listenAddr.name(), ":222" ) ).build();
        Config config3 = Config.newBuilder().setRaw( Map.of( listenAddr.name(), ":333", advertisedAddr.name(), "bar" ) ).build();
        Config config4 = Config.newBuilder().setRaw( Map.of( listenAddr.name(), "foo:444", advertisedAddr.name(), ":555" ) ).build();
        Config config5 = Config.newBuilder().setRaw( Map.of( listenAddr.name(), "foo", advertisedAddr.name(), "bar" ) ).build();
        Config config6 = Config.newBuilder().setRaw( Map.of( listenAddr.name(), "foo:666", advertisedAddr.name(), "bar:777" ) ).build();

        var logProvider = new AssertableLogProvider();
        config1.setLogger( logProvider.getLog( Config.class ) );
        config2.setLogger( logProvider.getLog( Config.class ) );
        config3.setLogger( logProvider.getLog( Config.class ) );
        config4.setLogger( logProvider.getLog( Config.class ) );
        config5.setLogger( logProvider.getLog( Config.class ) );
        config6.setLogger( logProvider.getLog( Config.class ) );

        assertEquals( new SocketAddress( "localhost", 111 ), config1.get( advertisedAddr ) );
        assertEquals( new SocketAddress( "localhost", 222 ), config2.get( advertisedAddr ) );
        assertEquals( new SocketAddress( "bar", 333 ), config3.get( advertisedAddr ) );
        assertEquals( new SocketAddress( "localhost", 555 ), config4.get( advertisedAddr ) );
        assertEquals( new SocketAddress( "bar", advertisedAddr.defaultValue().getPort() ), config5.get( advertisedAddr ) );
        assertEquals( new SocketAddress( "bar", 777 ), config6.get( advertisedAddr ) );

        String msg = "Use of deprecated setting port propagation. port %s is migrated from %s to %s.";

        logProvider.assertAtLeastOnce( inLog( Config.class ).warn( msg, 111, listenAddr.name(), advertisedAddr.name() ) );
        logProvider.assertAtLeastOnce( inLog( Config.class ).warn( msg, 222, listenAddr.name(), advertisedAddr.name() ) );
        logProvider.assertAtLeastOnce( inLog( Config.class ).warn( msg, 333, listenAddr.name(), advertisedAddr.name() ) );

        logProvider.assertNone( inLog( Config.class ).warn( msg, 444, listenAddr.name(), advertisedAddr.name() ) );
        logProvider.assertNone( inLog( Config.class ).warn( msg, 555, listenAddr.name(), advertisedAddr.name() ) );
        logProvider.assertNone( inLog( Config.class ).warn( msg, 666, listenAddr.name(), advertisedAddr.name() ) );
    }

    private static void testMigrateSslPolicy( String oldGroupnameSetting, SslPolicyConfig policyConfig )
    {
        String oldFormatSetting = "dbms.ssl.policy.foo.trust_all";
        var config = Config.newBuilder().setRaw( Map.of( oldGroupnameSetting, "foo", oldFormatSetting, "true" ) ).build();

        var logProvider = new AssertableLogProvider();
        config.setLogger( logProvider.getLog( Config.class ) );

        assertTrue( config.get( policyConfig.trust_all ) );

        logProvider.assertAtLeastOnce( inLog( Config.class ).warn( "Use of deprecated setting %s.", oldGroupnameSetting ) );
        logProvider.assertAtLeastOnce( inLog( Config.class )
                .warn( "Use of deprecated setting %s. It is replaced by %s", oldFormatSetting, policyConfig.trust_all.name() ) );
    }

    private void shouldRemoveAllowKeyGeneration( String toRemove, String value )
    {
        var config = Config.newBuilder().setRaw( Map.of( toRemove, value ) ).build();

        var logProvider = new AssertableLogProvider();
        config.setLogger( logProvider.getLog( Config.class ) );

        assertThrows( IllegalArgumentException.class, () -> config.getSetting( toRemove ) );

        logProvider.assertAtLeastOnce( inLog( Config.class ).warn( "Setting %s is removed. A valid key and certificate are required " +
                "to be present in the key and certificate path configured in this ssl policy.", toRemove ) );
    }
}
