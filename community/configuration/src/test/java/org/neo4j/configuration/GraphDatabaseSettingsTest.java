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

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.Connector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpsConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.logging.AssertableLogProvider;

import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.default_advertised_address;
import static org.neo4j.configuration.GraphDatabaseSettings.default_listen_address;
import static org.neo4j.configuration.GraphDatabaseSettings.keep_logical_logs;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_sampling_percentage;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_tracing_level;
import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.internal.helpers.collection.MapUtil.stringMap;
import static org.neo4j.logging.AssertableLogProvider.inLog;

class GraphDatabaseSettingsTest
{
    @Test
    void mustHaveNullDefaultPageCacheMemorySizeInBytes()
    {
        String bytes = Config.defaults().get( GraphDatabaseSettings.pagecache_memory );
        assertThat( bytes, is( nullValue() ) );
    }

    @Test
    void pageCacheSettingMustAcceptArbitraryUserSpecifiedValue()
    {
        Setting<String> setting = GraphDatabaseSettings.pagecache_memory;
        assertThat( Config.defaults( setting, "245760" ).get( setting ), is( "245760" ) );
        assertThat( Config.defaults( setting, "2244g" ).get( setting ), is( "2244g" ) );
        assertThat( Config.defaults( setting, "string" ).get( setting ), is( "string" ) );
    }

    @Test
    void noDuplicateSettingsAreAllowed() throws Exception
    {
        final HashMap<String,String> fields = new HashMap<>();
        for ( Field field : GraphDatabaseSettings.class.getDeclaredFields() )
        {
            if ( field.getType() == Setting.class )
            {
                Setting<?> setting = (Setting<?>) field.get( null );

                assertFalse( fields.containsKey( setting.name() ),
                        format( "'%s' in %s has already been defined in %s", setting.name(), field.getName(), fields.get( setting.name() ) ) );
                fields.put( setting.name(), field.getName() );
            }
        }
    }

    @Test
    void shouldEnableBoltByDefault()
    {
        // given
        Config config = Config.defaults( GraphDatabaseSettings.SERVER_DEFAULTS );

        // when
        BoltConnector boltConnector = config.getGroups( BoltConnector.class ).values().stream().findFirst().orElseThrow();
        SocketAddress listenSocketAddress = config.get( boltConnector.listen_address );

        // then
        assertEquals( new SocketAddress( "localhost", 7687 ), listenSocketAddress );
    }

    @Test
    void shouldBeAbleToDisableBoltConnectorWithJustOneParameter()
    {
        // given
        Config config = Config.defaults( BoltConnector.group( "bolt" ).enabled, FALSE );

        // then
        assertThat( config.getGroups( BoltConnector.class ).size(), is( 1 ) );
        assertThat( ConfigUtils.getEnabledBoltConnectors( config ).size(), is( 0 ) );
    }

    @Test
    void shouldBeAbleToOverrideBoltListenAddressesWithJustOneParameter()
    {
        // given
        Config config = Config.defaults( stringMap(
                BoltConnector.group( "bolt" ).enabled.name(), TRUE,
                BoltConnector.group( "bolt" ).listen_address.name(), ":8000" ) );

        BoltConnector boltConnector = config.getGroups( BoltConnector.class ).values().stream().findFirst().orElseThrow();

        // then
        assertEquals( new SocketAddress( "localhost", 8000 ), config.get( boltConnector.listen_address ) );
    }

    @Test
    void shouldDeriveBoltListenAddressFromDefaultListenAddress()
    {
        // given
        Config config = Config.defaults( stringMap(
                BoltConnector.group( "bolt" ).enabled.name(), TRUE,
                GraphDatabaseSettings.default_listen_address.name(), "0.0.0.0" ) );

        BoltConnector boltConnector = config.getGroups( BoltConnector.class ).values().stream().findFirst().orElseThrow();

        // then
        assertEquals( new SocketAddress( "0.0.0.0", 7687 ), config.get( boltConnector.listen_address ) );
    }

    @Test
    void shouldDeriveBoltListenAddressFromDefaultListenAddressAndSpecifiedPort()
    {
        // given
        Config config = Config.defaults( stringMap(
                GraphDatabaseSettings.default_listen_address.name(), "0.0.0.0",
                BoltConnector.group( "bolt" ).enabled.name(), TRUE,
                BoltConnector.group( "bolt" ).listen_address.name(), ":8000" ) );

        BoltConnector boltConnector = config.getGroups( BoltConnector.class ).values().stream().findFirst().orElseThrow();

        // then
        assertEquals( new SocketAddress( "0.0.0.0", 8000 ), config.get( boltConnector.listen_address ) );
    }

    @Test
    void shouldSupportMultipleBoltConnectorsWithCustomNames()
    {
        Config config = Config.defaults( stringMap(
                BoltConnector.group( "1" ).enabled.name(), TRUE,
                BoltConnector.group( "1" ).listen_address.name(), ":8000",
                BoltConnector.group( "2" ).enabled.name(), TRUE,
                BoltConnector.group( "2" ).listen_address.name(), ":9000"
        ) );

        // when
        var groups = config.getGroups( BoltConnector.class );

        // then
        assertEquals( 2, groups.size() );
        assertEquals( new SocketAddress( "localhost", 8000 ), config.get( groups.get( "1" ).listen_address ) );
        assertEquals( new SocketAddress( "localhost", 9000 ), config.get( groups.get( "2" ).listen_address ) );
    }

    @Test
    void shouldSupportMultipleBoltConnectorsWithDefaultAndCustomName()
    {
        Config config = Config.defaults( stringMap(
                BoltConnector.group( "1" ).enabled.name(), TRUE,
                BoltConnector.group( "1" ).listen_address.name(), ":8000",
                BoltConnector.group( "2" ).enabled.name(), TRUE,
                BoltConnector.group( "2" ).listen_address.name(), ":9000" ) );

        var groups = config.getGroups( BoltConnector.class );
        // when
        BoltConnector boltConnector1 = groups.get( "1" );
        BoltConnector boltConnector2 = groups.get( "2" );

        // then
        assertEquals( new SocketAddress( "localhost", 8000 ), config.get( boltConnector1.listen_address ) );
        assertEquals( new SocketAddress( "localhost", 9000 ), config.get( boltConnector2.listen_address ) );
    }

    @Test
    void testServerDefaultSettings()
    {
        // given
        Config config = Config.defaults( GraphDatabaseSettings.SERVER_DEFAULTS );

        // when
        HttpConnector httpConnector = config.getGroups( HttpConnector.class ).values().stream().findFirst().orElseThrow();
        HttpsConnector httpsConnector = config.getGroups( HttpsConnector.class ).values().stream().findFirst().orElseThrow();

        assertEquals( new SocketAddress( "localhost", 7474 ),
                config.get( httpConnector.listen_address ) );
        assertEquals( new SocketAddress( "localhost", 7473 ),
                config.get( httpsConnector.listen_address ) );

    }

    @Test
    void shouldBeAbleToDisableHttpConnectorWithJustOneParameter()
    {
        // given
        Config disableHttpConfig = Config.defaults(
                stringMap( HttpConnector.group( "1" ).enabled.name(), FALSE,
                        HttpsConnector.group( "2" ).enabled.name(), FALSE ) );

        // then
        assertTrue( ConfigUtils.getEnabledHttpConnectors( disableHttpConfig ).isEmpty() );
        assertTrue( ConfigUtils.getEnabledHttpsConnectors( disableHttpConfig ).isEmpty() );
        assertEquals( 2, disableHttpConfig.getGroupsFromInheritance( Connector.class ).size() );
    }

    @Test
    void shouldBeAbleToOverrideHttpListenAddressWithJustOneParameter()
    {
        // given
        Config config = Config.defaults( stringMap(
                HttpConnector.group( "1" ).enabled.name(), TRUE,
                HttpConnector.group( "1" ).listen_address.name(), ":8000" ) );

        // then
        assertEquals( 1, ConfigUtils.getEnabledHttpConnectors( config ).size() );

        HttpConnector httpConnector = config.getGroups( HttpConnector.class ).values().stream().findFirst().orElseThrow();

        assertEquals( new SocketAddress( "localhost", 8000 ),
                config.get( httpConnector.listen_address ) );
    }

    @Test
    void hasDefaultBookmarkAwaitTimeout()
    {
        Config config = Config.defaults();
        long bookmarkReadyTimeoutMs = config.get( GraphDatabaseSettings.bookmark_ready_timeout ).toMillis();
        assertEquals( TimeUnit.SECONDS.toMillis( 30 ), bookmarkReadyTimeoutMs );
    }

    @Test
    void shouldBeAbleToOverrideHttpsListenAddressWithJustOneParameter()
    {
        // given
        Config config = Config.defaults( stringMap(
                HttpsConnector.group( "1" ).enabled.name(), TRUE,
                HttpsConnector.group( "1" ).listen_address.name(), ":8000" ) );

        // then
        assertEquals( 1, ConfigUtils.getEnabledHttpsConnectors( config ).size() );

        HttpsConnector httpsConnector = config.getGroups( HttpsConnector.class ).values().stream().findFirst().orElseThrow();

        assertEquals( new SocketAddress( "localhost", 8000 ),
                config.get( httpsConnector.listen_address ) );
    }

    @Test
    void throwsForIllegalBookmarkAwaitTimeout()
    {
        String[] illegalValues = { "0ms", "0s", "10ms", "99ms", "999ms", "42ms" };

        for ( String value : illegalValues )
        {
            assertThrows( IllegalArgumentException.class, () ->
            {
                Config config = Config.defaults( stringMap( GraphDatabaseSettings.bookmark_ready_timeout.name(), value ) );
                config.get( GraphDatabaseSettings.bookmark_ready_timeout );
            }, "Exception expected for value '" + value + "'" );
        }
    }

    @Test
    void shouldDeriveListenAddressFromDefaultListenAddress()
    {
        // given
        Config config = Config.newBuilder()
                .set( GraphDatabaseSettings.default_listen_address, "0.0.0.0"  )
                .set( GraphDatabaseSettings.SERVER_DEFAULTS )
                .build();

        HttpConnector httpConnector = config.getGroups( HttpConnector.class ).values().stream().findFirst().orElseThrow();
        HttpsConnector httpsConnector = config.getGroups( HttpsConnector.class ).values().stream().findFirst().orElseThrow();

        // then
        assertEquals( 2, config.getGroups( HttpConnector.class ).size() + config.getGroups( HttpsConnector.class ).size() );

        assertEquals( "0.0.0.0", config.get( httpConnector.listen_address ).getHostname() );
        assertEquals( "0.0.0.0", config.get( httpsConnector.listen_address ).getHostname() );
    }

    @Test
    void shouldDeriveListenAddressFromDefaultListenAddressAndSpecifiedPorts()
    {
        // given
        Config config = Config.defaults( stringMap( HttpsConnector.group( "https" ).enabled.name(), TRUE,
                HttpsConnector.group( "https" ).listen_address.name(), ":9000" ,
                HttpConnector.group( "http" ).enabled.name(), TRUE,
                HttpConnector.group( "http" ).listen_address.name(), ":8000",
                GraphDatabaseSettings.default_listen_address.name(), "0.0.0.0"
        ) );

        // then
        assertEquals( 2, config.getGroupsFromInheritance( Connector.class ).size() );
        HttpConnector httpConnector = config.getGroups( HttpConnector.class ).values().stream().findFirst().orElseThrow();
        HttpsConnector httpsConnector = config.getGroups( HttpsConnector.class ).values().stream().findFirst().orElseThrow();

        assertEquals( new SocketAddress( "0.0.0.0", 9000 ), config.get( httpsConnector.listen_address ) );
        assertEquals( new SocketAddress( "0.0.0.0", 8000 ), config.get( httpConnector.listen_address ) );
    }

    @Test
    void validateRetentionPolicy()
    {
        String[] validSet =
                new String[]{"true", "keep_all", "false", "keep_none", "10 files", "10k files", "10K size", "10m txs",
                        "10M entries", "10g hours", "10G days"};

        String[] invalidSet = new String[]{"invalid", "all", "10", "10k", "10k a"};

        for ( String valid : validSet )
        {
            assertEquals( valid, Config.defaults( keep_logical_logs, valid ).get( keep_logical_logs ) );
        }

        for ( String invalid : invalidSet )
        {
            assertThrows( IllegalArgumentException.class, () -> Config.defaults( keep_logical_logs, invalid ),
                    "Value \"" + invalid + "\" should be considered invalid" );

        }
    }

    @Test
    void transactionSamplingCanBePercentageValues()
    {
        IntStream range = IntStream.range( 1, 101 );
        range.forEach( percentage ->
        {
            Config config = Config.defaults( transaction_sampling_percentage, String.valueOf( percentage ) );
            int configuredSampling = config.get( transaction_sampling_percentage );
            assertEquals( percentage, configuredSampling );
        } );
        assertThrows( IllegalArgumentException.class, () -> Config.defaults( transaction_sampling_percentage, "0" ) );
        assertThrows( IllegalArgumentException.class, () -> Config.defaults( transaction_sampling_percentage, "101" ) );
        assertThrows( IllegalArgumentException.class, () -> Config.defaults( transaction_sampling_percentage, "10101" ) );
    }

    @Test
    void validateTransactionTracingLevelValues()
    {
        GraphDatabaseSettings.TransactionTracingLevel[] values = GraphDatabaseSettings.TransactionTracingLevel.values();
        for ( GraphDatabaseSettings.TransactionTracingLevel level : values )
        {
            assertEquals( level, Config.defaults( transaction_tracing_level, level.name() ).get( transaction_tracing_level ) );
        }
        assertThrows( IllegalArgumentException.class, () -> Config.defaults( transaction_tracing_level, "TRACE" ) );
    }

    @Test
    void configValuesContainsConnectors()
    {
        assertThat( GraphDatabaseSettings.SERVER_DEFAULTS.keySet(), containsInAnyOrder(
                "dbms.connector.http.http.enabled",
                "dbms.connector.https.https.enabled",
                "dbms.connector.bolt.bolt.enabled",
                "dbms.security.auth_enabled"
        ) );
    }

    @Test
    void testDefaultAddressOnlyAllowsHostname()
    {
        assertDoesNotThrow( () -> Config.defaults( default_listen_address, "foo" ) );
        assertDoesNotThrow( () -> Config.defaults( default_advertised_address, "bar" ) );

        assertThrows( IllegalArgumentException.class, () -> Config.defaults( default_listen_address, "foo:123" ) );
        assertThrows( IllegalArgumentException.class, () -> Config.defaults( default_advertised_address, "bar:456" ) );

        assertThrows( IllegalArgumentException.class, () -> Config.defaults( default_listen_address, ":123" ) );
        assertThrows( IllegalArgumentException.class, () -> Config.defaults( default_advertised_address, ":456" ) );
    }

    @Test
    void testDefaultAddressMigration()
    {
        String oldDefaultListen = "dbms.connectors.default_listen_address";
        String oldDefaultAdvertised = "dbms.connectors.default_advertised_address";

        var config = Config.defaults( Map.of( oldDefaultListen, "foo", oldDefaultAdvertised, "bar" ) );

        var logProvider = new AssertableLogProvider();
        config.setLogger( logProvider.getLog( Config.class ) );

        assertThrows( IllegalArgumentException.class, () -> config.getSetting( oldDefaultListen ) );
        assertThrows( IllegalArgumentException.class, () -> config.getSetting( oldDefaultAdvertised ) );
        assertEquals( new SocketAddress( "foo" ), config.get( default_listen_address ) );
        assertEquals( new SocketAddress( "bar" ), config.get( default_advertised_address) );

        logProvider.assertAtLeastOnce( inLog( Config.class )
                .warn( "Use of deprecated setting %s. It is replaced by %s", oldDefaultListen, default_listen_address.name() ) );

        logProvider.assertAtLeastOnce( inLog( Config.class )
                .warn( "Use of deprecated setting %s. It is replaced by %s", oldDefaultAdvertised, default_advertised_address.name() ) );

    }
}
