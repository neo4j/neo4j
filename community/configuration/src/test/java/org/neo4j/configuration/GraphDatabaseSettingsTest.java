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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.neo4j.configuration.connectors.BoltConnector;
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
import static org.neo4j.configuration.SettingValueParsers.DURATION;
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
        Config config = Config.newBuilder().setDefaults( GraphDatabaseSettings.SERVER_DEFAULTS ).build();

        // when
        SocketAddress listenSocketAddress = config.get( BoltConnector.listen_address );

        // then
        assertEquals( new SocketAddress( "localhost", 7687 ), listenSocketAddress );
    }

    @Test
    void shouldBeAbleToOverrideBoltListenAddressesWithJustOneParameter()
    {
        // given
        Config config = Config.newBuilder()
                .set( BoltConnector.enabled, true )
                .set( BoltConnector.listen_address, new SocketAddress( 8000 ) )
                .build();

        // then
        assertEquals( new SocketAddress( "localhost", 8000 ), config.get( BoltConnector.listen_address ) );
    }

    @Test
    void shouldDeriveBoltListenAddressFromDefaultListenAddress()
    {
        // given
        Config config = Config.newBuilder()
                .set( BoltConnector.enabled, true )
                .set( GraphDatabaseSettings.default_listen_address, new SocketAddress( "0.0.0.0" ) )
                .build();

        // then
        assertEquals( new SocketAddress( "0.0.0.0", 7687 ), config.get( BoltConnector.listen_address ) );
    }

    @Test
    void shouldDeriveBoltListenAddressFromDefaultListenAddressAndSpecifiedPort()
    {
        // given
        Config config = Config.newBuilder()
                .set( GraphDatabaseSettings.default_listen_address, new SocketAddress( "0.0.0.0" ) )
                .set( BoltConnector.enabled, true )
                .set( BoltConnector.listen_address, new SocketAddress( 8000 ) )
                .build();

        // then
        assertEquals( new SocketAddress( "0.0.0.0", 8000 ), config.get( BoltConnector.listen_address ) );
    }

    @Test
    void testServerDefaultSettings()
    {
        // given
        Config config = Config.newBuilder().setDefaults( GraphDatabaseSettings.SERVER_DEFAULTS ).build();

        // then
        assertEquals( new SocketAddress( "localhost", 7474 ), config.get( HttpConnector.listen_address ) );
        assertEquals( new SocketAddress( "localhost", 7473 ), config.get( HttpsConnector.listen_address ) );
        assertEquals( new SocketAddress( "localhost", 7687 ), config.get( BoltConnector.listen_address ) );

        assertTrue( config.get( HttpConnector.enabled ) );
        assertFalse( config.get( HttpsConnector.enabled ) );
        assertTrue( config.get( BoltConnector.enabled ) );
    }

    @Test
    void hasDefaultBookmarkAwaitTimeout()
    {
        Config config = Config.defaults();
        long bookmarkReadyTimeoutMs = config.get( GraphDatabaseSettings.bookmark_ready_timeout ).toMillis();
        assertEquals( TimeUnit.SECONDS.toMillis( 30 ), bookmarkReadyTimeoutMs );
    }

    @Test
    void throwsForIllegalBookmarkAwaitTimeout()
    {
        String[] illegalValues = { "0ms", "0s", "10ms", "99ms", "999ms", "42ms" };

        for ( String value : illegalValues )
        {
            assertThrows( IllegalArgumentException.class, () ->
            {
                Config config = Config.defaults( GraphDatabaseSettings.bookmark_ready_timeout, DURATION.parse( value ) );
                config.get( GraphDatabaseSettings.bookmark_ready_timeout );
            }, "Exception expected for value '" + value + "'" );
        }
    }

    @Test
    void shouldDeriveListenAddressFromDefaultListenAddress()
    {
        // given
        Config config = Config.newBuilder()
                .set( GraphDatabaseSettings.default_listen_address, new SocketAddress( "0.0.0.0" )  )
                .setDefaults( GraphDatabaseSettings.SERVER_DEFAULTS )
                .build();

        // then
        assertEquals( "0.0.0.0", config.get( HttpConnector.listen_address ).getHostname() );
        assertEquals( "0.0.0.0", config.get( HttpsConnector.listen_address ).getHostname() );
        assertEquals( "0.0.0.0", config.get( BoltConnector.listen_address ).getHostname() );
    }

    @Test
    void shouldDeriveListenAddressFromDefaultListenAddressAndSpecifiedPorts()
    {
        // given
        Config config = Config.newBuilder()
                .set( GraphDatabaseSettings.default_listen_address, new SocketAddress( "0.0.0.0" ) )
                .set( HttpConnector.listen_address, new SocketAddress( 8000 ) )
                .set( HttpsConnector.listen_address, new SocketAddress( 9000 ) )
                .set( BoltConnector.listen_address, new SocketAddress( 10000 ) )
                .build();

        // then
        assertEquals( new SocketAddress( "0.0.0.0", 9000 ), config.get( HttpsConnector.listen_address ) );
        assertEquals( new SocketAddress( "0.0.0.0", 8000 ), config.get( HttpConnector.listen_address ) );
        assertEquals( new SocketAddress( "0.0.0.0", 10000 ), config.get( BoltConnector.listen_address ) );
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
            Config config = Config.defaults( transaction_sampling_percentage, percentage );
            int configuredSampling = config.get( transaction_sampling_percentage );
            assertEquals( percentage, configuredSampling );
        } );
        assertThrows( IllegalArgumentException.class, () -> Config.defaults( transaction_sampling_percentage, 0 ) );
        assertThrows( IllegalArgumentException.class, () -> Config.defaults( transaction_sampling_percentage, 101 ) );
        assertThrows( IllegalArgumentException.class, () -> Config.defaults( transaction_sampling_percentage, 10101 ) );
    }

    @Test
    void validateTransactionTracingLevelValues()
    {
        GraphDatabaseSettings.TransactionTracingLevel[] values = GraphDatabaseSettings.TransactionTracingLevel.values();
        for ( GraphDatabaseSettings.TransactionTracingLevel level : values )
        {
            assertEquals( level, Config.defaults( transaction_tracing_level, level ).get( transaction_tracing_level ) );
        }
        assertThrows( IllegalArgumentException.class, () -> Config.newBuilder().setRaw( Map.of( transaction_tracing_level.name(), "TRACE" ) ).build() );
    }

    @Test
    void configValuesContainsConnectors()
    {
        assertThat( GraphDatabaseSettings.SERVER_DEFAULTS.keySet().stream().map( Setting::name ).collect( Collectors.toList() ), containsInAnyOrder(
                "dbms.connector.http.enabled",
                "dbms.connector.https.enabled",
                "dbms.connector.bolt.enabled",
                "dbms.security.auth_enabled"
        ) );
    }

    @Test
    void testDefaultAddressOnlyAllowsHostname()
    {
        assertDoesNotThrow( () -> Config.defaults( default_listen_address, new SocketAddress( "foo" ) ) );
        assertDoesNotThrow( () -> Config.defaults( default_advertised_address, new SocketAddress( "bar" ) ) );

        assertThrows( IllegalArgumentException.class, () -> Config.defaults( default_listen_address, new SocketAddress( "foo", 123 ) ) );
        assertThrows( IllegalArgumentException.class, () -> Config.defaults( default_advertised_address, new SocketAddress( "bar", 456 ) ) );

        assertThrows( IllegalArgumentException.class, () -> Config.defaults( default_listen_address, new SocketAddress( 123 ) ) );
        assertThrows( IllegalArgumentException.class, () -> Config.defaults( default_advertised_address, new SocketAddress( 456 ) ) );
    }

    @Test
    void testDefaultAddressMigration()
    {
        String oldDefaultListen = "dbms.connectors.default_listen_address";
        String oldDefaultAdvertised = "dbms.connectors.default_advertised_address";

        var config = Config.newBuilder().setRaw( Map.of( oldDefaultListen, "foo", oldDefaultAdvertised, "bar" ) ).build();

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
