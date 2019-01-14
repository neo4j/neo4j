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
package org.neo4j.graphdb.factory;

import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.HttpConnector;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.keep_logical_logs;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.configuration.HttpConnector.Encryption.TLS;

public class GraphDatabaseSettingsTest
{
    @Test
    public void mustHaveNullDefaultPageCacheMemorySizeInBytes()
    {
        String bytes = Config.defaults().get( GraphDatabaseSettings.pagecache_memory );
        assertThat( bytes, is( nullValue() ) );
    }

    @Test
    public void pageCacheSettingMustAcceptArbitraryUserSpecifiedValue()
    {
        Setting<String> setting = GraphDatabaseSettings.pagecache_memory;
        assertThat( Config.defaults( setting, "245760" ).get( setting ), is( "245760" ) );
        assertThat( Config.defaults( setting, "2244g" ).get( setting ), is( "2244g" ) );
        assertThat( Config.defaults( setting, "string" ).get( setting ), is( "string" ) );
    }

    @Test
    public void noDuplicateSettingsAreAllowed() throws Exception
    {
        final HashMap<String,String> fields = new HashMap<>();
        for ( Field field : GraphDatabaseSettings.class.getDeclaredFields() )
        {
            if ( field.getType() == Setting.class )
            {
                Setting<?> setting = (Setting<?>) field.get( null );

                assertFalse(
                        String.format( "'%s' in %s has already been defined in %s", setting.name(), field.getName(),
                                fields.get( setting.name() ) ), fields.containsKey( setting.name() ) );
                fields.put( setting.name(), field.getName() );
            }
        }
    }

    @Test
    public void groupToScopeSetting()
    {
        // given
        String hostname = "my_other_host";
        int port = 9999;
        String scoping = "bla";
        Map<String,String> config = stringMap(
                GraphDatabaseSettings.default_advertised_address.name(), hostname,
                new BoltConnector( scoping ).advertised_address.name(), ":" + port
        );

        // when
        BoltConnector boltConnector = new BoltConnector( scoping );
        Setting<AdvertisedSocketAddress> advertisedAddress = boltConnector.advertised_address;
        AdvertisedSocketAddress advertisedSocketAddress = advertisedAddress.apply( config::get );

        // then
        assertEquals( hostname, advertisedSocketAddress.getHostname() );
        assertEquals( port, advertisedSocketAddress.getPort() );
    }

    @Test
    public void shouldEnableBoltByDefault()
    {
        // given
        Config config = Config.builder().withServerDefaults().build();

        // when
        BoltConnector boltConnector = config.boltConnectors().get( 0 );
        ListenSocketAddress listenSocketAddress = config.get( boltConnector.listen_address );

        // then
        assertEquals( new ListenSocketAddress( "127.0.0.1", 7687 ), listenSocketAddress );
    }

    @Test
    public void shouldBeAbleToDisableBoltConnectorWithJustOneParameter()
    {
        // given
        Config config = Config.defaults( new BoltConnector( "bolt" ).enabled, "false" );

        // then
        assertThat( config.boltConnectors().size(), is( 1 ) );
        assertThat( config.enabledBoltConnectors(), empty() );
    }

    @Test
    public void shouldBeAbleToOverrideBoltListenAddressesWithJustOneParameter()
    {
        // given
        Config config = Config.defaults( stringMap(
                "dbms.connector.bolt.enabled", "true",
                "dbms.connector.bolt.listen_address", ":8000" ) );

        BoltConnector boltConnector = config.boltConnectors().get( 0 );

        // then
        assertEquals( new ListenSocketAddress( "127.0.0.1", 8000 ), config.get( boltConnector.listen_address ) );
    }

    @Test
    public void shouldDeriveBoltListenAddressFromDefaultListenAddress()
    {
        // given
        Config config = Config.defaults( stringMap(
                "dbms.connector.bolt.enabled", "true",
                "dbms.connectors.default_listen_address", "0.0.0.0" ) );

        BoltConnector boltConnector = config.boltConnectors().get( 0 );

        // then
        assertEquals( new ListenSocketAddress( "0.0.0.0", 7687 ), config.get( boltConnector.listen_address ) );
    }

    @Test
    public void shouldDeriveBoltListenAddressFromDefaultListenAddressAndSpecifiedPort()
    {
        // given
        Config config = Config.defaults( stringMap(
                "dbms.connectors.default_listen_address", "0.0.0.0",
                "dbms.connector.bolt.enabled", "true",
                "dbms.connector.bolt.listen_address", ":8000" ) );

        BoltConnector boltConnector = config.boltConnectors().get( 0 );

        // then
        assertEquals( new ListenSocketAddress( "0.0.0.0", 8000 ), config.get( boltConnector.listen_address ) );
    }

    @Test
    public void shouldStillSupportCustomNameForBoltConnector()
    {
        Config config = Config.defaults( stringMap(
                "dbms.connector.random_name_that_will_be_unsupported.type", "BOLT",
                "dbms.connector.random_name_that_will_be_unsupported.enabled", "true",
                "dbms.connector.random_name_that_will_be_unsupported.listen_address", ":8000" ) );

        // when
        BoltConnector boltConnector = config.boltConnectors().get( 0 );

        // then
        assertEquals( new ListenSocketAddress( "127.0.0.1", 8000 ), config.get( boltConnector.listen_address ) );
    }

    @Test
    public void shouldSupportMultipleBoltConnectorsWithCustomNames()
    {
        Config config = Config.defaults( stringMap(
                "dbms.connector.bolt1.type", "BOLT",
                "dbms.connector.bolt1.enabled", "true",
                "dbms.connector.bolt1.listen_address", ":8000",
                "dbms.connector.bolt2.type", "BOLT",
                "dbms.connector.bolt2.enabled", "true",
                "dbms.connector.bolt2.listen_address", ":9000"
        ) );

        // when
        List<ListenSocketAddress> addresses = config.boltConnectors().stream()
                .map( c -> config.get( c.listen_address ) )
                .collect( Collectors.toList() );

        // then
        assertEquals( 2, addresses.size() );

        if ( addresses.get( 0 ).getPort() == 8000 )
        {
            assertEquals( new ListenSocketAddress( "127.0.0.1", 8000 ), addresses.get( 0 ) );
            assertEquals( new ListenSocketAddress( "127.0.0.1", 9000 ), addresses.get( 1 ) );
        }
        else
        {
            assertEquals( new ListenSocketAddress( "127.0.0.1", 8000 ), addresses.get( 1 ) );
            assertEquals( new ListenSocketAddress( "127.0.0.1", 9000 ), addresses.get( 0 ) );
        }
    }

    @Test
    public void shouldSupportMultipleBoltConnectorsWithDefaultAndCustomName()
    {
        Config config = Config.defaults( stringMap(
                "dbms.connector.bolt.type", "BOLT",
                "dbms.connector.bolt.enabled", "true",
                "dbms.connector.bolt.listen_address", ":8000",
                "dbms.connector.bolt2.type", "BOLT",
                "dbms.connector.bolt2.enabled", "true",
                "dbms.connector.bolt2.listen_address", ":9000" ) );

        // when
        BoltConnector boltConnector1 = config.boltConnectors().get( 0 );
        BoltConnector boltConnector2 = config.boltConnectors().get( 1 );

        // then
        assertEquals( new ListenSocketAddress( "127.0.0.1", 8000 ), config.get( boltConnector1.listen_address ) );
        assertEquals( new ListenSocketAddress( "127.0.0.1", 9000 ), config.get( boltConnector2.listen_address ) );
    }

    /// JONAS HTTP FOLLOWS
    @Test
    public void testServerDefaultSettings()
    {
        // given
        Config config = Config.builder().withServerDefaults().build();

        // when
        List<HttpConnector> connectors = config.httpConnectors();

        // then
        assertEquals( 2, connectors.size() );
        if ( connectors.get( 0 ).encryptionLevel().equals( TLS ) )
        {
            assertEquals( new ListenSocketAddress( "localhost", 7474 ),
                    config.get( connectors.get( 1 ).listen_address ) );
            assertEquals( new ListenSocketAddress( "localhost", 7473 ),
                    config.get( connectors.get( 0 ).listen_address ) );
        }
        else
        {
            assertEquals( new ListenSocketAddress( "127.0.0.1", 7474 ),
                    config.get( connectors.get( 0 ).listen_address ) );
            assertEquals( new ListenSocketAddress( "127.0.0.1", 7473 ),
                    config.get( connectors.get( 1 ).listen_address ) );
        }
    }

    @Test
    public void shouldBeAbleToDisableHttpConnectorWithJustOneParameter()
    {
        // given
        Config disableHttpConfig = Config.defaults(
                stringMap( "dbms.connector.http.enabled", "false",
                        "dbms.connector.https.enabled", "false" ) );

        // then
        assertTrue( disableHttpConfig.enabledHttpConnectors().isEmpty() );
        assertEquals( 2, disableHttpConfig.httpConnectors().size() );
    }

    @Test
    public void shouldBeAbleToOverrideHttpListenAddressWithJustOneParameter()
    {
        // given
        Config config = Config.defaults( stringMap(
                "dbms.connector.http.enabled", "true",
                "dbms.connector.http.listen_address", ":8000" ) );

        // then
        assertEquals( 1, config.enabledHttpConnectors().size() );

        HttpConnector httpConnector = config.enabledHttpConnectors().get( 0 );

        assertEquals( new ListenSocketAddress( "127.0.0.1", 8000 ),
                config.get( httpConnector.listen_address ) );
    }

    @Test
    public void hasDefaultBookmarkAwaitTimeout()
    {
        Config config = Config.defaults();
        long bookmarkReadyTimeoutMs = config.get( GraphDatabaseSettings.bookmark_ready_timeout ).toMillis();
        assertEquals( TimeUnit.SECONDS.toMillis( 30 ), bookmarkReadyTimeoutMs );
    }

    @Test
    public void shouldBeAbleToOverrideHttpsListenAddressWithJustOneParameter()
    {
        // given
        Config config = Config.defaults( stringMap(
                "dbms.connector.https.enabled", "true",
                "dbms.connector.https.listen_address", ":8000" ) );

        // then
        assertEquals( 1, config.enabledHttpConnectors().size() );
        HttpConnector httpConnector = config.enabledHttpConnectors().get( 0 );

        assertEquals( new ListenSocketAddress( "127.0.0.1", 8000 ),
                config.get( httpConnector.listen_address ) );
    }

    @Test
    public void throwsForIllegalBookmarkAwaitTimeout()
    {
        String[] illegalValues = { "0ms", "0s", "10ms", "99ms", "999ms", "42ms" };

        for ( String value : illegalValues )
        {
            try
            {
                Config config = Config.defaults( stringMap(
                        GraphDatabaseSettings.bookmark_ready_timeout.name(), value ) );
                config.get( GraphDatabaseSettings.bookmark_ready_timeout );
                fail( "Exception expected for value '" + value + "'" );
            }
            catch ( Exception e )
            {
                assertThat( e, instanceOf( InvalidSettingException.class ) );
            }
        }
    }

    @Test
    public void shouldDeriveListenAddressFromDefaultListenAddress()
    {
        // given
        Config config = Config.fromSettings( stringMap( "dbms.connector.https.enabled", "true",
                "dbms.connector.http.enabled", "true",
                "dbms.connectors.default_listen_address", "0.0.0.0" ) ).withServerDefaults().build();

        // then
        assertEquals( 2, config.enabledHttpConnectors().size() );
        config.enabledHttpConnectors().forEach( c ->
                assertEquals( "0.0.0.0", config.get( c.listen_address ).getHostname() ) );
    }

    @Test
    public void shouldDeriveListenAddressFromDefaultListenAddressAndSpecifiedPorts()
    {
        // given
        Config config = Config.defaults( stringMap( "dbms.connector.https.enabled", "true",
                "dbms.connector.http.enabled", "true",
                "dbms.connectors.default_listen_address", "0.0.0.0",
                "dbms.connector.http.listen_address", ":8000",
                "dbms.connector.https.listen_address", ":9000" ) );

        // then
        assertEquals( 2, config.enabledHttpConnectors().size() );

        config.enabledHttpConnectors().forEach( c ->
                {
                    if ( c.key().equals( "https" ) )
                    {
                        assertEquals( new ListenSocketAddress( "0.0.0.0", 9000 ),
                                config.get( c.listen_address ) );
                    }
                    else
                    {
                        assertEquals( new ListenSocketAddress( "0.0.0.0", 8000 ),
                                config.get( c.listen_address ) );
                    }
                }
        );
    }

    @Test
    public void shouldStillSupportCustomNameForHttpConnector()
    {
        Config config = Config.defaults( stringMap(
                "dbms.connector.random_name_that_will_be_unsupported.type", "HTTP",
                "dbms.connector.random_name_that_will_be_unsupported.encryption", "NONE",
                "dbms.connector.random_name_that_will_be_unsupported.enabled", "true",
                "dbms.connector.random_name_that_will_be_unsupported.listen_address", ":8000" ) );

        // then
        assertEquals( 1, config.enabledHttpConnectors().size() );
        assertEquals( new ListenSocketAddress( "127.0.0.1", 8000 ),
                config.get( config.enabledHttpConnectors().get( 0 ).listen_address ) );
    }

    @Test
    public void shouldStillSupportCustomNameForHttpsConnector()
    {
        Config config = Config.defaults( stringMap(
                "dbms.connector.random_name_that_will_be_unsupported.type", "HTTP",
                "dbms.connector.random_name_that_will_be_unsupported.encryption", "TLS",
                "dbms.connector.random_name_that_will_be_unsupported.enabled", "true",
                "dbms.connector.random_name_that_will_be_unsupported.listen_address", ":9000" ) );

        // then
        assertEquals( 1, config.enabledHttpConnectors().size() );
        assertEquals( new ListenSocketAddress( "127.0.0.1", 9000 ),
                config.get( config.enabledHttpConnectors().get( 0 ).listen_address ) );
    }

    @Test
    public void validateRetentionPolicy()
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
            try
            {
                Config.defaults( keep_logical_logs, invalid );
                fail( "Value \"" + invalid + "\" should be considered invalid" );
            }
            catch ( InvalidSettingException ignored )
            {
            }
        }
    }
}
