/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.graphdb.factory;

import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashMap;

import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings.BoltConnector;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.io.ByteUnit;
import org.neo4j.kernel.configuration.Config;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.boltConnectors;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class GraphDatabaseSettingsTest
{
    @Test
    public void mustHaveNullDefaultPageCacheMemorySizeInBytes() throws Exception
    {
        Long bytes = Config.defaults().get( GraphDatabaseSettings.pagecache_memory );
        assertThat( bytes, is( nullValue() ) );
    }

    @Test
    public void pageCacheSettingMustAcceptArbitraryUserSpecifiedValue() throws Exception
    {
        Setting<Long> setting = GraphDatabaseSettings.pagecache_memory;
        String name = setting.name();
        assertThat( new Config( stringMap( name, "245760" ) ).get( setting ), is( ByteUnit.kibiBytes( 240 ) ) );
        assertThat( new Config( stringMap( name, "2244g" ) ).get( setting ), is( ByteUnit.gibiBytes( 2244 ) ) );
    }

    @Test( expected = InvalidSettingException.class )
    public void pageCacheSettingMustRejectOverlyConstrainedMemorySetting() throws Exception
    {
        long pageSize = Config.defaults().get( GraphDatabaseSettings.mapped_memory_page_size );
        Setting<Long> setting = GraphDatabaseSettings.pagecache_memory;
        String name = setting.name();
        // We configure the page cache to have one byte less than two pages worth of memory. This must throw:
        new Config( stringMap( name, "" + (pageSize * 2 - 1) ) ).get( setting );
    }

    @Test
    public void noDuplicateSettingsAreAllowed() throws Exception
    {
        final HashMap<String,String> fields = new HashMap<>();
        for ( Field field : GraphDatabaseSettings.class.getDeclaredFields() )
        {
            if ( field.getType() == Setting.class )
            {
                Setting setting = (Setting) field.get( null );

                assertFalse(
                        String.format( "'%s' in %s has already been defined in %s", setting.name(), field.getName(),
                                fields.get( setting.name() ) ), fields.containsKey( setting.name() ) );
                fields.put( setting.name(), field.getName() );
            }
        }
    }

    @Test
    public void groupToScopeSetting() throws Exception
    {
        // given
        String hostname = "my_other_host";
        int port = 9999;
        Config config = Config.defaults();
        config.augment( stringMap( GraphDatabaseSettings.default_advertised_address.name(), hostname ) );
        String scoping = "bla";
        config.augment( stringMap( GraphDatabaseSettings.boltConnector( scoping ).advertised_address.name(), ":" + port ) );

        // when
        BoltConnector boltConnector = GraphDatabaseSettings.boltConnector( scoping );
        Setting<AdvertisedSocketAddress> advertised_address = boltConnector.advertised_address;
        AdvertisedSocketAddress advertisedSocketAddress = config.get( advertised_address );

        // then
        assertEquals( hostname, advertisedSocketAddress.getHostname() );
        assertEquals( port, advertisedSocketAddress.getPort() );
    }

    @Test
    public void shouldBeAbleToDisableBoltConnectorWithJustOneParameter() throws Exception
    {
        // given
        Config config = Config.defaults();
        config.augment( stringMap( "dbms.connector.bolt.enabled", "false" ) );

        // then
        assertThat( boltConnectors( config ), empty() );
    }

    @Test
    public void shouldBeAbleToOverrideBoltListenAddressesWithJustOneParameter() throws Exception
    {
        // given
        Config config = Config.defaults();
        config.augment( stringMap( "dbms.connector.bolt.enabled", "true" ) );
        config.augment( stringMap( "dbms.connector.bolt.listen_address", ":8000" ) );

        BoltConnector boltConnector = boltConnectors( config ).get( 0 );

        // then
        assertEquals( new ListenSocketAddress( "localhost", 8000 ), config.get( boltConnector.listen_address ) );
    }

    @Test
    public void shouldDeriveBoltListenAddressFromDefaultListenAddress() throws Exception
    {
        // given
        Config config = Config.defaults();
        config.augment( stringMap( "dbms.connector.bolt.enabled", "true" ) );
        config.augment( stringMap( "dbms.connectors.default_listen_address", "0.0.0.0" ) );

        BoltConnector boltConnector = boltConnectors( config ).get( 0 );

        // then
        assertEquals( new ListenSocketAddress( "0.0.0.0", 7687 ), config.get( boltConnector.listen_address ) );
    }

    @Test
    public void shouldDeriveBoltListenAddressFromDefaultListenAddressAndSpecifiedPort() throws Exception
    {
        // given
        Config config = Config.defaults();
        config.augment( stringMap( "dbms.connectors.default_listen_address", "0.0.0.0" ) );
        config.augment( stringMap( "dbms.connector.bolt.enabled", "true" ) );
        config.augment( stringMap( "dbms.connector.bolt.listen_address", ":8000" ) );

        BoltConnector boltConnector = boltConnectors( config ).get( 0 );

        // then
        assertEquals( new ListenSocketAddress( "0.0.0.0", 8000 ), config.get( boltConnector.listen_address ) );
    }

    @Test
    public void shouldStillSupportCustomNameForBoltConnector() throws Exception
    {
        Config config = Config.defaults();
        config.augment( stringMap( "dbms.connector.random_name_that_will_be_unsupported.type", "BOLT" ) );
        config.augment( stringMap( "dbms.connector.random_name_that_will_be_unsupported.enabled", "true" ) );
        config.augment( stringMap( "dbms.connector.random_name_that_will_be_unsupported.listen_address", ":8000" ) );

        // when
        BoltConnector boltConnector = boltConnectors( config ).get( 0 );

        // then
        assertEquals( new ListenSocketAddress( "localhost", 8000 ), config.get( boltConnector.listen_address ) );
    }

    @Test
    public void shouldSupportMultipleBoltConnectorsWithCustomNames() throws Exception
    {
        Config config = Config.defaults();
        config.augment( stringMap( "dbms.connector.bolt1.type", "BOLT" ) );
        config.augment( stringMap( "dbms.connector.bolt1.enabled", "true" ) );
        config.augment( stringMap( "dbms.connector.bolt1.listen_address", ":8000" ) );
        config.augment( stringMap( "dbms.connector.bolt2.type", "BOLT" ) );
        config.augment( stringMap( "dbms.connector.bolt2.enabled", "true" ) );
        config.augment( stringMap( "dbms.connector.bolt2.listen_address", ":9000" ) );

        // when
        BoltConnector boltConnector1 = boltConnectors( config ).get( 0 );
        BoltConnector boltConnector2 = boltConnectors( config ).get( 1 );

        // then
        assertEquals( new ListenSocketAddress( "localhost", 8000 ), config.get( boltConnector1.listen_address ) );
        assertEquals( new ListenSocketAddress( "localhost", 9000 ), config.get( boltConnector2.listen_address ) );
    }

    @Test
    public void shouldSupportMultipleBoltConnectorsWithDefaultAndCustomName() throws Exception
    {
        Config config = Config.defaults();
        config.augment( stringMap( "dbms.connector.bolt.type", "BOLT" ) );
        config.augment( stringMap( "dbms.connector.bolt.enabled", "true" ) );
        config.augment( stringMap( "dbms.connector.bolt.listen_address", ":8000" ) );
        config.augment( stringMap( "dbms.connector.bolt2.type", "BOLT" ) );
        config.augment( stringMap( "dbms.connector.bolt2.enabled", "true" ) );
        config.augment( stringMap( "dbms.connector.bolt2.listen_address", ":9000" ) );

        // when
        BoltConnector boltConnector1 = boltConnectors( config ).get( 0 );
        BoltConnector boltConnector2 = boltConnectors( config ).get( 1 );

        // then
        assertEquals( new ListenSocketAddress( "localhost", 8000 ), config.get( boltConnector1.listen_address ) );
        assertEquals( new ListenSocketAddress( "localhost", 9000 ), config.get( boltConnector2.listen_address ) );
    }
}
