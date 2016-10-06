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
package org.neo4j.server.configuration;

import org.junit.Ignore;
import org.junit.Test;

import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.server.configuration.ClientConnectorSettings.HttpConnector.Encryption;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.server.configuration.ClientConnectorSettings.httpConnector;

@SuppressWarnings("OptionalGetWithoutIsPresent")
public class ClientConnectorSettingsTest
{
    @Test
    @Ignore("This is loaded via the default values in the config file. Embedded should NOT get these defaults.")
    public void shouldHaveHttpAndHttpsEnabledByDefault() throws Exception
    {
        // given
        Config config = Config.defaults();

        // when
        ClientConnectorSettings.HttpConnector httpConnector = httpConnector( config, Encryption.NONE ).get();
        ClientConnectorSettings.HttpConnector httpsConnector = httpConnector( config, Encryption.TLS ).get();

        // then
        assertEquals( new ListenSocketAddress( "localhost", 7474 ), config.get( httpConnector.listen_address ) );
        assertEquals( new ListenSocketAddress( "localhost", 7473 ), config.get( httpsConnector.listen_address ) );
    }

    @Test
    public void shouldBeAbleToDisableHttpConnectorWithJustOneParameter() throws Exception
    {
        // given
        Config disableHttpConfig = Config.defaults();
        disableHttpConfig.augment( stringMap( "dbms.connector.http.enabled", "false" ) );

        // then
        assertFalse( httpConnector( disableHttpConfig, Encryption.NONE ).isPresent() );
    }

    @Test
    public void shouldBeAbleToDisableHttpsConnectorWithJustOneParameter() throws Exception
    {
        // given
        Config disableHttpsConfig = Config.defaults();
        disableHttpsConfig.augment( stringMap( "dbms.connector.https.enabled", "false" ) );

        // then
        assertFalse( httpConnector( disableHttpsConfig, Encryption.TLS ).isPresent() );
    }

    @Test
    public void shouldBeAbleToOverrideHttpListenAddressWithJustOneParameter() throws Exception
    {
        // given
        Config config = Config.defaults();
        config.augment( stringMap( "dbms.connector.http.enabled", "true" ) );
        config.augment( stringMap( "dbms.connector.http.listen_address", ":8000" ) );

        ClientConnectorSettings.HttpConnector httpConnector = httpConnector( config, Encryption.NONE ).get();

        // then
        assertEquals( new ListenSocketAddress( "localhost", 8000 ), config.get( httpConnector.listen_address ) );
    }

    @Test
    public void shouldBeAbleToOverrideHttpsListenAddressWithJustOneParameter() throws Exception
    {
        // given
        Config config = Config.defaults();
        config.augment( stringMap( "dbms.connector.https.enabled", "true" ) );
        config.augment( stringMap( "dbms.connector.https.listen_address", ":9000" ) );

        ClientConnectorSettings.HttpConnector httpsConnector = httpConnector( config, Encryption.TLS ).get();

        // then
        assertEquals( new ListenSocketAddress( "localhost", 9000 ), config.get( httpsConnector.listen_address ) );
    }

    @Test
    public void shouldDeriveListenAddressFromDefaultListenAddress() throws Exception
    {
        // given
        Config config = Config.defaults();
        config.augment( stringMap( "dbms.connector.https.enabled", "true" ) );
        config.augment( stringMap( "dbms.connector.http.enabled", "true" ) );
        config.augment( stringMap( "dbms.connectors.default_listen_address", "0.0.0.0" ) );

        // when
        ClientConnectorSettings.HttpConnector httpConnector = httpConnector( config, Encryption.NONE ).get();
        ClientConnectorSettings.HttpConnector httpsConnector = httpConnector( config, Encryption.TLS ).get();

        // then
        assertEquals( new ListenSocketAddress( "0.0.0.0", 7474 ), config.get( httpConnector.listen_address ) );
        assertEquals( new ListenSocketAddress( "0.0.0.0", 7473 ), config.get( httpsConnector.listen_address ) );
    }

    @Test
    public void shouldDeriveListenAddressFromDefaultListenAddressAndSpecifiedPorts() throws Exception
    {
        // given
        Config config = Config.defaults();
        config.augment( stringMap( "dbms.connector.https.enabled", "true" ) );
        config.augment( stringMap( "dbms.connector.http.enabled", "true" ) );
        config.augment( stringMap( "dbms.connectors.default_listen_address", "0.0.0.0" ) );
        config.augment( stringMap( "dbms.connector.http.listen_address", ":8000" ) );
        config.augment( stringMap( "dbms.connector.https.listen_address", ":9000" ) );

        // when
        ClientConnectorSettings.HttpConnector httpConnector = httpConnector( config, Encryption.NONE ).get();
        ClientConnectorSettings.HttpConnector httpsConnector = httpConnector( config, Encryption.TLS ).get();

        // then
        assertEquals( new ListenSocketAddress( "0.0.0.0", 8000 ), config.get( httpConnector.listen_address ) );
        assertEquals( new ListenSocketAddress( "0.0.0.0", 9000 ), config.get( httpsConnector.listen_address ) );
    }

    @Test
    public void shouldStillSupportCustomNameForHttpConnector() throws Exception
    {
        Config config = Config.defaults();
        config.augment( stringMap( "dbms.connector.random_name_that_will_be_unsupported.type", "HTTP" ) );
        config.augment( stringMap( "dbms.connector.random_name_that_will_be_unsupported.encryption", "NONE" ) );
        config.augment( stringMap( "dbms.connector.random_name_that_will_be_unsupported.enabled", "true" ) );
        config.augment( stringMap( "dbms.connector.random_name_that_will_be_unsupported.listen_address", ":8000" ) );

        // when
        ClientConnectorSettings.HttpConnector httpConnector = httpConnector( config, Encryption.NONE ).get();

        // then
        assertEquals( new ListenSocketAddress( "localhost", 8000 ), config.get( httpConnector.listen_address ) );
    }

    @Test
    public void shouldStillSupportCustomNameForHttpsConnector() throws Exception
    {
        Config config = Config.defaults();
        config.augment( stringMap( "dbms.connector.random_name_that_will_be_unsupported.type", "HTTP" ) );
        config.augment( stringMap( "dbms.connector.random_name_that_will_be_unsupported.encryption", "TLS" ) );
        config.augment( stringMap( "dbms.connector.random_name_that_will_be_unsupported.enabled", "true" ) );
        config.augment( stringMap( "dbms.connector.random_name_that_will_be_unsupported.listen_address", ":9000" ) );

        // when
        ClientConnectorSettings.HttpConnector httpsConnector = httpConnector( config, Encryption.TLS ).get();

        // then
        assertEquals( new ListenSocketAddress( "localhost", 9000 ), config.get( httpsConnector.listen_address ) );
    }
}
