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
package org.neo4j.kernel.configuration;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.configuration.HttpConnector.Encryption;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.configuration.Connector.ConnectorType.HTTP;
import static org.neo4j.kernel.configuration.ConnectorValidator.DEPRECATED_CONNECTOR_MSG;

public class HttpConnectorValidatorTest
{
    HttpConnectorValidator cv = new HttpConnectorValidator();

    @Rule
    public ExpectedException expected = ExpectedException.none();

    Consumer<String> warningConsumer = mock( Consumer.class );

    @Test
    public void doesNotValidateUnrelatedStuff()
    {
        assertEquals( 0, cv.validate( stringMap( "dbms.connector.bolt.enabled", "true",
                "dbms.blabla.boo", "123" ), warningConsumer ).size() );
    }

    @Test
    public void onlyEnabledRequiredWhenNameIsHttpOrHttps()
    {
        String httpEnabled = "dbms.connector.http.enabled";
        String httpsEnabled = "dbms.connector.https.enabled";

        assertEquals( stringMap( httpEnabled, "true" ),
                cv.validate( stringMap( httpEnabled, "true" ), warningConsumer ) );

        assertEquals( stringMap( httpsEnabled, "true" ),
                cv.validate( stringMap( httpsEnabled, "true" ), warningConsumer ) );
    }

    @Test
    public void requiresTypeWhenNameIsNotHttpOrHttps()
    {
        String randomEnabled = "dbms.connector.bla.enabled";
        String randomType = "dbms.connector.bla.type";

        assertEquals( stringMap( randomEnabled, "true", randomType, HTTP.name() ),
                cv.validate( stringMap( randomEnabled, "true", randomType, HTTP.name() ), warningConsumer ) );

        expected.expect( InvalidSettingException.class );
        expected.expectMessage( "Missing mandatory value for 'dbms.connector.bla.type'" );

        cv.validate( stringMap( randomEnabled, "true" ), warningConsumer );
    }

    @Test
    public void warnsWhenNameIsNotHttpOrHttps()
    {
        String randomEnabled = "dbms.connector.bla.enabled";
        String randomType = "dbms.connector.bla.type";

        cv.validate( stringMap( randomEnabled, "true", randomType, "HTTP" ), warningConsumer );

        verify( warningConsumer ).accept(
                format( DEPRECATED_CONNECTOR_MSG,
                        format( ">  %s%n>  %s%n", randomEnabled, randomType ) ) );
    }

    @Test
    public void errorsOnInvalidConnectorSetting1()
    {
        String invalidSetting = "dbms.connector.bla.0.enabled";

        expected.expect( InvalidSettingException.class );
        expected.expectMessage( "Invalid connector setting: dbms.connector.bla.0.enabled" );

        cv.validate( stringMap( invalidSetting, "true" ), warningConsumer );
    }

    @Test
    public void errorsOnInvalidConnectorSetting2()
    {
        String invalidSetting = "dbms.connector.http.foobar";

        expected.expect( InvalidSettingException.class );
        expected.expectMessage( "Invalid connector setting: dbms.connector.http.foobar" );

        cv.validate( stringMap( invalidSetting, "true" ), warningConsumer );
    }

    @Test
    public void validatesEncryption()
    {
        String key = "dbms.connector.bla.encryption";
        String type = "dbms.connector.bla.type";

        assertEquals( stringMap( key, Encryption.NONE.name(),
                type, HTTP.name() ),
                cv.validate( stringMap( key, Encryption.NONE.name(), type, HTTP.name() ), warningConsumer ) );

        assertEquals( stringMap( key, Encryption.TLS.name(),
                type, HTTP.name() ),
                cv.validate( stringMap( key, Encryption.TLS.name(), type, HTTP.name() ), warningConsumer ) );

        expected.expect( InvalidSettingException.class );
        expected.expectMessage(
                "Bad value 'BOBO' for setting 'dbms.connector.bla.encryption': must be one of [NONE, TLS] case " +
                        "sensitive" );

        cv.validate( stringMap( key, "BOBO", type, HTTP.name() ), warningConsumer );
    }

    @Test
    public void httpsConnectorCanOnlyHaveTLS()
    {
        String key = "dbms.connector.https.encryption";

        assertEquals( stringMap( key, Encryption.TLS.name() ),
                cv.validate( stringMap( key, Encryption.TLS.name() ), warningConsumer ) );

        expected.expect( InvalidSettingException.class );
        expected.expectMessage(
                "'dbms.connector.https.encryption' is only allowed to be 'TLS'; not 'NONE'" );
        cv.validate( stringMap( key, Encryption.NONE.name() ), warningConsumer );
    }

    @Test
    public void httpConnectorCanNotHaveTLS()
    {
        String key = "dbms.connector.http.encryption";

        assertEquals( stringMap( key, Encryption.NONE.name() ),
                cv.validate( stringMap( key, Encryption.NONE.name() ), warningConsumer ) );

        expected.expect( InvalidSettingException.class );
        expected.expectMessage(
                "'dbms.connector.http.encryption' is only allowed to be 'NONE'; not 'TLS'" );
        cv.validate( stringMap( key, Encryption.TLS.name() ), warningConsumer );
    }

    @Test
    public void validatesAddress()
    {
        String key = "dbms.connector.http.address";

        assertEquals( stringMap( key, "localhost:123" ),
                cv.validate( stringMap( key, "localhost:123" ), warningConsumer ) );

        key = "dbms.connector.bla.address";
        String type = "dbms.connector.bla.type";

        assertEquals( stringMap( key, "localhost:123",
                type, HTTP.name() ),
                cv.validate( stringMap( key, "localhost:123", type, HTTP.name() ), warningConsumer ) );

        assertEquals( stringMap( key, "localhost:123",
                type, HTTP.name() ),
                cv.validate( stringMap( key, "localhost:123", type, HTTP.name() ), warningConsumer ) );

        expected.expect( InvalidSettingException.class );
        expected.expectMessage( "Setting \"dbms.connector.bla.address\" must be in the format \"hostname:port\" or " +
                "\":port\". \"BOBO\" does not conform to these formats" );

        cv.validate( stringMap( key, "BOBO", type, HTTP.name() ), warningConsumer );
    }

    @Test
    public void validatesListenAddress()
    {
        String key = "dbms.connector.http.listen_address";

        assertEquals( stringMap( key, "localhost:123" ),
                cv.validate( stringMap( key, "localhost:123" ), warningConsumer ) );

        key = "dbms.connector.bla.listen_address";
        String type = "dbms.connector.bla.type";

        assertEquals( stringMap( key, "localhost:123",
                type, HTTP.name() ),
                cv.validate( stringMap( key, "localhost:123", type, HTTP.name() ), warningConsumer ) );

        assertEquals( stringMap( key, "localhost:123",
                type, HTTP.name() ),
                cv.validate( stringMap( key, "localhost:123", type, HTTP.name() ), warningConsumer ) );

        expected.expect( InvalidSettingException.class );
        expected.expectMessage( "Setting \"dbms.connector.bla.listen_address\" must be in the format " +
                "\"hostname:port\" or \":port\". \"BOBO\" does not conform to these formats" );

        cv.validate( stringMap( key, "BOBO", type, HTTP.name() ), warningConsumer );
    }

    @Test
    public void validatesAdvertisedAddress()
    {
        String key = "dbms.connector.http.advertised_address";

        assertEquals( stringMap( key, "localhost:123" ),
                cv.validate( stringMap( key, "localhost:123" ), warningConsumer ) );

        key = "dbms.connector.bla.advertised_address";
        String type = "dbms.connector.bla.type";

        assertEquals( stringMap( key, "localhost:123",
                type, HTTP.name() ),
                cv.validate( stringMap( key, "localhost:123", type, HTTP.name() ), warningConsumer ) );

        assertEquals( stringMap( key, "localhost:123",
                type, HTTP.name() ),
                cv.validate( stringMap( key, "localhost:123", type, HTTP.name() ), warningConsumer ) );

        expected.expect( InvalidSettingException.class );
        expected.expectMessage( "Setting \"dbms.connector.bla.advertised_address\" must be in the format " +
                "\"hostname:port\" or \":port\". \"BOBO\" does not conform to these formats" );

        cv.validate( stringMap( key, "BOBO", type, HTTP.name() ), warningConsumer );
    }

    @Test
    public void validatesType()
    {
        String type = "dbms.connector.bla.type";

        expected.expect( InvalidSettingException.class );
        expected.expectMessage( "'dbms.connector.bla.type' must be one of BOLT, HTTP; not 'BOBO'" );

        cv.validate( stringMap( type, "BOBO" ), warningConsumer );
    }

    @Test
    public void setsDeprecationFlagOnAddress()
    {
        Setting setting =
                cv.getSettingFor( "dbms.connector.http.address", Collections.emptyMap() )
                        .orElseThrow( () -> new RuntimeException( "missing setting!" ) );

        assertTrue( setting.deprecated() );
        assertEquals( Optional.of( "dbms.connector.http.listen_address" ), setting.replacement() );
    }

    @Test
    public void setsDeprecationFlagOnEncryption()
    {
        Setting setting =
                cv.getSettingFor( "dbms.connector.http.encryption", Collections.emptyMap() )
                        .orElseThrow( () -> new RuntimeException( "missing setting!" ) );

        assertTrue( setting.deprecated() );
        assertEquals( Optional.empty(), setting.replacement() );
    }

    @Test
    public void sdfa()
    {
        Setting setting =
                cv.getSettingFor( "dbms.connector.http.type", Collections.emptyMap() )
                        .orElseThrow( () -> new RuntimeException( "missing setting!" ) );

        assertTrue( setting.deprecated() );
        assertEquals( Optional.empty(), setting.replacement() );
    }

    @Test
    public void setsDeprecationFlagOnType()
    {
        Setting setting =
                cv.getSettingFor( "dbms.connector.http.type", Collections.emptyMap() )
                        .orElseThrow( () -> new RuntimeException( "missing setting!" ) );

        assertTrue( setting.deprecated() );
        assertEquals( Optional.empty(), setting.replacement() );
    }

    @Test
    public void setsDeprecationFlagOnCustomNamedHttpConnectors()
    {
        List<Setting<Object>> settings = cv.settings( stringMap( "dbms.connector.0.type", "HTTP",
                "dbms.connector.0.enabled", "false",
                "dbms.connector.0.listen_address", "1.2.3.4:123",
                "dbms.connector.0.advertised_address", "localhost:123",
                "dbms.connector.0.encryption", Encryption.NONE.toString() ) );

        assertEquals( 5, settings.size() );

        for ( Setting s : settings )
        {
            assertTrue( "every setting should be deprecated: " + s.name(), s.deprecated() );
            String[] parts = s.name().split( "\\." );
            if ( !"encryption".equals( parts[3] ) && !"type".equals( parts[3] ) )
            {
                assertEquals( Optional.of( format( "%s.%s.%s.%s", parts[0], parts[1], "http", parts[3] ) ),
                        s.replacement() );
            }
        }
    }

    @Test
    public void setsDeprecationFlagOnCustomNamedHttpsConnectors()
    {
        List<Setting<Object>> settings = cv.settings( stringMap( "dbms.connector.0.type", "HTTP",
                "dbms.connector.0.enabled", "false",
                "dbms.connector.0.listen_address", "1.2.3.4:123",
                "dbms.connector.0.advertised_address", "localhost:123",
                "dbms.connector.0.encryption", Encryption.TLS.toString() ) );

        assertEquals( 5, settings.size() );

        for ( Setting s : settings )
        {
            assertTrue( "every setting should be deprecated: " + s.name(), s.deprecated() );
            String[] parts = s.name().split( "\\." );

            if ( !"encryption".equals( parts[3] ) && !"type".equals( parts[3] ) )
            {
                assertEquals( Optional.of( format( "%s.%s.%s.%s", parts[0], parts[1], "https", parts[3] ) ),
                        s.replacement() );
            }
        }
    }
}
