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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.configuration.BoltConnector.EncryptionLevel;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.configuration.Connector.ConnectorType.BOLT;
import static org.neo4j.kernel.configuration.ConnectorValidator.DEPRECATED_CONNECTOR_MSG;


public class BoltConnectorValidatorTest
{
    BoltConnectorValidator cv = new BoltConnectorValidator();

    @Rule
    public ExpectedException expected = ExpectedException.none();
    private Consumer<String> warningConsumer;

    @Before
    public void setup()
    {
        warningConsumer = mock( Consumer.class );
    }

    @Test
    public void doesNotValidateUnrelatedStuff()
    {
        assertEquals( 0, cv.validate( stringMap( "dbms.connector.http.enabled", "true",
                "dbms.blabla.boo", "123" ), warningConsumer ).size() );
    }

    @Test
    public void onlyEnabledRequiredWhenNameIsBolt()
    {
        String boltEnabled = "dbms.connector.bolt.enabled";

        assertEquals( stringMap( boltEnabled, "true" ),
                cv.validate( stringMap( boltEnabled, "true" ), warningConsumer ) );
    }

    @Test
    public void requiresTypeWhenNameIsNotBolt()
    {
        String randomEnabled = "dbms.connector.bla.enabled";
        String randomType = "dbms.connector.bla.type";

        assertEquals( stringMap( randomEnabled, "true", randomType, BOLT.name() ),
                cv.validate( stringMap( randomEnabled, "true", randomType, BOLT.name() ), warningConsumer ) );

        expected.expect( InvalidSettingException.class );
        expected.expectMessage( "Missing mandatory value for 'dbms.connector.bla.type'" );

        cv.validate( stringMap( randomEnabled, "true" ), warningConsumer );
    }

    @Test
    public void requiresCorrectTypeWhenNameIsNotBolt()
    {
        String randomEnabled = "dbms.connector.bla.enabled";
        String randomType = "dbms.connector.bla.type";

        expected.expect( InvalidSettingException.class );
        expected.expectMessage( "dbms.connector.bla.type' must be one of BOLT, HTTP; not 'woo'" );

        cv.validate( stringMap( randomEnabled, "true", randomType, "woo" ), warningConsumer );
    }

    @Test
    public void warnsWhenNameIsNotBolt()
    {
        String randomEnabled = "dbms.connector.bla.enabled";
        String randomType = "dbms.connector.bla.type";

        cv.validate( stringMap( randomEnabled, "true", randomType, "BOLT" ), warningConsumer );

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
        String invalidSetting = "dbms.connector.bolt.foobar";

        expected.expect( InvalidSettingException.class );
        expected.expectMessage( "Invalid connector setting: dbms.connector.bolt.foobar" );

        cv.validate( stringMap( invalidSetting, "true" ), warningConsumer );
    }

    @Test
    public void validatesTlsLevel()
    {
        String key = "dbms.connector.bolt.tls_level";

        assertEquals( stringMap( key, EncryptionLevel.DISABLED.name() ),
                cv.validate( stringMap( key, EncryptionLevel.DISABLED.name() ), warningConsumer ) );

        assertEquals( stringMap( key, EncryptionLevel.OPTIONAL.name() ),
                cv.validate( stringMap( key, EncryptionLevel.OPTIONAL.name() ), warningConsumer ) );

        assertEquals( stringMap( key, EncryptionLevel.REQUIRED.name() ),
                cv.validate( stringMap( key, EncryptionLevel.REQUIRED.name() ), warningConsumer ) );

        key = "dbms.connector.bla.tls_level";
        String type = "dbms.connector.bla.type";

        assertEquals( stringMap( key, EncryptionLevel.DISABLED.name(), type, BOLT.name() ),
                cv.validate( stringMap( key, EncryptionLevel.DISABLED.name(), type, BOLT.name() ), warningConsumer ) );

        assertEquals( stringMap( key, EncryptionLevel.OPTIONAL.name(), type, BOLT.name() ),
                cv.validate( stringMap( key, EncryptionLevel.OPTIONAL.name(), type, BOLT.name() ), warningConsumer ) );

        assertEquals( stringMap( key, EncryptionLevel.REQUIRED.name(), type, BOLT.name() ),
                cv.validate( stringMap( key, EncryptionLevel.REQUIRED.name(), type, BOLT.name() ), warningConsumer ) );

        expected.expect( InvalidSettingException.class );
        expected.expectMessage(
                "Bad value 'BOBO' for setting 'dbms.connector.bla.tls_level': must be one of [REQUIRED, OPTIONAL, " +
                        "DISABLED] case sensitive" );

        cv.validate( stringMap( key, "BOBO", type, BOLT.name() ), warningConsumer );
    }

    @Test
    public void validatesAddress()
    {
        String key = "dbms.connector.bolt.address";

        assertEquals( stringMap( key, "localhost:123" ),
                cv.validate( stringMap( key, "localhost:123" ), warningConsumer ) );

        key = "dbms.connector.bla.address";
        String type = "dbms.connector.bla.type";

        assertEquals( stringMap( key, "localhost:123",
                type, BOLT.name() ),
                cv.validate( stringMap( key, "localhost:123", type, BOLT.name() ), warningConsumer ) );

        assertEquals( stringMap( key, "localhost:123",
                type, BOLT.name() ),
                cv.validate( stringMap( key, "localhost:123", type, BOLT.name() ), warningConsumer ) );

        expected.expect( InvalidSettingException.class );
        expected.expectMessage( "Setting \"dbms.connector.bla.address\" must be in the format \"hostname:port\" or " +
                "\":port\". \"BOBO\" does not conform to these formats" );

        cv.validate( stringMap( key, "BOBO", type, BOLT.name() ), warningConsumer );
    }

    @Test
    public void validatesListenAddress()
    {
        String key = "dbms.connector.bolt.listen_address";

        assertEquals( stringMap( key, "localhost:123" ),
                cv.validate( stringMap( key, "localhost:123" ), warningConsumer ) );

        key = "dbms.connector.bla.listen_address";
        String type = "dbms.connector.bla.type";

        assertEquals( stringMap( key, "localhost:123",
                type, BOLT.name() ),
                cv.validate( stringMap( key, "localhost:123", type, BOLT.name() ), warningConsumer ) );

        assertEquals( stringMap( key, "localhost:123",
                type, BOLT.name() ),
                cv.validate( stringMap( key, "localhost:123", type, BOLT.name() ), warningConsumer ) );

        expected.expect( InvalidSettingException.class );
        expected.expectMessage( "Setting \"dbms.connector.bla.listen_address\" must be in the format " +
                "\"hostname:port\" or \":port\". \"BOBO\" does not conform to these formats" );

        cv.validate( stringMap( key, "BOBO", type, BOLT.name() ), warningConsumer );
    }

    @Test
    public void validatesAdvertisedAddress()
    {
        String key = "dbms.connector.bolt.advertised_address";

        assertEquals( stringMap( key, "localhost:123" ),
                cv.validate( stringMap( key, "localhost:123" ), warningConsumer ) );

        key = "dbms.connector.bla.advertised_address";
        String type = "dbms.connector.bla.type";

        assertEquals( stringMap( key, "localhost:123",
                type, BOLT.name() ),
                cv.validate( stringMap( key, "localhost:123", type, BOLT.name() ), warningConsumer ) );

        assertEquals( stringMap( key, "localhost:123",
                type, BOLT.name() ),
                cv.validate( stringMap( key, "localhost:123", type, BOLT.name() ), warningConsumer ) );

        expected.expect( InvalidSettingException.class );
        expected.expectMessage( "Setting \"dbms.connector.bla.advertised_address\" must be in the format " +
                "\"hostname:port\" or \":port\". \"BOBO\" does not conform to these formats" );

        cv.validate( stringMap( key, "BOBO", type, BOLT.name() ), warningConsumer );
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
                cv.getSettingFor( "dbms.connector.bolt.address", Collections.emptyMap() )
                        .orElseThrow( () -> new RuntimeException( "missing setting!" ) );

        assertTrue( setting.deprecated() );
        assertEquals( Optional.of( "dbms.connector.bolt.listen_address" ), setting.replacement() );
    }

    @Test
    public void setsDeprecationFlagOnType()
    {
        Setting setting =
                cv.getSettingFor( "dbms.connector.bolt.type", Collections.emptyMap() )
                        .orElseThrow( () -> new RuntimeException( "missing setting!" ) );

        assertTrue( setting.deprecated() );
        assertEquals( Optional.empty(), setting.replacement() );
    }

    @Test
    public void setsDeprecationFlagOnCustomNamedBoltConnectors()
    {
        List<Setting<Object>> settings = cv.settings( stringMap( "dbms.connector.0.type", "BOLT",
                "dbms.connector.0.enabled", "false",
                "dbms.connector.0.listen_address", "1.2.3.4:123",
                "dbms.connector.0.advertised_address", "localhost:123",
                "dbms.connector.0.tls_level", EncryptionLevel.OPTIONAL.toString() ) );

        assertEquals( 5, settings.size() );

        for ( Setting s : settings )
        {
            assertTrue( "every setting should be deprecated: " + s.name(), s.deprecated() );
            String[] parts = s.name().split( "\\." );
            if ( !"type".equals( parts[3] ) )
            {
                assertEquals( Optional.of( format( "%s.%s.%s.%s", parts[0], parts[1], "bolt", parts[3] ) ),
                        s.replacement() );
            }
        }
    }
}
