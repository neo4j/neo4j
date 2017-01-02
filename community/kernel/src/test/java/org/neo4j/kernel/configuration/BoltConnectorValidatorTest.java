/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.configuration;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.kernel.configuration.BoltConnector.EncryptionLevel;

import static java.util.Collections.emptyMap;
import static org.junit.Assert.*;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.configuration.Connector.ConnectorType.BOLT;


public class BoltConnectorValidatorTest
{
    BoltConnectorValidator cv = new BoltConnectorValidator();

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test
    public void doesNotValidateUnrelatedStuff() throws Exception
    {
        assertEquals( 0, cv.validate( stringMap( "dbms.connector.http.enabled", "true",
                "dbms.blabla.boo", "123" ) ).size() );
    }

    @Test
    public void onlyEnabledRequiredWhenNameIsBolt() throws Exception
    {
        String boltEnabled = "dbms.connector.bolt.enabled";

        assertEquals( stringMap( boltEnabled, "true" ),
                cv.validate( stringMap( boltEnabled, "true" ) ) );
    }

    @Test
    public void requiresTypeWhenNameIsNotBolt() throws Exception
    {
        String randomEnabled = "dbms.connector.bla.enabled";
        String randomType = "dbms.connector.bla.type";

        assertEquals( stringMap( randomEnabled, "true", randomType, BOLT.name() ),
                cv.validate( stringMap( randomEnabled, "true", randomType, BOLT.name() ) ) );

        expected.expect( InvalidSettingException.class );
        expected.expectMessage( "Missing mandatory value for 'dbms.connector.bla.type'" );

        cv.validate( stringMap( randomEnabled, "true" ) );
    }

    @Test
    public void validatesTlsLevel() throws Exception
    {
        String key = "dbms.connector.bolt.tls_level";

        assertEquals( stringMap( key, EncryptionLevel.DISABLED.name() ),
                cv.validate( stringMap( key, EncryptionLevel.DISABLED.name() ) ) );

        assertEquals( stringMap( key,EncryptionLevel.OPTIONAL.name() ),
                cv.validate( stringMap( key, EncryptionLevel.OPTIONAL.name() ) ) );

        assertEquals( stringMap( key,EncryptionLevel.REQUIRED.name() ),
                cv.validate( stringMap( key, EncryptionLevel.REQUIRED.name() ) ) );

        key = "dbms.connector.bla.tls_level";
        String type = "dbms.connector.bla.type";

        assertEquals( stringMap( key, EncryptionLevel.DISABLED.name(), type, BOLT.name() ),
                cv.validate( stringMap( key, EncryptionLevel.DISABLED.name(), type, BOLT.name() ) ) );

        assertEquals( stringMap( key,EncryptionLevel.OPTIONAL.name(), type, BOLT.name() ),
                cv.validate( stringMap( key, EncryptionLevel.OPTIONAL.name(), type, BOLT.name() ) ) );

        assertEquals( stringMap( key,EncryptionLevel.REQUIRED.name(), type, BOLT.name() ),
                cv.validate( stringMap( key, EncryptionLevel.REQUIRED.name(), type, BOLT.name() ) ) );

        expected.expect( InvalidSettingException.class );
        expected.expectMessage( "Bad value 'BOBO' for setting 'dbms.connector.bla.tls_level': must be one of [REQUIRED, OPTIONAL, DISABLED] case sensitive" );

        cv.validate( stringMap( key, "BOBO", type, BOLT.name() ) );
    }

    @Test
    public void validatesAddress() throws Exception
    {
        String key = "dbms.connector.bolt.address";

        assertEquals( stringMap( key, "localhost:123" ),
                cv.validate( stringMap( key, "localhost:123" ) ) );

        key = "dbms.connector.bla.address";
        String type = "dbms.connector.bla.type";

        assertEquals( stringMap( key, "localhost:123",
                type, BOLT.name() ),
                cv.validate( stringMap( key, "localhost:123", type, BOLT.name() ) ) );

        assertEquals( stringMap( key, "localhost:123",
                type, BOLT.name() ),
                cv.validate( stringMap( key, "localhost:123", type, BOLT.name() ) ) );

        expected.expect( InvalidSettingException.class );
        expected.expectMessage( "Setting \"dbms.connector.bla.address\" must be in the format \"hostname:port\" or " +
                "\":port\". \"BOBO\" does not conform to these formats" );

        cv.validate( stringMap( key, "BOBO", type, BOLT.name() ) );
    }

    @Test
    public void validatesListenAddress() throws Exception
    {
        String key = "dbms.connector.bolt.listen_address";

        assertEquals( stringMap( key, "localhost:123" ),
                cv.validate( stringMap( key, "localhost:123" ) ) );

        key = "dbms.connector.bla.listen_address";
        String type = "dbms.connector.bla.type";

        assertEquals( stringMap( key, "localhost:123",
                type, BOLT.name() ),
                cv.validate( stringMap( key, "localhost:123", type, BOLT.name() ) ) );

        assertEquals( stringMap( key, "localhost:123",
                type, BOLT.name() ),
                cv.validate( stringMap( key, "localhost:123", type, BOLT.name() ) ) );

        expected.expect( InvalidSettingException.class );
        expected.expectMessage( "Setting \"dbms.connector.bla.listen_address\" must be in the format " +
                "\"hostname:port\" or \":port\". \"BOBO\" does not conform to these formats" );

        cv.validate( stringMap( key, "BOBO", type, BOLT.name() ) );
    }

    @Test
    public void validatesAdvertisedAddress() throws Exception
    {
        String key = "dbms.connector.bolt.advertised_address";

        assertEquals( stringMap( key, "localhost:123" ),
                cv.validate( stringMap( key, "localhost:123" ) ) );

        key = "dbms.connector.bla.advertised_address";
        String type = "dbms.connector.bla.type";

        assertEquals( stringMap( key, "localhost:123",
                type, BOLT.name() ),
                cv.validate( stringMap( key, "localhost:123", type, BOLT.name() ) ) );

        assertEquals( stringMap( key, "localhost:123",
                type, BOLT.name() ),
                cv.validate( stringMap( key, "localhost:123", type, BOLT.name() ) ) );

        expected.expect( InvalidSettingException.class );
        expected.expectMessage( "Setting \"dbms.connector.bla.advertised_address\" must be in the format " +
                "\"hostname:port\" or \":port\". \"BOBO\" does not conform to these formats" );

        cv.validate( stringMap( key, "BOBO", type, BOLT.name() ) );
    }

    @Test
    public void validatesType() throws Exception
    {
        String type = "dbms.connector.bla.type";

        expected.expect( InvalidSettingException.class );
        expected.expectMessage( "'dbms.connector.bla.type' must be one of BOLT, HTTP; not 'BOBO'" );

        cv.validate( stringMap( type, "BOBO" ) );
    }

    @Test
    public void unknownSubSettingsAreNotValidated() throws Exception
    {
        String madeup = "dbms.connector.bolt.imadethisup";

        assertEquals( emptyMap(), cv.validate( stringMap( madeup, "anything" ) ) );
    }
}
