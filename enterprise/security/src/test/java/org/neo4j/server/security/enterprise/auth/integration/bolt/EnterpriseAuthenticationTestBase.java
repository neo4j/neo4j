/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.security.enterprise.auth.integration.bolt;

import org.apache.directory.server.core.integ.AbstractLdapTestUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.parboiled.common.StringUtils;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.neo4j.bolt.v1.transport.integration.Neo4jWithSocket;
import org.neo4j.bolt.v1.transport.integration.TransportTestUtil;
import org.neo4j.bolt.v1.transport.socket.client.SecureSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.TransportConnection;
import org.neo4j.function.Factory;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static java.util.Arrays.stream;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anything;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.bolt.v1.messaging.message.InitMessage.init;
import static org.neo4j.bolt.v1.messaging.message.PullAllMessage.pullAll;
import static org.neo4j.bolt.v1.messaging.message.RunMessage.run;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgFailure;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgRecord;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgSuccess;
import static org.neo4j.bolt.v1.runtime.spi.StreamMatchers.eqRecord;
import static org.neo4j.bolt.v1.runtime.spi.StreamMatchers.greaterThanOrEqualTo;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.chunk;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.eventuallyDisconnects;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.eventuallyReceives;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;

public abstract class EnterpriseAuthenticationTestBase extends AbstractLdapTestUnit
{
    @Rule
    public Neo4jWithSocket server = new Neo4jWithSocket( getClass(), getTestGraphDatabaseFactory(),
            asSettings( getSettingsFunction() ) );

    protected static String createdUserPassword = "nativePassword";

    protected void restartNeo4jServerWithOverriddenSettings( Consumer<Map<Setting<?>,String>> overrideSettingsFunction )
            throws IOException
    {
        server.shutdownDatabase();
        server.ensureDatabase( asSettings( overrideSettingsFunction ) );
        lookupConnectorAddress();
    }

    protected Consumer<Map<String,String>> asSettings( Consumer<Map<Setting<?>,String>> overrideSettingsFunction )
    {
        return settings ->
        {
            Map<Setting<?>,String> o = new LinkedHashMap<>();
            overrideSettingsFunction.accept( o );
            for ( Setting key : o.keySet() )
            {
                settings.put( key.name(), o.get( key ) );
            }
        };
    }

    protected TestGraphDatabaseFactory getTestGraphDatabaseFactory()
    {
        return new TestEnterpriseGraphDatabaseFactory();
    }

    protected Consumer<Map<Setting<?>,String>> getSettingsFunction()
    {
        return settings -> settings.put( GraphDatabaseSettings.auth_enabled, "true" );
    }

    public Factory<TransportConnection> cf = (Factory<TransportConnection>) SecureSocketConnection::new;

    protected HostnamePort address;
    protected TransportConnection client;

    @Before
    public void setup()
    {
        this.client = cf.newInstance();
        lookupConnectorAddress();
    }

    protected void lookupConnectorAddress()
    {
        this.address = server.lookupDefaultConnector();
    }

    @After
    public void teardown() throws Exception
    {
        if ( client != null )
        {
            client.disconnect();
        }
    }

    protected void reconnect() throws Exception
    {
        if ( client != null )
        {
            client.disconnect();
        }
        this.client = cf.newInstance();
    }

    protected void testCreateReaderUser() throws Exception
    {
        testCreateReaderUser( "neo" );
    }

    protected void testAuthWithReaderUser() throws Exception
    {
        testAuthWithReaderUser( "neo", "abc123", null );
    }

    protected void testAuthWithPublisherUser() throws Exception
    {
        testAuthWithPublisherUser( "tank", "abc123", null );
    }

    protected void testCreateReaderUser( String username ) throws Exception
    {
        // NOTE: The default user 'neo4j' has password change required, so we have to first change it
        assertAuthAndChangePassword( "neo4j", "abc123", "123" );

        client.send( chunk(
                run( "CALL dbms.security.createUser( '" + username + "', '" + createdUserPassword + "', false ) " +
                     "CALL dbms.security.addRoleToUser( 'reader', '" + username + "' ) RETURN 0" ),
                pullAll() ) );

        assertThat( client, eventuallyReceives( msgSuccess(), msgRecord( eqRecord( equalTo( longValue( 0L ) ) ) ) ) );
    }

    protected void testAuthWithReaderUser( String username, String password, String realm ) throws Exception
    {
        assertAuth( username, password, realm );
        assertReadSucceeds();
        assertWriteFails( username, "reader" );
    }

    protected void testAuthWithPublisherUser( String username, String password, String realm ) throws Exception
    {
        assertAuth( username, password, realm );
        assertWriteSucceeds();
    }

    protected void testAuthWithNoPermissionUser( String username, String password ) throws Exception
    {
        assertAuth( username, password );
        assertReadFails( username, "" );
    }

    protected void assertAuth( String username, String password ) throws Exception
    {
        assertConnectionSucceeds( authToken( username, password, null ) );
    }

    protected void assertAuthAndChangePassword( String username, String password, String newPassword ) throws Exception
    {
        assertAuth( username, password );
        String query = format( "CALL dbms.security.changeUserPassword('%s', '%s', false)", username, newPassword );
        client.send( chunk( run( query ), pullAll() ) );
        assertThat( client, eventuallyReceives( msgSuccess(), msgSuccess() ) );
    }

    protected void assertAuth( String username, String password, String realm ) throws Exception
    {
        assertConnectionSucceeds( authToken( username, password, realm ) );
    }

    protected void assertAuthFail( String username, String password ) throws Exception
    {
        assertConnectionFails( map( "principal", username, "credentials", password, "scheme", "basic" ) );
    }

    protected void assertRoles( String... roles ) throws Exception
    {
        client.send( TransportTestUtil.chunk( run( "CALL dbms.showCurrentUser" ), pullAll() ) );

        // Then
        assertThat( client, eventuallyReceives(
                msgSuccess(),
                msgRecord( eqRecord( equalTo( stringValue( "tank" ) ),
                        containsInAnyOrder( stream( roles ).map( Values::stringValue ).toArray() ), anything() ) ),
                msgSuccess() ) );
    }

    protected void assertConnectionSucceeds( Map<String,Object> authToken ) throws Exception
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( chunk(
                        init( "TestClient/1.1", authToken ) ) );

        // Then
        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives( msgSuccess() ) );
    }

    protected void assertConnectionFails( Map<String,Object> authToken ) throws Exception
    {
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( chunk(
                        init( "TestClient/1.1", authToken ) ) );

        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives( msgFailure( Status.Security.Unauthorized,
                "The client is unauthorized due to authentication failure." ) ) );
        assertThat( client, eventuallyDisconnects() );
    }

    protected void assertReadSucceeds() throws Exception
    {
        // When
        client.send( TransportTestUtil.chunk( run( "MATCH (n) RETURN count(n)" ),
                pullAll() ) );

        // Then
        assertThat( client, eventuallyReceives(
                msgSuccess(),
                msgRecord( eqRecord( greaterThanOrEqualTo( 0L ) ) ),
                msgSuccess() ) );
    }

    protected void assertReadFails( String username, String roles ) throws Exception
    {
        // When
        client.send( chunk(
                run( "MATCH (n) RETURN n" ),
                pullAll() ) );

        String roleString = StringUtils.isEmpty( roles ) ? "no roles" : "roles [" + roles + "]";

        // Then
        assertThat( client, eventuallyReceives(
                msgFailure( Status.Security.Forbidden,
                        format( "Read operations are not allowed for user '%s' with %s.", username, roleString ) ) ) );
    }

    protected void assertWriteSucceeds() throws Exception
    {
        // When
        client.send( chunk(
                run( "CREATE ()" ),
                pullAll() ) );

        // Then
        assertThat( client, eventuallyReceives( msgSuccess(), msgSuccess() ) );
    }

    protected void assertWriteFails( String username, String roles ) throws Exception
    {
        // When
        client.send( chunk(
                run( "CREATE ()" ),
                pullAll() ) );

        String roleString = StringUtils.isEmpty( roles ) ? "no roles" : "roles [" + roles + "]";

        // Then
        assertThat( client, eventuallyReceives(
                msgFailure( Status.Security.Forbidden,
                        format( "Write operations are not allowed for user '%s' with %s.", username, roleString ) ) ) );
    }

    protected void assertBeginTransactionSucceeds() throws Exception
    {
        // When
        client.send( TransportTestUtil.chunk( run( "BEGIN" ),
                pullAll() ) );

        // Then
        assertThat( client, eventuallyReceives(
                msgSuccess(), msgSuccess() ) );
    }

    protected void assertCommitTransaction() throws Exception
    {
        // When
        client.send( chunk(
                run( "COMMIT" ),
                pullAll() ) );

        // Then
        assertThat( client, eventuallyReceives( msgSuccess(), msgSuccess() ) );
    }

    protected void assertQuerySucceeds( String query ) throws Exception
    {
        // When
        client.send( chunk(
                run( query ),
                pullAll() ) );

        // Then
        assertThat( client, eventuallyReceives( msgSuccess(), msgSuccess() ) );
    }

    protected Map<String,Object> authToken( String username, String password, String realm )
    {
        if ( realm != null && realm.length() > 0 )
        {
            return map( "principal", username, "credentials", password, "scheme", "basic", "realm", realm );
        }
        else
        {
            return map( "principal", username, "credentials", password, "scheme", "basic" );
        }
    }
}
