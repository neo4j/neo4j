/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.server.security.enterprise.auth;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.neo4j.bolt.security.auth.AuthenticationException;
import org.neo4j.bolt.v1.messaging.Neo4jPackV1;
import org.neo4j.bolt.v1.messaging.message.FailureMessage;
import org.neo4j.bolt.v1.messaging.message.InitMessage;
import org.neo4j.bolt.v1.messaging.message.PullAllMessage;
import org.neo4j.bolt.v1.messaging.message.RecordMessage;
import org.neo4j.bolt.v1.messaging.message.ResponseMessage;
import org.neo4j.bolt.v1.messaging.message.RunMessage;
import org.neo4j.bolt.v1.messaging.message.SuccessMessage;
import org.neo4j.bolt.v1.transport.integration.Neo4jWithSocket;
import org.neo4j.bolt.v1.transport.integration.TransportTestUtil;
import org.neo4j.bolt.v1.transport.socket.client.SocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.TransportConnection;
import org.neo4j.function.Factory;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.bolt.v1.messaging.message.ResetMessage.reset;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.api.security.AuthToken.BASIC_SCHEME;
import static org.neo4j.kernel.api.security.AuthToken.CREDENTIALS;
import static org.neo4j.kernel.api.security.AuthToken.NATIVE_REALM;
import static org.neo4j.kernel.api.security.AuthToken.PRINCIPAL;
import static org.neo4j.kernel.api.security.AuthToken.REALM_KEY;
import static org.neo4j.kernel.api.security.AuthToken.SCHEME_KEY;
import static org.neo4j.kernel.api.security.AuthToken.newBasicAuthToken;

class BoltInteraction implements NeoInteractionLevel<BoltInteraction.BoltSubject>
{
    private final TransportTestUtil util = new TransportTestUtil( new Neo4jPackV1() );
    private final Factory<TransportConnection> connectionFactory = SocketConnection::new;
    private final Neo4jWithSocket server;
    private Map<String,BoltSubject> subjects = new HashMap<>();
    private FileSystemAbstraction fileSystem;
    private EnterpriseAuthManager authManager;

    BoltInteraction( Map<String,String> config )
    {
        this( config, EphemeralFileSystemAbstraction::new );
    }

    BoltInteraction( Map<String,String> config, Supplier<FileSystemAbstraction> fileSystemSupplier )
    {
        TestEnterpriseGraphDatabaseFactory factory = new TestEnterpriseGraphDatabaseFactory();
        fileSystem = fileSystemSupplier.get();
        server = new Neo4jWithSocket( getClass(),
                factory,
                () -> fileSystem,
                settings ->
                {
                    settings.put( GraphDatabaseSettings.auth_enabled.name(), "true" );
                    settings.putAll( config );
                } );
        server.ensureDatabase( r ->
        {
        } );
        GraphDatabaseFacade db = (GraphDatabaseFacade) server.graphDatabaseService();
        authManager = db.getDependencyResolver().resolveDependency( EnterpriseAuthManager.class );
    }

    @Override
    public EnterpriseUserManager getLocalUserManager() throws Exception
    {
        if ( authManager instanceof EnterpriseAuthAndUserManager )
        {
            return ((EnterpriseAuthAndUserManager) authManager).getUserManager();
        }
        throw new Exception( "The used configuration does not have a user manager" );
    }

    @Override
    public GraphDatabaseFacade getLocalGraph()
    {
        return (GraphDatabaseFacade) server.graphDatabaseService();
    }

    @Override
    public FileSystemAbstraction fileSystem()
    {
        return fileSystem;
    }

    @Override
    public InternalTransaction beginLocalTransactionAsUser( BoltSubject subject, KernelTransaction.Type txType )
            throws Throwable
    {
        LoginContext loginContext = authManager.login( newBasicAuthToken( subject.username, subject.password ) );
        return getLocalGraph().beginTransaction( txType, loginContext );
    }

    @Override
    public String executeQuery( BoltSubject subject, String call, Map<String,Object> params,
            Consumer<ResourceIterator<Map<String,Object>>> resultConsumer )
    {
        if ( params == null )
        {
            params = Collections.emptyMap();
        }
        try
        {
            subject.client.send( util.chunk( RunMessage.run( call, ValueUtils.asMapValue( params ) ), PullAllMessage.pullAll() ) );
            resultConsumer.accept( collectResults( subject.client ) );
            return "";
        }
        catch ( Exception e )
        {
            return e.getMessage();
        }
    }

    @Override
    public BoltSubject login( String username, String password ) throws Exception
    {
        BoltSubject subject = subjects.get( username );
        if ( subject == null )
        {
            subject = new BoltSubject( connectionFactory.newInstance(), username, password );
            subjects.put( username, subject );
        }
        else
        {
            subject.client.disconnect();
            subject.client = connectionFactory.newInstance();
        }
        subject.client.connect( server.lookupDefaultConnector() )
                .send( util.acceptedVersions( 1, 0, 0, 0 ) )
                .send( util.chunk( InitMessage.init( "TestClient/1.1",
                        map( REALM_KEY, NATIVE_REALM, PRINCIPAL, username, CREDENTIALS, password,
                                SCHEME_KEY, BASIC_SCHEME ) ) ) );
        assertThat( subject.client, util.eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        subject.setLoginResult( util.receiveOneResponseMessage( subject.client ) );
        return subject;
    }

    @Override
    public void logout( BoltSubject subject ) throws Exception
    {
        subject.client.disconnect();
        subject.client = connectionFactory.newInstance();
    }

    @Override
    public void updateAuthToken( BoltSubject subject, String username, String password )
    {

    }

    @Override
    public String nameOf( BoltSubject subject )
    {
        return subject.username;
    }

    @Override
    public void tearDown() throws Throwable
    {
        for ( BoltSubject subject : subjects.values() )
        {
            subject.client.disconnect();
        }
        subjects.clear();
        server.graphDatabaseService().shutdown();
        fileSystem.close();
    }

    @Override
    public void assertAuthenticated( BoltSubject subject )
    {
        assertTrue( "Should be authenticated", subject.isAuthenticated() );
    }

    @Override
    public void assertPasswordChangeRequired( BoltSubject subject )
    {
        assertTrue( "Should need to change password", subject.passwordChangeRequired() );
    }

    @Override
    public void assertInitFailed( BoltSubject subject )
    {
        assertFalse( "Should not be authenticated", subject.isAuthenticated() );
    }

    @Override
    public void assertSessionKilled( BoltSubject subject )
    {
        assertThat( subject.client, util.eventuallyDisconnects() );
    }

    @Override
    public String getConnectionProtocol()
    {
        return "bolt";
    }

    @Override
    public HostnamePort lookupConnector( String connectorKey )
    {
        return server.lookupConnector( connectorKey );
    }

    private BoltResult collectResults( TransportConnection client ) throws Exception
    {
        ResponseMessage message = util.receiveOneResponseMessage( client );
        List<String> fieldNames = new ArrayList<>();
        List<Map<String,Object>> result = new ArrayList<>();

        if ( message instanceof SuccessMessage )
        {
            MapValue metadata = ((SuccessMessage) message).meta();
            ListValue fieldNameValues = (ListValue) metadata.get( "fields" );
            for ( AnyValue value : fieldNameValues )
            {
                fieldNames.add( ((TextValue) value).stringValue() );
            }
        }
        else if ( message instanceof FailureMessage )
        {
            FailureMessage failMessage = (FailureMessage) message;
            // drain ignoredMessage, ack failure, get successMessage
            util.receiveOneResponseMessage( client );
            client.send( util.chunk( reset() ) );
            util.receiveOneResponseMessage( client );
            throw new AuthenticationException( failMessage.status(), failMessage.message() );
        }

        do
        {
            message = util.receiveOneResponseMessage( client );
            if ( message instanceof RecordMessage )
            {
                Object[] row = ((RecordMessage) message).record().fields();
                Map<String,Object> rowMap = new HashMap<>();
                for ( int i = 0; i < row.length; i++ )
                {
                    rowMap.put( fieldNames.get( i ), row[i] );
                }
                result.add( rowMap );
            }
        }
        while ( !(message instanceof SuccessMessage) && !(message instanceof FailureMessage) );

        if ( message instanceof FailureMessage )
        {
            FailureMessage failMessage = (FailureMessage) message;
            // ack failure, get successMessage
            client.send( util.chunk( reset() ) );
            util.receiveOneResponseMessage( client );
            throw new AuthenticationException( failMessage.status(), failMessage.message() );
        }

        return new BoltResult( result );
    }

    static class BoltSubject
    {
        TransportConnection client;
        String username;
        String password;
        AuthenticationResult loginResult = AuthenticationResult.FAILURE;

        BoltSubject( TransportConnection client, String username, String password )
        {
            this.client = client;
            this.username = username;
            this.password = password;
        }

        void setLoginResult( ResponseMessage result )
        {
            if ( result instanceof SuccessMessage )
            {
                MapValue meta = ((SuccessMessage) result).meta();
                if ( meta.containsKey( "credentials_expired" ) &&
                     meta.get( "credentials_expired" ).equals( Values.TRUE ) )
                {
                    loginResult = AuthenticationResult.PASSWORD_CHANGE_REQUIRED;
                }
                else
                {
                    loginResult = AuthenticationResult.SUCCESS;
                }
            }
            else if ( result instanceof FailureMessage )
            {
                loginResult = AuthenticationResult.FAILURE;
                Status status = ((FailureMessage) result).status();
                if ( status.equals( Status.Security.AuthenticationRateLimit ) )
                {
                    loginResult = AuthenticationResult.TOO_MANY_ATTEMPTS;
                }
            }
        }

        boolean isAuthenticated()
        {
            return loginResult.equals( AuthenticationResult.SUCCESS );
        }

        boolean passwordChangeRequired()
        {
            return loginResult.equals( AuthenticationResult.PASSWORD_CHANGE_REQUIRED );
        }
    }

    static class BoltResult implements ResourceIterator<Map<String,Object>>
    {
        private int index;
        private List<Map<String,Object>> data;

        BoltResult( List<Map<String,Object>> data )
        {
            this.data = data;
        }

        @Override
        public void close()
        {
            index = data.size();
        }

        @Override
        public boolean hasNext()
        {
            return index < data.size();
        }

        @Override
        public Map<String,Object> next()
        {
            Map<String,Object> row = data.get( index );
            index++;
            return row;
        }
    }
}
