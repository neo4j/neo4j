/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.security.enterprise.auth;

import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.ConnectorPortRegister;
import org.neo4j.kernel.configuration.ssl.LegacySslPolicyConfig;
import org.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;
import org.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.kernel.configuration.BoltConnector.EncryptionLevel.OPTIONAL;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;

public class EmbeddedInteraction implements NeoInteractionLevel<EnterpriseLoginContext>
{
    private GraphDatabaseFacade db;
    private EnterpriseAuthManager authManager;
    private FileSystemAbstraction fileSystem;
    private ConnectorPortRegister connectorRegister;

    EmbeddedInteraction( Map<String, String> config ) throws Throwable
    {
        this( config, EphemeralFileSystemAbstraction::new );
    }

    EmbeddedInteraction( Map<String, String> config, Supplier<FileSystemAbstraction> fileSystemSupplier ) throws Throwable
    {
        TestEnterpriseGraphDatabaseFactory factory = new TestEnterpriseGraphDatabaseFactory();
        factory.setFileSystem( fileSystemSupplier.get() );
        GraphDatabaseBuilder builder = factory.newImpermanentDatabaseBuilder();
        this.fileSystem = factory.getFileSystem();
        init( builder, config );
    }

    public EmbeddedInteraction( GraphDatabaseBuilder builder, Map<String, String> config ) throws Throwable
    {
        init( builder, config );
    }

    private void init( GraphDatabaseBuilder builder, Map<String, String> config ) throws Throwable
    {
        builder.setConfig( new BoltConnector( "bolt" ).type, "BOLT" );
        builder.setConfig( new BoltConnector( "bolt" ).enabled, "true" );
        builder.setConfig( new BoltConnector( "bolt" ).encryption_level, OPTIONAL.name() );
        builder.setConfig( new BoltConnector( "bolt" ).listen_address, "localhost:0" );
        builder.setConfig( LegacySslPolicyConfig.tls_key_file, NeoInteractionLevel.tempPath( "key", ".key" ) );
        builder.setConfig( LegacySslPolicyConfig.tls_certificate_file,
                NeoInteractionLevel.tempPath( "cert", ".cert" ) );
        builder.setConfig( GraphDatabaseSettings.auth_enabled, "true" );

        builder.setConfig( config );

        db = (GraphDatabaseFacade) builder.newGraphDatabase();
        authManager = db.getDependencyResolver().resolveDependency( EnterpriseAuthManager.class );
        connectorRegister = db.getDependencyResolver().resolveDependency( ConnectorPortRegister.class );
    }

    @Override
    public EnterpriseUserManager getLocalUserManager() throws Exception
    {
        if ( authManager instanceof EnterpriseAuthAndUserManager )
        {
            return ((EnterpriseAuthAndUserManager) authManager).getUserManager();
        }
        throw new Exception( "The configuration used does not have a user manager" );
    }

    @Override
    public GraphDatabaseFacade getLocalGraph()
    {
        return db;
    }

    @Override
    public FileSystemAbstraction fileSystem()
    {
        return fileSystem;
    }

    @Override
    public InternalTransaction beginLocalTransactionAsUser( EnterpriseLoginContext loginContext, KernelTransaction.Type txType )
    {
        return db.beginTransaction( txType, loginContext );
    }

    @Override
    public String executeQuery( EnterpriseLoginContext loginContext, String call, Map<String,Object> params,
            Consumer<ResourceIterator<Map<String, Object>>> resultConsumer )
    {
        try ( InternalTransaction tx = db.beginTransaction( KernelTransaction.Type.implicit, loginContext ) )
        {
            Map<String,Object> p = (params == null) ? Collections.emptyMap() : params;
            resultConsumer.accept( db.execute( call, p ) );
            tx.success();
            return "";
        }
        catch ( Exception e )
        {
            return e.getMessage();
        }
    }

    @Override
    public EnterpriseLoginContext login( String username, String password ) throws Exception
    {
        return authManager.login( authToken( username, password ) );
    }

    @Override
    public void logout( EnterpriseLoginContext loginContext )
    {
        loginContext.subject().logout();
    }

    @Override
    public void updateAuthToken( EnterpriseLoginContext subject, String username, String password )
    {
    }

    @Override
    public String nameOf( EnterpriseLoginContext loginContext )
    {
        return loginContext.subject().username();
    }

    @Override
    public void tearDown()
    {
        db.shutdown();
    }

    @Override
    public void assertAuthenticated( EnterpriseLoginContext loginContext )
    {
        assertThat( loginContext.subject().getAuthenticationResult(), equalTo( AuthenticationResult.SUCCESS ) );
    }

    @Override
    public void assertPasswordChangeRequired( EnterpriseLoginContext loginContext )
    {
        assertThat( loginContext.subject().getAuthenticationResult(), equalTo( AuthenticationResult.PASSWORD_CHANGE_REQUIRED ) );
    }

    @Override
    public void assertInitFailed( EnterpriseLoginContext loginContext )
    {
        assertThat( loginContext.subject().getAuthenticationResult(), equalTo( AuthenticationResult.FAILURE ) );
    }

    @Override
    public void assertSessionKilled( EnterpriseLoginContext loginContext )
    {
        // There is no session that could have been killed
    }

    @Override
    public String getConnectionProtocol()
    {
        return "embedded";
    }

    @Override
    public HostnamePort lookupConnector( String connectorKey )
    {
        return connectorRegister.getLocalAddress( connectorKey );
    }
}
