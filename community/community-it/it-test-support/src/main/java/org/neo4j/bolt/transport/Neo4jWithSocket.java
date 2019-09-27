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
package org.neo4j.bolt.transport;

import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.configuration.ssl.SslPolicyConfig;
import org.neo4j.configuration.ssl.SslPolicyScope;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.ssl.SelfSignedCertificateFactory;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.DISABLED;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.OPTIONAL;

public class Neo4jWithSocket extends ExternalResource
{
    private final Supplier<FileSystemAbstraction> fileSystemProvider;
    private final Consumer<Map<Setting<?>,Object>> configure;
    private final TestDirectory testDirectory;
    private final TestDatabaseManagementServiceBuilder graphDatabaseFactory;
    private GraphDatabaseService gdb;
    private File workingDirectory;
    private ConnectorPortRegister connectorRegister;
    private DatabaseManagementService managementService;

    public Neo4jWithSocket( Class<?> testClass )
    {
        this( testClass, settings ->
        {
        } );
    }

    public Neo4jWithSocket( Class<?> testClass, Consumer<Map<Setting<?>,Object>> configure )
    {
        this( testClass, new TestDatabaseManagementServiceBuilder(), configure );
    }

    public Neo4jWithSocket( Class<?> testClass, TestDatabaseManagementServiceBuilder graphDatabaseFactory,
            Consumer<Map<Setting<?>,Object>> configure )
    {
        this( testClass, graphDatabaseFactory, EphemeralFileSystemAbstraction::new, configure );
    }

    public Neo4jWithSocket( Class<?> testClass, TestDatabaseManagementServiceBuilder graphDatabaseFactory,
            Supplier<FileSystemAbstraction> fileSystemProvider, Consumer<Map<Setting<?>,Object>> configure )
    {
        this.testDirectory = TestDirectory.testDirectory( testClass, fileSystemProvider.get() );
        this.graphDatabaseFactory = graphDatabaseFactory;
        this.fileSystemProvider = fileSystemProvider;
        this.configure = configure;
        this.workingDirectory = defaultWorkingDirectory();
    }

    public FileSystemAbstraction getFileSystem()
    {
        return this.graphDatabaseFactory.getFileSystem();
    }

    public File getWorkingDirectory()
    {
        return workingDirectory;
    }

    public DatabaseManagementService getManagementService()
    {
        return managementService;
    }

    @Override
    public Statement apply( final Statement statement, final Description description )
    {
        Statement testMethod = new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                // If this is used as class rule then getMethodName() returns null, so use
                // getClassName() instead.
                String name =
                        description.getMethodName() != null ? description.getMethodName() : description.getClassName();
                workingDirectory = testDirectory.directory( name );
                ensureDatabase( settings -> {} );
                try
                {
                    statement.evaluate();
                }
                finally
                {
                    shutdownDatabase();
                }
            }
        };

        Statement testMethodWithBeforeAndAfter = super.apply( testMethod, description );

        return testDirectory.apply( testMethodWithBeforeAndAfter, description );
    }

    public HostnamePort lookupConnector( String connectorKey )
    {
        return connectorRegister.getLocalAddress( connectorKey );
    }

    public HostnamePort lookupDefaultConnector()
    {
        return connectorRegister.getLocalAddress( BoltConnector.NAME );
    }

    public void shutdownDatabase()
    {
        try
        {
            if ( managementService != null )
            {
                managementService.shutdown();
            }
        }
        finally
        {
            connectorRegister = null;
            gdb = null;
            managementService = null;
        }
    }

    public void ensureDatabase( Consumer<Map<Setting<?>,Object>> overrideSettingsFunction )
    {
        if ( gdb != null )
        {
            return;
        }

        Map<Setting<?>,Object> settings = configure( overrideSettingsFunction );
        File storeDir = new File( workingDirectory, "storeDir" );

        installSelfSignedCertificateIfEncryptionEnabled( settings );

        graphDatabaseFactory.setFileSystem( fileSystemProvider.get() );
        managementService = graphDatabaseFactory.setDatabaseRootDirectory( storeDir ).impermanent().setConfig( settings ).build();
        gdb = managementService.database( DEFAULT_DATABASE_NAME );
        connectorRegister =
                ((GraphDatabaseAPI) gdb).getDependencyResolver().resolveDependency( ConnectorPortRegister.class );
    }

    private void installSelfSignedCertificateIfEncryptionEnabled( Map<Setting<?>,Object> settings )
    {
        var encryptionLevel = settings.get( BoltConnector.encryption_level );
        if ( encryptionLevel != DISABLED )
        {
            // Install self-signed certs if ssl is enabled
            var certificates = new File( workingDirectory, "certificates" );
            SelfSignedCertificateFactory.create( certificates );

            settings.put( SslPolicyConfig.forScope( SslPolicyScope.BOLT ).enabled, Boolean.TRUE );
            settings.put( SslPolicyConfig.forScope( SslPolicyScope.BOLT ).base_directory, certificates.toPath() );
        }
    }

    private Map<Setting<?>,Object> configure( Consumer<Map<Setting<?>,Object>> overrideSettingsFunction )
    {
        Map<Setting<?>,Object> settings = new HashMap<>();
        settings.put( GraphDatabaseSettings.auth_enabled, false );
        settings.put( BoltConnector.enabled, true );
        settings.put( BoltConnector.listen_address, new SocketAddress( "localhost", 0 ) );
        settings.put( BoltConnector.encryption_level, DISABLED );
        configure.accept( settings );
        overrideSettingsFunction.accept( settings );
        return settings;
    }

    public static Consumer<Map<Setting<?>,Object>> withOptionalBoltEncryption()
    {
        return settings -> settings.put( BoltConnector.encryption_level, OPTIONAL );
    }

    public GraphDatabaseService graphDatabaseService()
    {
        return gdb;
    }

    private File defaultWorkingDirectory()
    {
        try
        {
            return testDirectory.prepareDirectoryForTest( "default" );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
