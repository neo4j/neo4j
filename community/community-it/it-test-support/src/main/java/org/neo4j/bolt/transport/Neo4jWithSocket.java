/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.nio.file.Path;
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
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.ssl.SelfSignedCertificateFactory;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.DISABLED;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.OPTIONAL;

public class Neo4jWithSocket
{
    static final String NEO4J_WITH_SOCKET = "org.neo4j.bolt.transport.Neo4jWithSocket";

    private final Supplier<FileSystemAbstraction> fileSystemProvider;
    private Consumer<Map<Setting<?>,Object>> configure;
    private final TestDirectory testDirectory;
    private TestDatabaseManagementServiceBuilder graphDatabaseFactory;
    private GraphDatabaseService gdb;
    private Path workingDirectory;
    private ConnectorPortRegister connectorRegister;
    private DatabaseManagementService managementService;

    public Neo4jWithSocket( TestDatabaseManagementServiceBuilder graphDatabaseFactory,
                            Supplier<TestDirectory> testDirectorySupplier, Consumer<Map<Setting<?>,Object>> configure )
    {
        this.testDirectory = testDirectorySupplier.get();
        this.graphDatabaseFactory = graphDatabaseFactory;
        this.fileSystemProvider = testDirectory::getFileSystem;
        this.configure = configure;
    }

    public FileSystemAbstraction getFileSystem()
    {
        return this.graphDatabaseFactory.getFileSystem();
    }

    public DatabaseManagementService getManagementService()
    {
        return managementService;
    }

    public void setConfigure( Consumer<Map<Setting<?>, Object>> configure )
    {
        this.configure = configure;
    }

    public void setGraphDatabaseFactory( TestDatabaseManagementServiceBuilder graphDatabaseFactory )
    {
        this.graphDatabaseFactory = graphDatabaseFactory;
    }

    public void init( TestInfo testInfo ) throws IOException
    {
        var testName = testInfo.getTestMethod().get().getName();
        testDirectory.prepareDirectory( testInfo.getTestClass().get(), testName );
        workingDirectory = testDirectory.directory( testName );

        ensureDatabase( settings -> {} );
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
        Path storeDir = workingDirectory.resolve( "storeDir" );

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
            var certificates = workingDirectory.resolve( "certificates" );
            SelfSignedCertificateFactory.create( certificates );

            settings.put( SslPolicyConfig.forScope( SslPolicyScope.BOLT ).enabled, Boolean.TRUE );
            settings.put( SslPolicyConfig.forScope( SslPolicyScope.BOLT ).base_directory, certificates );
        }

        SslPolicyConfig clusterConfig = SslPolicyConfig.forScope( SslPolicyScope.CLUSTER );
        if ( settings.containsKey( clusterConfig.enabled ) )
        {
            var clusterCertificates = workingDirectory.resolve( "cluster-cert" );
            SelfSignedCertificateFactory.create( clusterCertificates );

            settings.put( SslPolicyConfig.forScope( SslPolicyScope.CLUSTER ).enabled, Boolean.TRUE );
            settings.put( SslPolicyConfig.forScope( SslPolicyScope.CLUSTER ).base_directory, clusterCertificates );
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

}
