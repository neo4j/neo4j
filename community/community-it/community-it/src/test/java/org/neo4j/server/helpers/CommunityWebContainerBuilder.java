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
package org.neo4j.server.helpers;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.SettingValueParsers;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpsConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.configuration.ssl.ClientAuth;
import org.neo4j.configuration.ssl.SslPolicyConfig;
import org.neo4j.configuration.ssl.SslPolicyScope;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.WebContainerTestUtils;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.ssl.SelfSignedCertificateFactory;

import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.internal.helpers.collection.MapUtil.stringMap;
import static org.neo4j.server.WebContainerTestUtils.addDefaultRelativeProperties;
import static org.neo4j.server.WebContainerTestUtils.asOneLine;
import static org.neo4j.server.WebContainerTestUtils.writeConfigToFile;
import static org.neo4j.util.Preconditions.checkState;

public class CommunityWebContainerBuilder
{
    private static final SocketAddress ANY_ADDRESS = new SocketAddress( "localhost", 0 );

    private final LogProvider logProvider;
    private SocketAddress address = new SocketAddress( "localhost", HttpConnector.DEFAULT_PORT );
    private SocketAddress httpsAddress = new SocketAddress( "localhost", HttpsConnector.DEFAULT_PORT );
    private String maxThreads;
    private String dataDir;
    private String dbUri = "/db";
    private String restUri = "/db/data";
    private final Map<String, String> thirdPartyPackages = new HashMap<>();
    private final Properties arbitraryProperties = new Properties();

    static
    {
        System.setProperty( "sun.net.http.allowRestrictedHeaders", "true" );
    }

    private boolean persistent;
    private boolean httpEnabled = true;
    private boolean httpsEnabled;
    private DependencyResolver dependencies = new Dependencies();

    public static CommunityWebContainerBuilder builder( LogProvider logProvider )
    {
        return new CommunityWebContainerBuilder( logProvider );
    }

    public static CommunityWebContainerBuilder builder()
    {
        return new CommunityWebContainerBuilder( NullLogProvider.getInstance() );
    }

    public static CommunityWebContainerBuilder serverOnRandomPorts()
    {
        return builder().onRandomPorts();
    }

    public TestWebContainer build() throws IOException
    {
        checkState( dataDir != null || !persistent, "Must specify path" );
        final File configFile = createConfigFiles();

        Log log = logProvider.getLog( getClass() );
        Config config = Config.newBuilder()
                .setDefaults( GraphDatabaseSettings.SERVER_DEFAULTS )
                .fromFile( configFile )
                .build();
        config.setLogger( log );
        return new TestWebContainer( build( config ) );
    }

    private DatabaseManagementService build( Config config )
    {
        var managementServiceBuilder = createManagementServiceBuilder();
        if ( !persistent )
        {
            managementServiceBuilder = managementServiceBuilder.impermanent();
        }
        else
        {
            managementServiceBuilder.setDatabaseRootDirectory( new File( dataDir ) );
        }
        return managementServiceBuilder.setConfig( config )
                .setInternalLogProvider( logProvider )
                .setExternalDependencies( dependencies ).build();
    }

    protected TestDatabaseManagementServiceBuilder createManagementServiceBuilder()
    {
        return new TestDatabaseManagementServiceBuilder();
    }

    private File createConfigFiles() throws IOException
    {
        File testFolder = persistent ? new File( dataDir ) : WebContainerTestUtils.createTempDir();
        File temporaryConfigFile = WebContainerTestUtils.createTempConfigFile( testFolder );

        writeConfigToFile( createConfiguration( testFolder ), temporaryConfigFile );

        return temporaryConfigFile;
    }

    public Map<String, String> createConfiguration( File temporaryFolder )
    {
        Map<String, String> properties = stringMap(
                ServerSettings.db_api_path.name(), dbUri,
                ServerSettings.rest_api_path.name(), restUri
        );

        addDefaultRelativeProperties( properties, temporaryFolder );

        if ( dataDir != null )
        {
            properties.put( GraphDatabaseSettings.data_directory.name(), dataDir );
        }

        if ( maxThreads != null )
        {
            properties.put( ServerSettings.webserver_max_threads.name(), maxThreads );
        }

        if ( thirdPartyPackages.keySet().size() > 0 )
        {
            properties.put( ServerSettings.third_party_packages.name(), asOneLine( thirdPartyPackages ) );
        }

        properties.put( HttpConnector.enabled.name(), String.valueOf( httpEnabled ) );
        properties.put( HttpConnector.listen_address.name(), address.toString() );

        properties.put( HttpsConnector.enabled.name(), String.valueOf( httpsEnabled ) );
        properties.put( HttpsConnector.listen_address.name(), httpsAddress.toString() );

        properties.put( GraphDatabaseSettings.neo4j_home.name(), temporaryFolder.getAbsolutePath() );

        properties.put( GraphDatabaseSettings.auth_enabled.name(), FALSE );

        if ( httpsEnabled )
        {
            var certificates = new File( temporaryFolder, "certificates" );
            SelfSignedCertificateFactory.create( certificates );
            SslPolicyConfig policy = SslPolicyConfig.forScope( SslPolicyScope.HTTPS );
            properties.put( policy.enabled.name(), Boolean.TRUE.toString() );
            properties.put( policy.base_directory.name(), certificates.getAbsolutePath() );
            properties.put( policy.trust_all.name(), SettingValueParsers.TRUE );
            properties.put( policy.client_auth.name(), ClientAuth.NONE.name() );
        }

        properties.put( GraphDatabaseSettings.logs_directory.name(),
                new File( temporaryFolder, "logs" ).getAbsolutePath() );
        properties.put( GraphDatabaseSettings.transaction_logs_root_path.name(),
                new File( temporaryFolder, "transaction-logs" ).getAbsolutePath() );
        properties.put( GraphDatabaseSettings.pagecache_memory.name(), "8m" );
        properties.put( GraphDatabaseSettings.shutdown_transaction_end_timeout.name(), "0s" );

        for ( Object key : arbitraryProperties.keySet() )
        {
            properties.put( String.valueOf( key ), String.valueOf( arbitraryProperties.get( key ) ) );
        }
        return properties;
    }

    protected CommunityWebContainerBuilder( LogProvider logProvider )
    {
        this.logProvider = logProvider;
    }

    public CommunityWebContainerBuilder withDependencies( DependencyResolver dependencies )
    {
        this.dependencies = dependencies;
        return this;
    }

    public CommunityWebContainerBuilder persistent()
    {
        this.persistent = true;
        return this;
    }

    public CommunityWebContainerBuilder withMaxJettyThreads( int maxThreads )
    {
        this.maxThreads = String.valueOf( maxThreads );
        return this;
    }

    public CommunityWebContainerBuilder usingDataDir( String dataDir )
    {
        this.dataDir = dataDir;
        return this;
    }

    public CommunityWebContainerBuilder withRelativeDatabaseApiPath( String uri )
    {
        this.dbUri = getPath( uri );
        return this;
    }

    public CommunityWebContainerBuilder withRelativeRestApiPath( String uri )
    {
        this.restUri = getPath( uri );
        return this;
    }

    public CommunityWebContainerBuilder withDefaultDatabaseTuning()
    {
        return this;
    }

    public CommunityWebContainerBuilder withThirdPartyJaxRsPackage( String packageName, String mountPoint )
    {
        thirdPartyPackages.put( packageName, mountPoint );
        return this;
    }

    public CommunityWebContainerBuilder onRandomPorts()
    {
        this.onHttpsAddress( ANY_ADDRESS );
        this.onAddress( ANY_ADDRESS );
        return this;
    }

    public CommunityWebContainerBuilder onAddress( SocketAddress address )
    {
        this.address = address;
        return this;
    }

    public CommunityWebContainerBuilder onHttpsAddress( SocketAddress address )
    {
        this.httpsAddress = address;
        return this;
    }

    public CommunityWebContainerBuilder withHttpsEnabled()
    {
        httpsEnabled = true;
        return this;
    }

    public CommunityWebContainerBuilder withHttpDisabled()
    {
        httpEnabled = false;
        return this;
    }

    public CommunityWebContainerBuilder withProperty( String key, String value )
    {
        arbitraryProperties.put( key, value );
        return this;
    }

    private static String getPath( String uri )
    {
        URI theUri = URI.create( uri );
        if ( theUri.isAbsolute() )
        {
            return theUri.getPath();
        }
        else
        {
            return theUri.toString();
        }
    }
}
