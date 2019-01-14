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
package org.neo4j.server.helpers;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.HttpConnector;
import org.neo4j.kernel.configuration.HttpConnector.Encryption;
import org.neo4j.kernel.configuration.ssl.LegacySslPolicyConfig;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.ServerTestUtils;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.LifecycleManagingDatabase;
import org.neo4j.server.preflight.PreFlightTasks;
import org.neo4j.server.rest.paging.LeaseManager;
import org.neo4j.server.rest.web.DatabaseActions;
import org.neo4j.server.rest.web.ScriptExecutionMode;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.time.Clocks;

import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.server.ServerTestUtils.asOneLine;
import static org.neo4j.server.database.LifecycleManagingDatabase.lifecycleManagingDatabase;

public class CommunityServerBuilder
{
    protected static final ListenSocketAddress ANY_ADDRESS = new ListenSocketAddress( "localhost", 0 );

    protected final LogProvider logProvider;
    private ListenSocketAddress address = new ListenSocketAddress( "localhost", 7474 );
    private ListenSocketAddress httpsAddress = new ListenSocketAddress( "localhost", 7473 );
    private String maxThreads;
    private String dataDir;
    private String managementUri = "/db/manage/";
    private String restUri = "/db/data/";
    private PreFlightTasks preflightTasks;
    private final HashMap<String, String> thirdPartyPackages = new HashMap<>();
    private final Properties arbitraryProperties = new Properties();

    static
    {
        System.setProperty( "sun.net.http.allowRestrictedHeaders", "true" );
    }

    private static LifecycleManagingDatabase.GraphFactory IN_MEMORY_DB = ( config, dependencies ) ->
    {
        File storeDir = config.get( GraphDatabaseSettings.database_path );
        config.augment( stringMap( GraphDatabaseFacadeFactory.Configuration.ephemeral.name(), "true",
                new BoltConnector( "bolt" ).listen_address.name(), "localhost:0" ) );
        return new ImpermanentGraphDatabase( storeDir, config,
                GraphDatabaseDependencies.newDependencies( dependencies ) );
    };

    private Clock clock;
    private String[] autoIndexedNodeKeys;
    private final String[] autoIndexedRelationshipKeys = null;
    private String[] securityRuleClassNames;
    private boolean persistent;
    private boolean httpsEnabled;

    public static CommunityServerBuilder server( LogProvider logProvider )
    {
        return new CommunityServerBuilder( logProvider );
    }

    public static CommunityServerBuilder server()
    {
        return new CommunityServerBuilder( NullLogProvider.getInstance() );
    }

    public static CommunityServerBuilder serverOnRandomPorts()
    {
        return server().onRandomPorts();
    }

    public CommunityNeoServer build() throws IOException
    {
        if ( dataDir == null && persistent )
        {
            throw new IllegalStateException( "Must specify path" );
        }
        final File configFile = buildBefore();

        Log log = logProvider.getLog( getClass() );
        Config config = Config.fromFile( configFile ).withServerDefaults().build();
        config.setLogger( log );
        return build( configFile, config, GraphDatabaseDependencies.newDependencies().userLogProvider( logProvider )
                .monitors( new Monitors() ) );
    }

    protected CommunityNeoServer build( File configFile, Config config,
            GraphDatabaseFacadeFactory.Dependencies dependencies )
    {
        return new TestCommunityNeoServer( config, configFile, dependencies, logProvider );
    }

    public File createConfigFiles() throws IOException
    {
        File temporaryConfigFile = ServerTestUtils.createTempConfigFile();
        File temporaryFolder = ServerTestUtils.createTempDir();

        ServerTestUtils.writeConfigToFile( createConfiguration( temporaryFolder ), temporaryConfigFile );

        return temporaryConfigFile;
    }

    public CommunityServerBuilder withClock( Clock clock )
    {
        this.clock = clock;
        return this;
    }

    public Map<String, String> createConfiguration( File temporaryFolder )
    {
        Map<String, String> properties = stringMap(
                ServerSettings.management_api_path.name(), managementUri,
                ServerSettings.rest_api_path.name(), restUri );

        ServerTestUtils.addDefaultRelativeProperties( properties, temporaryFolder );

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

        if ( autoIndexedNodeKeys != null && autoIndexedNodeKeys.length > 0 )
        {
            properties.put( "dbms.auto_index.nodes.enabled", "true" );
            String propertyKeys = org.apache.commons.lang.StringUtils.join( autoIndexedNodeKeys, "," );
            properties.put( "dbms.auto_index.nodes.keys", propertyKeys );
        }

        if ( autoIndexedRelationshipKeys != null && autoIndexedRelationshipKeys.length > 0 )
        {
            properties.put( "dbms.auto_index.relationships.enabled", "true" );
            String propertyKeys = org.apache.commons.lang.StringUtils.join( autoIndexedRelationshipKeys, "," );
            properties.put( "dbms.auto_index.relationships.keys", propertyKeys );
        }

        if ( securityRuleClassNames != null && securityRuleClassNames.length > 0 )
        {
            String propertyKeys = org.apache.commons.lang.StringUtils.join( securityRuleClassNames, "," );
            properties.put( ServerSettings.security_rules.name(), propertyKeys );
        }

        HttpConnector httpConnector = new HttpConnector( "http", Encryption.NONE );
        HttpConnector httpsConnector = new HttpConnector( "https", Encryption.TLS );

        properties.put( httpConnector.type.name(), "HTTP" );
        properties.put( httpConnector.enabled.name(), "true" );
        properties.put( httpConnector.address.name(), address.toString() );
        properties.put( httpConnector.encryption.name(), "NONE" );

        properties.put( httpsConnector.type.name(), "HTTP" );
        properties.put( httpsConnector.enabled.name(), String.valueOf( httpsEnabled ) );
        properties.put( httpsConnector.address.name(), httpsAddress.toString() );
        properties.put( httpsConnector.encryption.name(), "TLS" );

        properties.put( GraphDatabaseSettings.auth_enabled.name(), "false" );
        properties.put( LegacySslPolicyConfig.certificates_directory.name(),
                new File( temporaryFolder, "certificates" ).getAbsolutePath() );
        properties.put( GraphDatabaseSettings.logs_directory.name(),
                new File( temporaryFolder, "logs" ).getAbsolutePath() );
        properties.put( GraphDatabaseSettings.logical_logs_location.name(),
                new File( temporaryFolder, "transaction-logs" ).getAbsolutePath() );
        properties.put( GraphDatabaseSettings.pagecache_memory.name(), "8m" );

        for ( Object key : arbitraryProperties.keySet() )
        {
            properties.put( String.valueOf( key ), String.valueOf( arbitraryProperties.get( key ) ) );
        }
        return properties;
    }

    protected CommunityServerBuilder( LogProvider logProvider )
    {
        this.logProvider = logProvider;
    }

    public CommunityServerBuilder persistent()
    {
        this.persistent = true;
        return this;
    }

    public CommunityServerBuilder withMaxJettyThreads( int maxThreads )
    {
        this.maxThreads = String.valueOf( maxThreads );
        return this;
    }

    public CommunityServerBuilder usingDataDir( String dataDir )
    {
        this.dataDir = dataDir;
        return this;
    }

    public CommunityServerBuilder withRelativeManagementApiUriPath( String uri )
    {
        try
        {
            URI theUri = new URI( uri );
            if ( theUri.isAbsolute() )
            {
                this.managementUri = theUri.getPath();
            }
            else
            {
                this.managementUri = theUri.toString();
            }
        }
        catch ( URISyntaxException e )
        {
            throw new RuntimeException( e );
        }
        return this;
    }

    public CommunityServerBuilder withRelativeRestApiUriPath( String uri )
    {
        try
        {
            URI theUri = new URI( uri );
            if ( theUri.isAbsolute() )
            {
                this.restUri = theUri.getPath();
            }
            else
            {
                this.restUri = theUri.toString();
            }
        }
        catch ( URISyntaxException e )
        {
            throw new RuntimeException( e );
        }
        return this;
    }

    public CommunityServerBuilder withDefaultDatabaseTuning()
    {
        return this;
    }

    public CommunityServerBuilder withThirdPartyJaxRsPackage( String packageName, String mountPoint )
    {
        thirdPartyPackages.put( packageName, mountPoint );
        return this;
    }

    public CommunityServerBuilder withAutoIndexingEnabledForNodes( String... keys )
    {
        autoIndexedNodeKeys = keys;
        return this;
    }

    public CommunityServerBuilder onRandomPorts()
    {
        this.onHttpsAddress( ANY_ADDRESS );
        this.onAddress( ANY_ADDRESS );
        return this;
    }

    public CommunityServerBuilder onAddress( ListenSocketAddress address )
    {
        this.address = address;
        return this;
    }

    public CommunityServerBuilder onHttpsAddress( ListenSocketAddress address )
    {
        this.httpsAddress = address;
        return this;
    }

    public CommunityServerBuilder withSecurityRules( String... securityRuleClassNames )
    {
        this.securityRuleClassNames = securityRuleClassNames;
        return this;
    }

    public CommunityServerBuilder withHttpsEnabled()
    {
        httpsEnabled = true;
        return this;
    }

    public CommunityServerBuilder withProperty( String key, String value )
    {
        arbitraryProperties.put( key, value );
        return this;
    }

    protected DatabaseActions createDatabaseActionsObject( Database database, Config config )
    {
        Clock clockToUse = (clock != null) ? clock : Clocks.systemClock();

        return new DatabaseActions(
                new LeaseManager( clockToUse ),
                ScriptExecutionMode.getConfiguredMode( config ), database.getGraph() );
    }

    private File buildBefore() throws IOException
    {
        File configFile = createConfigFiles();

        if ( preflightTasks == null )
        {
            preflightTasks = new PreFlightTasks( NullLogProvider.getInstance() )
            {
                @Override
                public boolean run()
                {
                    return true;
                }
            };
        }
        return configFile;
    }

    private class TestCommunityNeoServer extends CommunityNeoServer
    {
        private final File configFile;

        private TestCommunityNeoServer( Config config, File configFile, GraphDatabaseFacadeFactory
                .Dependencies dependencies, LogProvider logProvider )
        {
            super( config, lifecycleManagingDatabase( persistent ? COMMUNITY_FACTORY : IN_MEMORY_DB ), dependencies,
                    logProvider );
            this.configFile = configFile;
        }

        @Override
        protected DatabaseActions createDatabaseActions()
        {
            return createDatabaseActionsObject( database, getConfig() );
        }

        @Override
        public void stop()
        {
            super.stop();
            if ( configFile != null )
            {
                configFile.delete();
            }
        }
    }
}
