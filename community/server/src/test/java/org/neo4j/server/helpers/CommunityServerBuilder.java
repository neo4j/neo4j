/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.server.helpers;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Clock;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.CommunityFacadeFactory;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.ServerTestUtils;
import org.neo4j.server.configuration.BaseServerConfigLoader;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.LifecycleManagingDatabase;
import org.neo4j.server.preflight.PreFlightTasks;
import org.neo4j.server.preflight.PreflightTask;
import org.neo4j.server.rest.paging.LeaseManager;
import org.neo4j.server.rest.web.DatabaseActions;
import org.neo4j.test.ImpermanentGraphDatabase;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;

import static org.neo4j.helpers.Clock.SYSTEM_CLOCK;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.server.ServerTestUtils.asOneLine;
import static org.neo4j.server.database.LifecycleManagingDatabase.lifecycleManagingDatabase;

public class CommunityServerBuilder
{
    protected final LogProvider logProvider;
    private String portNo = "7474";
    private String maxThreads = null;
    protected String dataDir = null;
    private String webAdminUri = "/db/manage/";
    private String webAdminDataUri = "/db/data/";
    protected PreFlightTasks preflightTasks;
    private final HashMap<String, String> thirdPartyPackages = new HashMap<>();
    private final Properties arbitraryProperties = new Properties();

    public static LifecycleManagingDatabase.GraphFactory  IN_MEMORY_DB = ( config, dependencies ) -> {
        File storeDir = config.get( ServerSettings.database_path );
        Map<String, String> params = config.getParams();
        params.put( CommunityFacadeFactory.Configuration.ephemeral.name(), "true" );
        return new ImpermanentGraphDatabase( storeDir, params, GraphDatabaseDependencies.newDependencies(dependencies) );
    };

    protected Clock clock = null;
    private String[] autoIndexedNodeKeys = null;
    private String[] autoIndexedRelationshipKeys = null;
    private String host = null;
    private String[] securityRuleClassNames;
    public boolean persistent;
    private Boolean httpsEnabled = FALSE;

    public static CommunityServerBuilder server( LogProvider logProvider )
    {
        return new CommunityServerBuilder( logProvider );
    }

    public static CommunityServerBuilder server()
    {
        return new CommunityServerBuilder( NullLogProvider.getInstance() );
    }

    public CommunityNeoServer build() throws IOException
    {
        if ( dataDir == null && persistent )
        {
            throw new IllegalStateException( "Must specify path" );
        }
        final File configFile = buildBefore();

        Log log = logProvider.getLog( getClass() );
        BaseServerConfigLoader configLoader = new BaseServerConfigLoader();
        Config config = configLoader.loadConfig( null, configFile, log );
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
        File temporaryFolder = temporaryConfigFile.getParentFile();

        ServerTestUtils.writeConfigToFile( createConfiguration( temporaryFolder ), temporaryConfigFile );

        return temporaryConfigFile;
    }

    public CommunityServerBuilder withClock( Clock clock )
    {
        this.clock = clock;
        return this;
    }

    private Map<String, String> createConfiguration( File temporaryFolder )
    {
        Map<String, String> properties = stringMap(
                ServerSettings.management_api_path.name(), webAdminUri,
                ServerSettings.rest_api_path.name(), webAdminDataUri );

        ServerTestUtils.addDefaultRelativeProperties( properties, temporaryFolder );

        if ( dataDir != null )
        {
            properties.put( ServerSettings.data_directory.name(), dataDir );
        }

        if ( portNo != null )
        {
            properties.put( ServerSettings.webserver_port.name(), portNo );
        }
        if ( host != null )
        {
            properties.put( ServerSettings.webserver_address.name(), host );
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
            properties.put( "node_auto_indexing", "true" );
            String propertyKeys = org.apache.commons.lang.StringUtils.join( autoIndexedNodeKeys, "," );
            properties.put( "node_keys_indexable", propertyKeys );
        }

        if ( autoIndexedRelationshipKeys != null && autoIndexedRelationshipKeys.length > 0 )
        {
            properties.put( "relationship_auto_indexing", "true" );
            String propertyKeys = org.apache.commons.lang.StringUtils.join( autoIndexedRelationshipKeys, "," );
            properties.put( "relationship_keys_indexable", propertyKeys );
        }

        if ( securityRuleClassNames != null && securityRuleClassNames.length > 0 )
        {
            String propertyKeys = org.apache.commons.lang.StringUtils.join( securityRuleClassNames, "," );
            properties.put( ServerSettings.security_rules.name(), propertyKeys );
        }

        if ( httpsEnabled != null )
        {
            if ( httpsEnabled )
            {
                properties.put( ServerSettings.webserver_https_enabled.name(), "true" );
            }
            else
            {
                properties.put( ServerSettings.webserver_https_enabled.name(), "false" );
            }
        }

        properties.put( ServerSettings.auth_enabled.name(), "false" );
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

    public CommunityServerBuilder onPort( int portNo )
    {
        this.portNo = String.valueOf( portNo );
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

    public CommunityServerBuilder withRelativeWebAdminUriPath( String webAdminUri )
    {
        try
        {
            URI theUri = new URI( webAdminUri );
            if ( theUri.isAbsolute() )
            {
                this.webAdminUri = theUri.getPath();
            }
            else
            {
                this.webAdminUri = theUri.toString();
            }
        }
        catch ( URISyntaxException e )
        {
            throw new RuntimeException( e );
        }
        return this;
    }

    public CommunityServerBuilder withRelativeWebDataAdminUriPath( String webAdminDataUri )
    {
        try
        {
            URI theUri = new URI( webAdminDataUri );
            if ( theUri.isAbsolute() )
            {
                this.webAdminDataUri = theUri.getPath();
            }
            else
            {
                this.webAdminDataUri = theUri.toString();
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

    public CommunityServerBuilder onHost( String host )
    {
        this.host = host;
        return this;
    }

    public CommunityServerBuilder withSecurityRules( String... securityRuleClassNames )
    {
        this.securityRuleClassNames = securityRuleClassNames;
        return this;
    }

    public CommunityServerBuilder withHttpsEnabled()
    {
        httpsEnabled = TRUE;
        return this;
    }

    public CommunityServerBuilder withProperty( String key, String value )
    {
        arbitraryProperties.put( key, value );
        return this;
    }

    public CommunityServerBuilder withPreflightTasks( PreflightTask... tasks )
    {
        this.preflightTasks = new PreFlightTasks( NullLogProvider.getInstance(), tasks );
        return this;
    }

    protected DatabaseActions createDatabaseActionsObject( Database database, Config config )
    {
        Clock clockToUse = (clock != null) ? clock : SYSTEM_CLOCK;

        return new DatabaseActions(
                new LeaseManager( clockToUse ),
                config.get( ServerSettings.script_sandboxing_enabled ), database.getGraph() );
    }

    protected File buildBefore() throws IOException
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
            configFile.delete();
        }
    }
}
