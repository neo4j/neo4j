/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.transaction.xaframework.ForceMode;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.ServerTestUtils;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.PropertyFileConfigurator;
import org.neo4j.server.configuration.validation.DatabaseLocationMustBeSpecifiedRule;
import org.neo4j.server.configuration.validation.Validator;
import org.neo4j.server.database.CommunityDatabase;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.EphemeralDatabase;
import org.neo4j.server.preflight.PreFlightTasks;
import org.neo4j.server.preflight.PreflightTask;
import org.neo4j.server.rest.paging.LeaseManager;
import org.neo4j.server.rest.web.DatabaseActions;
import org.neo4j.tooling.Clock;
import org.neo4j.tooling.FakeClock;
import org.neo4j.tooling.RealClock;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;

import static org.neo4j.server.ServerTestUtils.asOneLine;
import static org.neo4j.server.ServerTestUtils.createTempPropertyFile;
import static org.neo4j.server.ServerTestUtils.writePropertiesToFile;
import static org.neo4j.server.ServerTestUtils.writePropertyToFile;

public class CommunityServerBuilder
{
    private String portNo = "7474";
    private String maxThreads = null;
    protected String dbDir = null;
    private String webAdminUri = "/db/manage/";
    private String webAdminDataUri = "/db/data/";
    protected PreFlightTasks preflightTasks;
    private final HashMap<String, String> thirdPartyPackages = new HashMap<>();
    private final Properties arbitraryProperties = new Properties();

    private static enum WhatToDo
    {
        CREATE_GOOD_TUNING_FILE,
        CREATE_DANGLING_TUNING_FILE_PROPERTY,
        CREATE_CORRUPT_TUNING_FILE
    }

    private WhatToDo action;
    protected Clock clock = null;
    private String[] autoIndexedNodeKeys = null;
    private String[] autoIndexedRelationshipKeys = null;
    private String host = null;
    private String[] securityRuleClassNames;
    public boolean persistent;
    private Boolean httpsEnabled = FALSE;

    public static CommunityServerBuilder server()
    {
        return new CommunityServerBuilder();
    }

    public CommunityNeoServer build() throws IOException
    {
        if ( dbDir == null && persistent )
        {
            throw new IllegalStateException( "Must specify path" );
        }
        final File configFile = createPropertiesFiles();

        if ( preflightTasks == null )
        {
            preflightTasks = new PreFlightTasks()
            {
                @Override
                public boolean run()
                {
                    return true;
                }
            };
        }

        return new TestCommunityNeoServer( new PropertyFileConfigurator( new Validator(
                new DatabaseLocationMustBeSpecifiedRule() ), configFile ), configFile );
    }

    public File createPropertiesFiles() throws IOException
    {
        File temporaryConfigFile = createTempPropertyFile();

        createPropertiesFile( temporaryConfigFile );
        createTuningFile( temporaryConfigFile );

        return temporaryConfigFile;
    }

    public CommunityServerBuilder withClock( Clock clock )
    {
        this.clock = clock;
        return this;
    }

    private void createPropertiesFile( File temporaryConfigFile )
    {
        Map<String, String> properties = MapUtil.stringMap(
                Configurator.MANAGEMENT_PATH_PROPERTY_KEY, webAdminUri,
                Configurator.REST_API_PATH_PROPERTY_KEY, webAdminDataUri );
        if ( dbDir != null )
        {
            properties.put( Configurator.DATABASE_LOCATION_PROPERTY_KEY, dbDir );
        }

        if ( portNo != null )
        {
            properties.put( Configurator.WEBSERVER_PORT_PROPERTY_KEY, portNo );
        }
        if ( host != null )
        {
            properties.put( Configurator.WEBSERVER_ADDRESS_PROPERTY_KEY, host );
        }
        if ( maxThreads != null )
        {
            properties.put( Configurator.WEBSERVER_MAX_THREADS_PROPERTY_KEY, maxThreads );
        }

        if ( thirdPartyPackages.keySet().size() > 0 )
        {
            properties.put( Configurator.THIRD_PARTY_PACKAGES_KEY, asOneLine( thirdPartyPackages ) );
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
            properties.put( Configurator.SECURITY_RULES_KEY, propertyKeys );
        }

        if ( httpsEnabled != null )
        {
            if ( httpsEnabled )
            {
                properties.put( Configurator.WEBSERVER_HTTPS_ENABLED_PROPERTY_KEY, "true" );
            }
            else
            {
                properties.put( Configurator.WEBSERVER_HTTPS_ENABLED_PROPERTY_KEY, "false" );
            }
        }

        for ( Object key : arbitraryProperties.keySet() )
        {
            properties.put( String.valueOf( key ), String.valueOf( arbitraryProperties.get( key ) ) );
        }

        ServerTestUtils.writePropertiesToFile( properties, temporaryConfigFile );
    }

    private void createTuningFile( File temporaryConfigFile ) throws IOException
    {
        if ( action == WhatToDo.CREATE_GOOD_TUNING_FILE )
        {
            File databaseTuningPropertyFile = createTempPropertyFile();
            Map<String, String> properties = MapUtil.stringMap(
                    "neostore.nodestore.db.mapped_memory", "25M",
                    "neostore.relationshipstore.db.mapped_memory", "50M",
                    "neostore.propertystore.db.mapped_memory", "90M",
                    "neostore.propertystore.db.strings.mapped_memory", "130M",
                    "neostore.propertystore.db.arrays.mapped_memory", "130M" );
            writePropertiesToFile( properties, databaseTuningPropertyFile );
            writePropertyToFile( Configurator.DB_TUNING_PROPERTY_FILE_KEY,
                    databaseTuningPropertyFile.getAbsolutePath(), temporaryConfigFile );
        }
        else if ( action == WhatToDo.CREATE_DANGLING_TUNING_FILE_PROPERTY )
        {
            writePropertyToFile( Configurator.DB_TUNING_PROPERTY_FILE_KEY, createTempPropertyFile().getAbsolutePath(),
                    temporaryConfigFile );
        }
        else if ( action == WhatToDo.CREATE_CORRUPT_TUNING_FILE )
        {
            File corruptTuningFile = trashFile();
            writePropertyToFile( Configurator.DB_TUNING_PROPERTY_FILE_KEY, corruptTuningFile.getAbsolutePath(),
                    temporaryConfigFile );
        }
    }

    private File trashFile() throws IOException
    {
        File f = createTempPropertyFile();

        try ( FileWriter fstream = new FileWriter( f, true ); BufferedWriter out = new BufferedWriter( fstream ) )
        {
            for ( int i = 0; i < 100; i++ )
            {
                out.write( (int) System.currentTimeMillis() );
            }
        }

        return f;
    }

    protected CommunityServerBuilder()
    {
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

    public CommunityServerBuilder usingDatabaseDir( String dbDir )
    {
        this.dbDir = dbDir;
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

    public CommunityServerBuilder withoutWebServerPort()
    {
        portNo = null;
        return this;
    }

    public CommunityServerBuilder withFailingPreflightTasks()
    {
        preflightTasks = new PreFlightTasks()
        {
            @Override
            public boolean run()
            {
                return false;
            }

            @Override
            public PreflightTask failedTask()
            {
                return new PreflightTask()
                {

                    @Override
                    public String getFailureMessage()
                    {
                        return "mockFailure";
                    }

                    @Override
                    public boolean run()
                    {
                        return false;
                    }
                };
            }
        };
        return this;
    }

    public CommunityServerBuilder withDefaultDatabaseTuning()
    {
        action = WhatToDo.CREATE_GOOD_TUNING_FILE;
        return this;
    }

    public CommunityServerBuilder withNonResolvableTuningFile()
    {
        action = WhatToDo.CREATE_DANGLING_TUNING_FILE_PROPERTY;
        return this;
    }

    public CommunityServerBuilder withCorruptTuningFile()
    {
        action = WhatToDo.CREATE_CORRUPT_TUNING_FILE;
        return this;
    }

    public CommunityServerBuilder withThirdPartyJaxRsPackage( String packageName, String mountPoint )
    {
        thirdPartyPackages.put( packageName, mountPoint );
        return this;
    }

    public CommunityServerBuilder withFakeClock()
    {
        clock = new FakeClock();
        return this;
    }

    public CommunityServerBuilder withAutoIndexingEnabledForNodes( String... keys )
    {
        autoIndexedNodeKeys = keys;
        return this;
    }

    public CommunityServerBuilder withAutoIndexingEnabledForRelationships( String... keys )
    {
        autoIndexedRelationshipKeys = keys;
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
        this.preflightTasks = new PreFlightTasks( tasks );
        return this;
    }

    private class TestCommunityNeoServer extends CommunityNeoServer
    {
        private final File configFile;

        public TestCommunityNeoServer( PropertyFileConfigurator propertyFileConfigurator, File configFile )
        {
            super( propertyFileConfigurator );
            this.configFile = configFile;
        }

        @Override
        protected PreFlightTasks createPreflightTasks()
        {
            return preflightTasks;
        }

        @Override
        protected Database createDatabase()
        {
            return persistent ?
                    new CommunityDatabase( configurator ) :
                    new EphemeralDatabase( configurator );
        }

        @Override
        protected DatabaseActions createDatabaseActions()
        {
            Clock clockToUse = (clock != null) ? clock : new RealClock();

            return new DatabaseActions(
                    new LeaseManager( clockToUse ),
                    ForceMode.forced,
                    configurator.configuration().getBoolean(
                            Configurator.SCRIPT_SANDBOXING_ENABLED_KEY,
                            Configurator.DEFAULT_SCRIPT_SANDBOXING_ENABLED ), database.getGraph() );
        }

        @Override
        public void stop()
        {
            super.stop();
            configFile.delete();
        }
    }

    ;
}
