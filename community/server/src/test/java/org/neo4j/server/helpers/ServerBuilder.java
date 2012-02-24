/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static org.neo4j.server.ServerTestUtils.asOneLine;
import static org.neo4j.server.ServerTestUtils.createTempDir;
import static org.neo4j.server.ServerTestUtils.createTempPropertyFile;
import static org.neo4j.server.ServerTestUtils.writePropertiesToFile;
import static org.neo4j.server.ServerTestUtils.writePropertyToFile;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.server.Bootstrapper;
import org.neo4j.server.EphemeralNeoServerBootstrapper;
import org.neo4j.server.NeoServerBootstrapper;
import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.ServerTestUtils;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.PropertyFileConfigurator;
import org.neo4j.server.configuration.validation.DatabaseLocationMustBeSpecifiedRule;
import org.neo4j.server.configuration.validation.Validator;
import org.neo4j.server.modules.DiscoveryModule;
import org.neo4j.server.modules.ManagementApiModule;
import org.neo4j.server.modules.RESTApiModule;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.server.modules.ThirdPartyJAXRSModule;
import org.neo4j.server.modules.WebAdminModule;
import org.neo4j.server.rest.paging.Clock;
import org.neo4j.server.rest.paging.FakeClock;
import org.neo4j.server.rest.paging.LeaseManagerProvider;
import org.neo4j.server.startup.healthcheck.StartupHealthCheck;
import org.neo4j.server.startup.healthcheck.StartupHealthCheckRule;
import org.neo4j.server.web.Jetty6WebServer;

public class ServerBuilder
{
    private String portNo = "7474";
    private String maxThreads = null;
    private String dbDir = null;
    private String webAdminUri = "/db/manage/";
    private String webAdminDataUri = "/db/data/";
    private StartupHealthCheck startupHealthCheck;
    private final HashMap<String, String> thirdPartyPackages = new HashMap<String, String>();

    private static enum WhatToDo
    {
        CREATE_GOOD_TUNING_FILE,
        CREATE_DANGLING_TUNING_FILE_PROPERTY,
        CREATE_CORRUPT_TUNING_FILE
    };

    private WhatToDo action;
    private List<Class<? extends ServerModule>> serverModules = null;
    private Clock clock = null;
    private String[] autoIndexedNodeKeys = null;
    private String[] autoIndexedRelationshipKeys = null;
    private String host = null;
    private String[] securityRuleClassNames;
    private boolean persistent;
    private Boolean httpsEnabled = false;

    public static ServerBuilder server()
    {
        return new ServerBuilder();
    }
    
    @SuppressWarnings( "unchecked" )
    public NeoServerWithEmbeddedWebServer build() throws IOException
    {
        if ( dbDir == null )
        {
            this.dbDir = createTempDir().getAbsolutePath();
        }
        File configFile = createPropertiesFiles();

        if ( serverModules == null )
        {
            withSpecificServerModulesOnly( RESTApiModule.class, WebAdminModule.class, ManagementApiModule.class,
                    ThirdPartyJAXRSModule.class, DiscoveryModule.class );
        }

        if ( startupHealthCheck == null )
        {
            startupHealthCheck = new StartupHealthCheck()
            {
                public boolean run()
                {
                    return true;
                }
            };
        }

        if ( clock != null )
        {
            LeaseManagerProvider.setClock( clock );
        }

        return new NeoServerWithEmbeddedWebServer( createBootstrapper(), startupHealthCheck,
                new PropertyFileConfigurator( new Validator( new DatabaseLocationMustBeSpecifiedRule() ), configFile ),
                new Jetty6WebServer(), serverModules );
    }

    protected Bootstrapper createBootstrapper()
    {
        return persistent ? new NeoServerBootstrapper() : new EphemeralNeoServerBootstrapper();
    }

    public File createPropertiesFiles() throws IOException
    {
        File temporaryConfigFile = createTempPropertyFile();

        createPropertiesFile( temporaryConfigFile );
        createTuningFile( temporaryConfigFile );

        return temporaryConfigFile;
    }

    private void createPropertiesFile( File temporaryConfigFile )
    {
        Map<String, String> properties = MapUtil.stringMap(
                Configurator.DATABASE_LOCATION_PROPERTY_KEY, dbDir,
                Configurator.MANAGEMENT_PATH_PROPERTY_KEY, webAdminUri,
                Configurator.REST_API_PATH_PROPERTY_KEY, webAdminDataUri );
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
        
        if(httpsEnabled != null) {
            if(httpsEnabled) {
                properties.put( Configurator.WEBSERVER_HTTPS_ENABLED_PROPERTY_KEY, "true" );
            } else {
                properties.put( Configurator.WEBSERVER_HTTPS_ENABLED_PROPERTY_KEY, "false" );
            }
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

        FileWriter fstream = new FileWriter( f, true );
        BufferedWriter out = new BufferedWriter( fstream );

        for ( int i = 0; i < 100; i++ )
        {
            out.write( (int) System.currentTimeMillis() );
        }

        out.close();
        return f;
    }

    protected ServerBuilder()
    {
    }

    public ServerBuilder persistent()
    {
        this.persistent = true;
        return this;
    }
    
    public ServerBuilder onPort( int portNo )
    {
        this.portNo = String.valueOf( portNo );
        return this;
    }

    public ServerBuilder withMaxJettyThreads( int maxThreads )
    {
        this.maxThreads = String.valueOf( maxThreads );
        return this;
    }

    public ServerBuilder usingDatabaseDir( String dbDir )
    {
        this.dbDir = dbDir;
        return this;
    }

    public ServerBuilder withRelativeWebAdminUriPath( String webAdminUri )
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

    public ServerBuilder withRelativeWebDataAdminUriPath( String webAdminDataUri )
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

    public ServerBuilder withoutWebServerPort()
    {
        portNo = null;
        return this;
    }

    public ServerBuilder withFailingStartupHealthcheck()
    {
        startupHealthCheck = new StartupHealthCheck()
        {
            public boolean run()
            {
                return false;
            }
            public StartupHealthCheckRule failedRule() {
                return new StartupHealthCheckRule()
                {

                    public String getFailureMessage()
                    {
                        return "mockFailure";
                    }

                    public boolean execute( Properties properties )
                    {
                        return false;
                    }
                };           
            }
        };
        return this;
    }

    public ServerBuilder withDefaultDatabaseTuning() throws IOException
    {
        action = WhatToDo.CREATE_GOOD_TUNING_FILE;
        return this;
    }

    public ServerBuilder withNonResolvableTuningFile() throws IOException
    {
        action = WhatToDo.CREATE_DANGLING_TUNING_FILE_PROPERTY;
        return this;
    }

    public ServerBuilder withCorruptTuningFile() throws IOException
    {
        action = WhatToDo.CREATE_CORRUPT_TUNING_FILE;
        return this;
    }

    public ServerBuilder withThirdPartyJaxRsPackage( String packageName, String mountPoint )
    {
        thirdPartyPackages.put( packageName, mountPoint );
        return this;
    }

    public ServerBuilder withSpecificServerModulesOnly( Class<? extends ServerModule>... modules )
    {
        serverModules = Arrays.asList( modules );
        return this;
    }

    public ServerBuilder withFakeClock()
    {
        clock = new FakeClock();
        return this;
    }

    public ServerBuilder withAutoIndexingEnabledForNodes( String... keys )
    {
        autoIndexedNodeKeys = keys;
        return this;
    }

    public ServerBuilder withAutoIndexingEnabledForRelationships( String... keys )
    {
        autoIndexedRelationshipKeys = keys;
        return this;
    }

    public ServerBuilder onHost( String host )
    {
        this.host = host;
        return this;
    }

    public ServerBuilder withSecurityRules( String... securityRuleClassNames )
    {
        this.securityRuleClassNames = securityRuleClassNames;
        return this;
    }
    
    public ServerBuilder withHttpsEnabled()
    {
        httpsEnabled  = true;
        return this;
    }
}
