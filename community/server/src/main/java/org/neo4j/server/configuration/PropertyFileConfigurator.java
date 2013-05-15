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
package org.neo4j.server.configuration;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.server.configuration.validation.Validator;
import org.neo4j.server.logging.Logger;

public class PropertyFileConfigurator implements Configurator
{
    private static final String NEO4J_PROPERTIES_FILENAME = "neo4j.properties";

    public static final Logger log = Logger.getLogger( PropertyFileConfigurator.class );

    private final CompositeConfiguration serverConfiguration = new CompositeConfiguration();
    private File propertyFileDirectory;

    private final Validator validator = new Validator();
    private Map<String, String> databaseTuningProperties = null;
    private HashSet<ThirdPartyJaxRsPackage> thirdPartyPackages;

    public PropertyFileConfigurator( File propertiesFile )
    {
        this( null, propertiesFile );
    }

    public PropertyFileConfigurator( Validator v )
    {
        this( v, null );
    }

    public PropertyFileConfigurator( Validator v, File propertiesFile )
    {
        if ( propertiesFile == null )
        {
            propertiesFile = new File( System.getProperty( Configurator.NEO_SERVER_CONFIG_FILE_KEY ) );
        }

        try
        {
            propertyFileDirectory = propertiesFile.getParentFile();
            loadPropertiesConfig( propertiesFile );
            loadDatabaseTuningProperties( propertiesFile );
            
            normalizeUris();
            ensureRelativeUris();
            
            if ( v != null )
            {
                v.validate( this.configuration() );
            }
        }
        catch ( ConfigurationException ce )
        {
            log.warn( ce );
        }

    }

    @Override
    public Configuration configuration()
    {
        return serverConfiguration == null ? new SystemConfiguration() : serverConfiguration;
    }

    private void loadDatabaseTuningProperties( File configFile ) throws ConfigurationException
    {
        String databaseTuningPropertyFileLocation = serverConfiguration.getString( DB_TUNING_PROPERTY_FILE_KEY );

        if ( databaseTuningPropertyFileLocation == null )
        {
            if ( propertyFileDirectoryContainsDBTuningFile() )
            {
                databaseTuningPropertyFileLocation = new File( propertyFileDirectory, NEO4J_PROPERTIES_FILENAME ).getAbsolutePath();
                log.info( "No database tuning file explicitly set, defaulting to [%s]",
                        databaseTuningPropertyFileLocation );
            }
            else
            {
                log.info(
                        "No database tuning properties (org.neo4j.server.db.tuning.properties) found in [%s], using defaults.",
                        databaseTuningPropertyFileLocation );
                return;
            }
        }

        File databaseTuningPropertyFile = new File( databaseTuningPropertyFileLocation );

        if ( !databaseTuningPropertyFile.exists() )
        {
            log.warn( "The specified file for database performance tuning properties [%s] does not exist.",
                    databaseTuningPropertyFileLocation );
            return;
        }

        try
        {
            databaseTuningProperties = MapUtil.load(databaseTuningPropertyFile);
        }
        catch( IOException e )
        {
            databaseTuningProperties = new HashMap<String, String>();
        }
    }

    private void loadPropertiesConfig( File configFile ) throws ConfigurationException
    {
        PropertiesConfiguration propertiesConfig = new PropertiesConfiguration( configFile );
        if ( validator.validate( propertiesConfig ) )
        {
            serverConfiguration.addConfiguration( propertiesConfig );
        }
        else
        {
            String failed = String.format( "Error processing [%s], configuration file has failed validation.",
                    configFile.getAbsolutePath() );
            log.fatal( failed );
            throw new InvalidServerConfigurationException( failed );
        }
    }

    private void normalizeUris()
    {
        try
        {
            for ( String key : new String[] { MANAGEMENT_PATH_PROPERTY_KEY, REST_API_PATH_PROPERTY_KEY } )
            {
                if ( configuration().containsKey( key ) )
                {
                    URI normalizedUri = new URI( (String) configuration().getProperty( key ) ).normalize();
                    configuration().clearProperty( key );
                    configuration().addProperty( key, normalizedUri.toString() );
                }
            }

        }
        catch ( URISyntaxException e )
        {
            throw new RuntimeException( e );
        }

    }

    private void ensureRelativeUris()
    {
        try
        {
            for ( String key : new String[] { MANAGEMENT_PATH_PROPERTY_KEY, REST_API_PATH_PROPERTY_KEY } )
            {
                if ( configuration().containsKey( key ) )
                {
                    String path = new URI( (String) configuration().getProperty( key ) ).getPath();
                    configuration().clearProperty( key );
                    configuration().addProperty( key, path );
                }
            }

        }
        catch ( URISyntaxException e )
        {
            throw new RuntimeException( e );
        }

    }

    private boolean propertyFileDirectoryContainsDBTuningFile()
    {
        File[] neo4jPropertyFiles = propertyFileDirectory.listFiles( new FilenameFilter()
        {

            @Override
            public boolean accept( File dir, String name )
            {
                return name.toLowerCase()
                        .equals( NEO4J_PROPERTIES_FILENAME );
            }
        } );
        return neo4jPropertyFiles != null && neo4jPropertyFiles.length == 1;
    }

    public File getPropertyFileDirectory()
    {
        return propertyFileDirectory;
    }

    @Override
    public Map<String, String> getDatabaseTuningProperties()
    {
        return databaseTuningProperties == null ? new HashMap<String, String>() : databaseTuningProperties;
    }

    @Override
    public Set<ThirdPartyJaxRsPackage> getThirdpartyJaxRsClasses()
    {
        thirdPartyPackages = new HashSet<ThirdPartyJaxRsPackage>();
        Properties properties = this.configuration()
                .getProperties( THIRD_PARTY_PACKAGES_KEY );
        for ( Object key : properties.keySet() )
        {
            thirdPartyPackages.add( new ThirdPartyJaxRsPackage( key.toString(), properties.getProperty( key.toString() ) ) );
        }
        return thirdPartyPackages;
    }
}
