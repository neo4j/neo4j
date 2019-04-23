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
package org.neo4j.graphdb.factory;

import java.io.File;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;

import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.common.Edition;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.Settings;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule;
import org.neo4j.graphdb.security.URLAccessRule;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.service.Services;

import static org.neo4j.graphdb.facade.GraphDatabaseDependencies.newDependencies;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

/**
 * Creates a {@link DatabaseManagementService} with Community Edition features.
 */
public class DatabaseManagementServiceBuilder
{
    protected final List<ExtensionFactory<?>> extensions = new ArrayList<>();
    protected Monitors monitors;
    protected LogProvider userLogProvider;
    protected DependencyResolver dependencies = new Dependencies();
    protected final Map<String,URLAccessRule> urlAccessRules = new HashMap<>();
    protected File databaseRootDir;
    protected Map<String,String> config = new HashMap<>();

    public DatabaseManagementServiceBuilder( File databaseRootDirectory )
    {
        this.databaseRootDir = databaseRootDirectory;
        Services.loadAll( ExtensionFactory.class ).forEach( extensions::add );
    }

    public DatabaseManagementService build()
    {
        return newDatabaseManagementService( databaseRootDir, Config.defaults( config ), databaseDependencies() );
    }

    protected DatabaseManagementService newDatabaseManagementService( File storeDir, Config config, ExternalDependencies dependencies )
    {
        config.augment( GraphDatabaseSettings.ephemeral, Settings.FALSE );
        return new DatabaseManagementServiceFactory( getDatabaseInfo(), getEditionFactory() )
                .newFacade( storeDir, augmentConfig( config ), dependencies );
    }

    protected DatabaseInfo getDatabaseInfo()
    {
        return DatabaseInfo.COMMUNITY;
    }

    protected Function<GlobalModule,AbstractEditionModule> getEditionFactory()
    {
        return CommunityEditionModule::new;
    }

    /**
     * Override to augment config values
     * @param config
     * @return
     */
    protected Config augmentConfig( Config config )
    {
        return config;
    }

    public DatabaseManagementServiceBuilder addURLAccessRule( String protocol, URLAccessRule rule )
    {
        urlAccessRules.put( protocol, rule );
        return this;
    }

    public DatabaseManagementServiceBuilder setUserLogProvider( LogProvider userLogProvider )
    {
        this.userLogProvider = userLogProvider;
        return this;
    }

    public DatabaseManagementServiceBuilder setMonitors( Monitors monitors )
    {
        this.monitors = monitors;
        return this;
    }

    public DatabaseManagementServiceBuilder setExternalDependencies( DependencyResolver dependencies )
    {
        this.dependencies = dependencies;
        return this;
    }

    public String getEdition()
    {
        return Edition.COMMUNITY.toString();
    }

    private ExternalDependencies databaseDependencies()
    {
        return newDependencies().
                monitors( monitors ).
                userLogProvider( userLogProvider ).
                dependencies( dependencies ).
                urlAccessRules( urlAccessRules ).
                extensions( extensions );
    }

    public DatabaseManagementServiceBuilder setConfig( Setting<?> setting, String value )
    {
        if ( value == null )
        {
            config.remove( setting.name() );
        }
        else
        {
            // Test if we can get this setting with an updated config
            Map<String,String> testValue = stringMap( setting.name(), value );
            setting.apply( key -> testValue.containsKey( key ) ? testValue.get( key ) : config.get( key ) );

            // No exception thrown, add it to existing config
            config.put( setting.name(), value );
        }
        return this;
    }

    @Deprecated
    public DatabaseManagementServiceBuilder setConfig( String name, String value )
    {
        if ( value == null )
        {
            config.remove( name );
        }
        else
        {
            config.put( name, value );
        }
        return this;
    }

    @Deprecated
    public DatabaseManagementServiceBuilder setConfigRaw( Map<String,String> config )
    {
        for ( Map.Entry<String,String> stringStringEntry : config.entrySet() )
        {
            setConfig( stringStringEntry.getKey(), stringStringEntry.getValue() );
        }
        return this;
    }

    public DatabaseManagementServiceBuilder setConfig( Map<Setting<?>,String> config )
    {
        for ( var stringStringEntry : config.entrySet() )
        {
            setConfig( stringStringEntry.getKey(), stringStringEntry.getValue() );
        }
        return this;
    }

    public DatabaseManagementServiceBuilder loadPropertiesFromFile( String fileName ) throws IllegalArgumentException
    {
        try
        {
            return loadPropertiesFromURL( new File( fileName ).toURI().toURL() );
        }
        catch ( MalformedURLException e )
        {
            throw new IllegalArgumentException( "Illegal filename:" + fileName, e );
        }
    }

    private DatabaseManagementServiceBuilder loadPropertiesFromURL( URL url ) throws IllegalArgumentException
    {
        Properties props = new Properties();
        try
        {
            try ( InputStream stream = url.openStream() )
            {
                props.load( stream );
            }
        }
        catch ( Exception e )
        {
            throw new IllegalArgumentException( "Unable to load " + url, e );
        }
        Set<Map.Entry<Object,Object>> entries = props.entrySet();
        for ( Map.Entry<Object,Object> entry : entries )
        {
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();
            setConfig( key, value );
        }

        return this;
    }
}
