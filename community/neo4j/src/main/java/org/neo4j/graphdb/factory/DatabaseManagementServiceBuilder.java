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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.Nonnull;

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
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

/**
 * Creates a {@link DatabaseManagementService} with Community Edition features.
 * <p>
 * Use {@link #newDatabaseManagementService(File)} or
 * {@link #newEmbeddedDatabaseBuilder(File)} to create a database instance.
 * <p>
 */
public class DatabaseManagementServiceBuilder implements DatabaseManagementServiceInternalBuilder
{
    protected final GraphDatabaseFactoryState state;
    protected EmbeddedDatabaseCreator creator;
    protected Map<String,String> config = new HashMap<>();

    //################ Swap ###########
    @Override
    public DatabaseManagementServiceInternalBuilder setConfig( Setting<?> setting, String value )
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

    @Override
    public DatabaseManagementServiceInternalBuilder setConfig( Config config )
    {
        this.config.putAll( config.getRaw() );
        return this;
    }

    @Override
    public DatabaseManagementServiceInternalBuilder setConfig( String name, String value )
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

    @Override
    public DatabaseManagementServiceInternalBuilder setConfig( Map<String,String> config )
    {
        for ( Map.Entry<String,String> stringStringEntry : config.entrySet() )
        {
            setConfig( stringStringEntry.getKey(), stringStringEntry.getValue() );
        }
        return this;
    }

    @Override
    public DatabaseManagementServiceInternalBuilder loadPropertiesFromFile( String fileName ) throws IllegalArgumentException
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

    private DatabaseManagementServiceInternalBuilder loadPropertiesFromURL( URL url ) throws IllegalArgumentException
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

    public DatabaseManagementService newDatabaseManagementService()
    {
        return creator.newDatabase( Config.defaults( config ) );
    }

    // ###################################

    public DatabaseManagementServiceBuilder()
    {
        this( new GraphDatabaseFactoryState() );
    }

    protected DatabaseManagementServiceBuilder( GraphDatabaseFactoryState state )
    {
        this.state = state;
    }

    protected GraphDatabaseFactoryState getCurrentState()
    {
        return state;
    }

    public DatabaseManagementService newDatabaseManagementService( File storeDir )
    {
        return newEmbeddedDatabaseBuilder( storeDir ).newDatabaseManagementService();
    }

    /**
     * @param storeDir desired embedded database store dir
     */
    public DatabaseManagementServiceInternalBuilder newEmbeddedDatabaseBuilder( File storeDir )
    {
        creator = new EmbeddedDatabaseCreator( storeDir, state );
        configure( this );
        return this;
    }

    protected DatabaseManagementService newEmbeddedDatabase( File storeDir, Config config, ExternalDependencies dependencies, boolean impermanent )
    {
        config.augment( GraphDatabaseSettings.ephemeral, Settings.FALSE );
        return getGraphDatabaseFacadeFactory().newFacade( storeDir, augmentConfig( config ), dependencies );
    }

    protected DatabaseInfo getDatabaseInfo()
    {
        return DatabaseInfo.COMMUNITY;
    }

    protected Function<GlobalModule,AbstractEditionModule> getEditionFactory()
    {
        return CommunityEditionModule::new;
    }

    protected DatabaseManagementServiceFactory getGraphDatabaseFacadeFactory()
    {
        return new DatabaseManagementServiceFactory( getDatabaseInfo(), getEditionFactory()  );
    }

    public DatabaseManagementServiceBuilder addURLAccessRule( String protocol, URLAccessRule rule )
    {
        getCurrentState().addURLAccessRule( protocol, rule );
        return this;
    }

    public DatabaseManagementServiceBuilder setUserLogProvider( LogProvider userLogProvider )
    {
        getCurrentState().setUserLogProvider( userLogProvider );
        return this;
    }

    public DatabaseManagementServiceBuilder setMonitors( Monitors monitors )
    {
        getCurrentState().setMonitors( monitors );
        return this;
    }

    public DatabaseManagementServiceBuilder setExternalDependencies( DependencyResolver dependencies )
    {
        getCurrentState().setDependencies( dependencies );
        return this;
    }

    public String getEdition()
    {
        return Edition.COMMUNITY.toString();
    }

    /**
     * Override to change default values
     * @param builder
     */
    protected void configure( DatabaseManagementServiceInternalBuilder builder )
    {
        // Let the default configuration pass through.
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

    protected class EmbeddedDatabaseCreator
    {
        private final File storeDir;
        private final GraphDatabaseFactoryState state;
        private final boolean impermanent;

        EmbeddedDatabaseCreator( File storeDir, GraphDatabaseFactoryState state )
        {
            this.storeDir = storeDir;
            this.state = state;
            impermanent = false;
        }

        public EmbeddedDatabaseCreator( File storeDir, GraphDatabaseFactoryState state, boolean impermanent )
        {
            this.storeDir = storeDir;
            this.state = state;
            this.impermanent = impermanent;
        }

        DatabaseManagementService newDatabase( @Nonnull Config config )
        {
            return newEmbeddedDatabase( storeDir, config, state.databaseDependencies(), impermanent );
        }
    }
}
