/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.graphdb.factory;

import java.io.File;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Setting;

import static org.neo4j.helpers.Functions.map;
import static org.neo4j.helpers.Functions.withDefaults;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

/**
 * Builder for {@link GraphDatabaseService}s that allows for setting and loading
 * configuration.
 */
public class GraphDatabaseBuilder
{
    public interface DatabaseCreator
    {
        GraphDatabaseService newDatabase( Map<String, String> config );
    }

    protected DatabaseCreator creator;
    protected Map<String, String> config = new HashMap<>();

    public GraphDatabaseBuilder( DatabaseCreator creator )
    {
        this.creator = creator;
    }

    /**
     * Set a database setting to a particular value.
     *
     * @param setting Database setting to set
     * @param value New value of the setting
     * @return the builder
     */
    public GraphDatabaseBuilder setConfig( Setting<?> setting, String value )
    {
        if ( value == null )
        {
            config.remove( setting.name() );
        }
        else
        {
            // Test if we can get this setting with an updated config
            Map<String, String> testValue = stringMap( setting.name(), value );
            setting.apply( withDefaults( map( config ), map( testValue ) ) );

            // No exception thrown, add it to existing config
            config.put( setting.name(), value );
        }
        return this;
    }

    /**
     * Set an unvalidated configuration option.
     * @deprecated Use setConfig with explicit {@link Setting} instead.
     * 
     * @param name Name of the setting
     * @param value New value of the setting
     * @return the builder
     */
    @Deprecated
    public GraphDatabaseBuilder setConfig( String name, String value )
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

    /**
     * Set a map of configuration settings into the builder. Overwrites any existing values.
     *
     * @param config Map of configuration settings
     * @return the builder
     * @deprecated Use setConfig with explicit {@link Setting} instead
     */
    @Deprecated
    @SuppressWarnings("deprecation")
    public GraphDatabaseBuilder setConfig( Map<String, String> config )
    {
        for ( Map.Entry<String, String> stringStringEntry : config.entrySet() )
        {
            setConfig( stringStringEntry.getKey(), stringStringEntry.getValue() );
        }
        return this;
    }

    /**
     * Load a Properties file from a given file, and add the settings to
     * the builder.
     *
     * @param fileName Filename of properties file to use
     * @return the builder
     * @throws IllegalArgumentException if the builder was unable to load from the given filename
     */

    public GraphDatabaseBuilder loadPropertiesFromFile( String fileName )
            throws IllegalArgumentException
    {
        try
        {
            return loadPropertiesFromURL( new File( fileName ).toURI().toURL() );
        }
        catch ( MalformedURLException e )
        {
            throw new IllegalArgumentException( "Illegal filename:" + fileName );
        }
    }

    /**
     * Load Properties file from a given URL, and add the settings to
     * the builder.
     *
     * @param url URL of properties file to use
     * @return the builder
     */
    public GraphDatabaseBuilder loadPropertiesFromURL( URL url )
            throws IllegalArgumentException
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
        Set<Map.Entry<Object, Object>> entries = props.entrySet();
        for ( Map.Entry<Object, Object> entry : entries )
        {
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();
            setConfig( key, value );
        }

        return this;
    }

    /**
     * Create a new database with the configuration registered
     * through the builder.
     *
     * @return an instance of GraphDatabaseService
     */
    public GraphDatabaseService newGraphDatabase()
    {
        return creator.newDatabase( config );
    }

    /**
     * Used by tests via GraphDatabaseBuilderTestTools.
     */
    Map<String, String> getRawConfig()
    {
        return config;
    }

    public static class Delegator extends GraphDatabaseBuilder
    {
        private final GraphDatabaseBuilder actual;

        public Delegator( GraphDatabaseBuilder actual )
        {
            super( null );
            this.actual = actual;
        }

        @Override
        public GraphDatabaseBuilder setConfig( Setting<?> setting, String value )
        {
            actual.setConfig( setting, value );
            return this;
        }

        @Override
        @SuppressWarnings("deprecation")
        public GraphDatabaseBuilder setConfig( String name, String value )
        {
            actual.setConfig( name, value );
            return this;
        }

        @Override
        public GraphDatabaseBuilder setConfig( Map<String, String> config )
        {
            actual.setConfig( config );
            return this;
        }

        @Override
        public GraphDatabaseBuilder loadPropertiesFromFile( String fileName ) throws IllegalArgumentException
        {
            actual.loadPropertiesFromFile( fileName );
            return this;
        }

        @Override
        public GraphDatabaseBuilder loadPropertiesFromURL( URL url ) throws IllegalArgumentException
        {
            actual.loadPropertiesFromURL( url );
            return this;
        }

        @Override
        public GraphDatabaseService newGraphDatabase()
        {
            return actual.newGraphDatabase();
        }
    }
}
