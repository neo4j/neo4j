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
package org.neo4j.graphdb.factory;

import static org.neo4j.helpers.Functions.map;
import static org.neo4j.helpers.Functions.withDefaults;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

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

/**
 * Builder for GraphDatabaseServices that allows for setting and loading configuration.
 */
public class GraphDatabaseBuilder
{

    public interface DatabaseCreator
    {
        GraphDatabaseService newDatabase( Map<String, String> config );
    }

    DatabaseCreator creator;
    Map<String, String> config = new HashMap<String, String>();

    public GraphDatabaseBuilder( DatabaseCreator creator )
    {
        this.creator = creator;
    }

    /**
     * Set a database setting to a particular value.
     *
     * @param setting
     * @param value
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
     * @param name
     * @param value
     * @return the builder
     * @deprecated Use setConfig with explicit GraphDatabaseSetting instead
     *             <p/>
     *             Set unvalidated config option
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

/* TODO When all settings are in GraphDatabaseSettings, then use this instead
        try
        {
            GraphDatabaseSetting setting = (GraphDatabaseSetting) CommunityGraphDatabaseSetting.class.getField( name
            ).get( null );
            setConfig( setting, value );
            return this;
        }
        catch( IllegalAccessException e )
        {
            throw new IllegalArgumentException( "Unknown configuration setting:"+name );
        }
        catch( NoSuchFieldException e )
        {
            throw new IllegalArgumentException( "Unknown configuration setting:"+name );
        }
*/
    }

    /**
     * Set a map of config settings into the builder. Overwrites any existing values.
     *
     * @param config
     * @return the builder
     */

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
     * @param fileName
     * @return the builder
     * @throws IllegalArgumentException if the builder was unable to load from the given filename
     */

    public GraphDatabaseBuilder loadPropertiesFromFile( String fileName )
            throws IllegalArgumentException
    {
        try
        {
            return loadPropertiesFromURL( new File( fileName ).toURL() );
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
     * @param url
     * @return the builder
     */
    public GraphDatabaseBuilder loadPropertiesFromURL( URL url )
            throws IllegalArgumentException
    {
        Properties props = new Properties();
        try
        {
            InputStream stream = url.openStream();
            try
            {
                props.load( stream );
            }
            finally
            {
                stream.close();
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
}
