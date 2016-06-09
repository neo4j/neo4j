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
package org.neo4j.kernel.configuration;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;

import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.info.DiagnosticsPhase;
import org.neo4j.kernel.info.DiagnosticsProvider;
import org.neo4j.logging.BufferingLog;
import org.neo4j.logging.Log;
import org.neo4j.logging.Logger;

import static java.util.Arrays.asList;
import static org.neo4j.helpers.collection.Iterables.concat;

/**
 * This class holds the overall configuration of a Neo4j database instance. Use the accessors to convert the internal
 * key-value settings to other types.
 * <p>
 * Users can assume that old settings have been migrated to their new counterparts, and that defaults have been
 * applied.
 */
public class Config implements DiagnosticsProvider, Configuration
{
    private final Map<String, String> params = new ConcurrentHashMap<>();
    private final Iterable<Class<?>> settingsClasses;
    private final ConfigurationMigrator migrator;
    private final ConfigurationValidator validator;

    private ConfigValues settingsFunction;

    // Messages to this log get replayed into a real logger once logging has been instantiated.
    private final BufferingLog bufferedLog = new BufferingLog();
    private Log log = bufferedLog;

    public static Config empty()
    {
        return new Config();
    }

    public static Config defaults()
    {
        return new Config();
    }

    private Config()
    {
        this( new HashMap<>() );
    }

    public Config( Map<String, String> inputParams )
    {
        this( inputParams, Collections.<Class<?>>emptyList() );
    }

    public Config( Map<String, String> inputParams, Class<?>... settingsClasses )
    {
        this( inputParams, asList( settingsClasses ) );
    }

    public Config( Map<String, String> params, Iterable<Class<?>> settingsClasses )
    {
        this( Optional.empty(), params, settings -> {}, ( classes ) -> settingsClasses );
    }

    public Config( Optional<File> configFile, Map<String, String> overriddenSettings,
            Consumer<Map<String, String>> settingsPostProcessor,
            Function<Map<String, String> ,Iterable<Class<?>>> settingClassesProvider)
    {
        Map<String,String> settings = initSettings( configFile, settingsPostProcessor, overriddenSettings );
        this.settingsClasses = settingClassesProvider.apply( settings );
        migrator = new AnnotationBasedConfigurationMigrator( settingsClasses );
        validator = new ConfigurationValidator( settingsClasses );
        replaceSettings( settings );
    }

    /**
     * Returns a copy of this config with the given modifications.
     *
     * @return a new modified config, leaves this config unchanged.
     */
    public Config with( Map<String, String> additionalConfig, Class<?>... settingsClasses )
    {
        Map<String, String> newParams = getParams(); // copy is returned
        newParams.putAll( additionalConfig );
        return new Config( newParams, concat( this.settingsClasses, asList( settingsClasses ) ) );
    }

    // TODO: Get rid of this, to allow us to have something more
    // elaborate as internal storage (eg. something that can keep meta data with
    // properties).
    public Map<String, String> getParams()
    {
        return new HashMap<>( this.params );
    }

    /**
     * Retrieve a configuration property.
     */
    @Override
    public <T> T get( Setting<T> setting )
    {
        return setting.apply( settingsFunction );
    }

    /**
     * Unlike the public {@link Setting} instances, the function passed in here has access to
     * the raw setting data, meaning it can provide functionality that cross multiple settings
     * and other more advanced use cases.
     */
    public <T> T view( Function<ConfigValues, T> projection )
    {
        return projection.apply( settingsFunction );
    }

    /**
     * Augment the existing config with new settings, overriding any conflicting settings, but keeping all old
     * non-overlapping ones.
     *
     * @param changes settings to add and override
     */
    public Config augment( Map<String, String> changes )
    {
        Map<String, String> params = getParams();
        params.putAll( changes );
        replaceSettings( params );
        return this;
    }

    public Iterable<Class<?>> getSettingsClasses()
    {
        return settingsClasses;
    }

    public void setLogger( Log log )
    {
        if ( this.log == bufferedLog )
        {
            bufferedLog.replayInto( log );
        }
        this.log = log;
    }

    @Override
    public String getDiagnosticsIdentifier()
    {
        return getClass().getName();
    }

    @Override
    public void acceptDiagnosticsVisitor( Object visitor )
    {
        // nothing visits configuration
    }

    @Override
    public void dump( DiagnosticsPhase phase, Logger logger )
    {
        if ( phase.isInitialization() || phase.isExplicitlyRequested() )
        {
            logger.log( "Neo4j Kernel properties:" );
            for ( Map.Entry<String, String> param : params.entrySet() )
            {
                logger.log( "%s=%s", param.getKey(), param.getValue() );
            }
        }
    }

    @Override
    public String toString()
    {
        List<String> keys = new ArrayList<>( params.keySet() );
        Collections.sort( keys );
        LinkedHashMap<String, String> output = new LinkedHashMap<>();
        for ( String key : keys )
        {
            output.put( key, params.get( key ) );
        }

        return output.toString();
    }

    private synchronized void replaceSettings( Map<String, String> newSettings )
    {
        Map<String,String> migratedSettings = migrator.apply( newSettings, log );
        validator.validate( migratedSettings );
        params.clear();
        params.putAll( migratedSettings );
        settingsFunction = new ConfigValues( params );
    }

    private Map<String,String> initSettings( Optional<File> configFile,
            Consumer<Map<String,String>> settingsPostProcessor, Map<String,String> overriddenSettings )
    {
        Map<String,String> settings = new HashMap<>();
        configFile.ifPresent( file -> settings.putAll( loadFromFile( file) ) );
        settingsPostProcessor.accept( settings );
        settings.putAll( overriddenSettings );
        return settings;
    }

    private Map<String, String> loadFromFile( File file )
    {
        if ( !file.exists() )
        {
            log.warn( "Config file [%s] does not exist.", file );
            return new HashMap<>();
        }
        try
        {
            return MapUtil.load( file );
        }
        catch ( IOException e )
        {
            log.error( "Unable to load config file [%s]: %s", file, e.getMessage() );
            return new HashMap<>();
        }
    }
}
