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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.info.DiagnosticsPhase;
import org.neo4j.kernel.info.DiagnosticsProvider;
import org.neo4j.logging.BufferingLog;
import org.neo4j.logging.Log;
import org.neo4j.logging.Logger;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

/**
 * This class holds the overall configuration of a Neo4j database instance. Use the accessors
 * to convert the internal key-value settings to other types.
 * <p>
 * Users can assume that old settings have been migrated to their new counterparts, and that defaults
 * have been applied.
 * <p>
 * UI's can change configuration by calling applyChanges. Any listener, such as services that use
 * this configuration, can be notified of changes by implementing the {@link ConfigurationChangeListener} interface.
 */
public class Config implements DiagnosticsProvider, Configuration
{
    private final List<ConfigurationChangeListener> listeners = new CopyOnWriteArrayList<>();
    private final Map<String, String> params = new ConcurrentHashMap<>();
    private final ConfigValues settingsFunction;

    // Messages to this log get replayed into a real logger once logging has been
    // instantiated.
    private final BufferingLog bufferedLog = new BufferingLog();
    private Log log = bufferedLog;

    private Iterable<Class<?>> settingsClasses = emptyList();
    private ConfigurationMigrator migrator;
    private ConfigurationValidator validator;

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

    public Config( Map<String, String> inputParams, Iterable<Class<?>> settingsClasses )
    {
        this.params.putAll( inputParams );
        this.settingsFunction = new ConfigValues( params );
        registerSettingsClasses( settingsClasses );
    }

    /**
     * Returns a copy of this config with the given modifications.
     *
     * @return a new modified config, leaves this config unchanged.
     */
    public Config with( Map<String, String> additionalConfig )
    {
        Map<String, String> newParams = getParams(); // copy is returned
        newParams.putAll( additionalConfig );
        return new Config( newParams );
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

    /**
     * Add more settings classes.
     */
    public Config registerSettingsClasses( Iterable<Class<?>> settingsClasses )
    {
        this.settingsClasses = Iterables.concat( settingsClasses, this.settingsClasses );
        this.migrator = new AnnotationBasedConfigurationMigrator( settingsClasses );
        this.validator = new ConfigurationValidator( settingsClasses );

        // Apply the requirements and changes the new settings classes introduce
        this.replaceSettings( getParams() );

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

    public void addConfigurationChangeListener( ConfigurationChangeListener listener )
    {
        listeners.add( listener );
    }

    public void removeConfigurationChangeListener( ConfigurationChangeListener listener )
    {
        listeners.remove( listener );
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

    private synchronized Config replaceSettings( Map<String, String> newValues )
    {
        newValues = migrator.apply( newValues, log );

        // Make sure all changes are valid
        validator.validate( newValues );

        // Figure out what changed
        if ( listeners.isEmpty() )
        {
            // Make the change
            params.clear();
            params.putAll( newValues );
        }
        else
        {
            List<ConfigurationChange> configurationChanges = new ArrayList<>();
            for ( Map.Entry<String, String> stringStringEntry : newValues.entrySet() )
            {
                String oldValue = params.get( stringStringEntry.getKey() );
                String newValue = stringStringEntry.getValue();
                if ( !(oldValue == null && newValue == null) &&
                        (oldValue == null || newValue == null || !oldValue.equals( newValue )) )
                {
                    configurationChanges.add( new ConfigurationChange( stringStringEntry.getKey(), oldValue,
                            newValue ) );
                }
            }

            if ( configurationChanges.isEmpty() )
            {
                // Don't bother... nothing changed.
                return this;
            }

            // Make the change
            params.clear();
            for ( Map.Entry<String, String> entry : newValues.entrySet() )
            {
                // Filter out nulls because we are using a ConcurrentHashMap under the covers, which doesn't support
                // null keys or values.
                String value = entry.getValue();
                if ( value != null )
                {
                    params.put( entry.getKey(), value );
                }
            }

            // Notify listeners
            for ( ConfigurationChangeListener listener : listeners )
            {
                listener.notifyConfigurationChanges( configurationChanges );
            }
        }

        return this;
    }
}
