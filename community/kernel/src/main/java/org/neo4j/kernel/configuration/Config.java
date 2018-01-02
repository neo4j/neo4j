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
package org.neo4j.kernel.configuration;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.Functions;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.info.DiagnosticsPhase;
import org.neo4j.kernel.info.DiagnosticsProvider;
import org.neo4j.logging.BufferingLog;
import org.neo4j.logging.Log;
import org.neo4j.logging.Logger;

import static java.lang.Character.isDigit;
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
public class Config implements DiagnosticsProvider
{
    private final List<ConfigurationChangeListener> listeners = new CopyOnWriteArrayList<>();
    private final Map<String, String> params = new ConcurrentHashMap<>(  );
    private final Function<String, String> settingsFunction;

    // Messages to this log get replayed into a real logger once logging has been
    // instantiated.
    private final BufferingLog bufferedLog = new BufferingLog();
    private Log log = bufferedLog;

    private Iterable<Class<?>> settingsClasses = emptyList();
    private ConfigurationMigrator migrator;
    private ConfigurationValidator validator;

    public Config()
    {
        this( new HashMap<String, String>(), Collections.<Class<?>>emptyList() );
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
        this.settingsFunction = Functions.map( params );
        this.params.putAll( inputParams );
        registerSettingsClasses( settingsClasses );
    }

    /** Add more settings classes. */
    public Config registerSettingsClasses( Iterable<Class<?>> settingsClasses )
    {
        this.settingsClasses = Iterables.concat( settingsClasses, this.settingsClasses );
        this.migrator = new AnnotationBasedConfigurationMigrator( settingsClasses );
        this.validator = new ConfigurationValidator( settingsClasses );

        // Apply the requirements and changes the new settings classes introduce
        this.applyChanges( getParams() );

        return this;
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
    public <T> T get( Setting<T> setting )
    {
        return setting.apply( settingsFunction );
    }

    /**
     * Use {@link Config#applyChanges(java.util.Map)} instead, so changes are applied in
     * bulk and the ConfigurationChangeListeners can process the changes in one go.
     */
    @Deprecated
    public Config setProperty( String key, Object value )
    {
        // This method here is for supporting legacy server configurator api.
        // None should call this except external users,
        // as "ideally" properties should not be changed once they are loaded.
        this.params.put( key, value.toString() );
        this.applyChanges( new HashMap<>( params ) );
        return this;
    }

    /**
     * Augment the existing config with new settings, overriding any conflicting settings, but keeping all old
     * non-overlapping ones.
     * @param changes settings to add and override
     */
    public Config augment( Map<String,String> changes )
    {
        Map<String,String> params = getParams();
        params.putAll( changes );
        applyChanges( params );
        return this;
    }

    /**
     * Replace the current set of configuration parameters with another one.
     */
    public synchronized Config applyChanges( Map<String, String> newConfiguration )
    {
        newConfiguration = migrator.apply( newConfiguration, log );

        // Make sure all changes are valid
        validator.validate( newConfiguration );

        // Figure out what changed
        if ( listeners.isEmpty() )
        {
            // Make the change
            params.clear();
            params.putAll( newConfiguration );
        }
        else
        {
            List<ConfigurationChange> configurationChanges = new ArrayList<>();
            for ( Map.Entry<String, String> stringStringEntry : newConfiguration.entrySet() )
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
            for ( Map.Entry<String, String> entry : newConfiguration.entrySet() )
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

    public static long parseLongWithUnit( String numberWithPotentialUnit )
    {
        int firstNonDigitIndex = findFirstNonDigit( numberWithPotentialUnit );
        String number = numberWithPotentialUnit.substring( 0, firstNonDigitIndex );

        long multiplier = 1;
        if ( firstNonDigitIndex < numberWithPotentialUnit.length() )
        {
            String unit = numberWithPotentialUnit.substring( firstNonDigitIndex );
            if ( unit.equalsIgnoreCase( "k" ) )
            {
                multiplier = 1024;
            }
            else if ( unit.equalsIgnoreCase( "m" ) )
            {
                multiplier = 1024 * 1024;
            }
            else if ( unit.equalsIgnoreCase( "g" ) )
            {
                multiplier = 1024 * 1024 * 1024;
            }
            else
            {
                throw new IllegalArgumentException(
                        "Illegal unit '" + unit + "' for number '" + numberWithPotentialUnit + "'" );
            }
        }

        return Long.parseLong( number ) * multiplier;
    }

    /**
     * @return index of first non-digit character in {@code numberWithPotentialUnit}. If all digits then
     * {@code numberWithPotentialUnit.length()} is returned.
     */
    private static int findFirstNonDigit( String numberWithPotentialUnit )
    {
        int firstNonDigitIndex = numberWithPotentialUnit.length();
        for ( int i = 0; i < numberWithPotentialUnit.length(); i++ )
        {
            if ( !isDigit( numberWithPotentialUnit.charAt( i ) ) )
            {
                firstNonDigitIndex = i;
                break;
            }
        }
        return firstNonDigitIndex;
    }

    /**
     * Returns a copy of this config with the given modifications.
     * @return a new modified config, leaves this config unchanged.
     */
    public Config with( Map<String, String> additionalConfig )
    {
        Map<String, String> newParams = getParams(); // copy is returned
        newParams.putAll( additionalConfig );
        return new Config( newParams );
    }

    /**
     * Looks at configured file {@code absoluteOrRelativeFile} and just returns it if absolute, otherwise
     * returns a {@link File} with {@code baseDirectoryIfRelative} as parent.
     *
     * @param baseDirectoryIfRelative base directory to use as parent if {@code absoluteOrRelativeFile}
     * is relative, otherwise unused.
     * @param absoluteOrRelativeFile file to return as absolute or relative to {@code baseDirectoryIfRelative}.
     */
    public static File absoluteFileOrRelativeTo( File baseDirectoryIfRelative, File absoluteOrRelativeFile )
    {
        return absoluteOrRelativeFile.isAbsolute()
                ? absoluteOrRelativeFile
                : new File( baseDirectoryIfRelative, absoluteOrRelativeFile.getPath() );
    }
}
