/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import java.util.Arrays;
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
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.info.DiagnosticsPhase;
import org.neo4j.kernel.info.DiagnosticsProvider;
import org.neo4j.kernel.logging.BufferingLogger;

import static java.lang.Character.isDigit;

/**
 * This class holds the overall configuration of a Neo4j database instance. Use the accessors
 * to convert the internal key-value settings to other types.
 * <p/>
 * Users can assume that old settings have been migrated to their new counterparts, and that defaults
 * have been applied.
 * <p/>
 * UI's can change configuration by calling applyChanges. Any listener, such as services that use
 * this configuration, can be notified of changes by implementing the {@link ConfigurationChangeListener} interface.
 */
public class Config implements DiagnosticsProvider
{
    private final List<ConfigurationChangeListener> listeners = new CopyOnWriteArrayList<ConfigurationChangeListener>();
    private final Map<String, String> params = new ConcurrentHashMap<String, String>(  );
    private final ConfigurationMigrator migrator;

    // Messages to this log get replayed into a real logger once logging has been
    // instantiated.
    private StringLogger log = new BufferingLogger();
    private final ConfigurationValidator validator;
    private final Function<String, String> settingsFunction;
    private final Iterable<Class<?>> settingsClasses;

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
        this( inputParams, Arrays.asList( settingsClasses ) );
    }

    public Config( Map<String, String> inputParams, Iterable<Class<?>> settingsClasses )
    {
        this.settingsClasses = settingsClasses;
        settingsFunction = Functions.map( params );

        this.migrator = new AnnotationBasedConfigurationMigrator( settingsClasses );
        this.validator = new ConfigurationValidator( settingsClasses );
        this.applyChanges( inputParams );
    }

    // TODO: Get rid of this, to allow us to have something more
    // elaborate as internal storage (eg. something that can keep meta data with
    // properties).
    public Map<String, String> getParams()
    {
        return new HashMap<String, String>( this.params );
    }

    /**
     * Retrieve a configuration property.
     */
    public <T> T get( Setting<T> setting )
    {
        return setting.apply( settingsFunction );
    }

    /**
     * Replace the current set of configuration parameters with another one.
     */
    public synchronized void applyChanges( Map<String, String> newConfiguration )
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
            List<ConfigurationChange> configurationChanges = new ArrayList<ConfigurationChange>();
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

            // Make the change
            params.clear();
            params.putAll( newConfiguration );

            // Notify listeners
            for ( ConfigurationChangeListener listener : listeners )
            {
                listener.notifyConfigurationChanges( configurationChanges );
            }
        }
    }

    public Iterable<Class<?>> getSettingsClasses()
    {
        return settingsClasses;
    }

    public void setLogger( StringLogger log )
    {
        if ( this.log instanceof BufferingLogger )
        {
            ((BufferingLogger) this.log).replayInto( log );
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
    public void dump( DiagnosticsPhase phase, StringLogger log )
    {
        if ( phase.isInitialization() || phase.isExplicitlyRequested() )
        {
            log.logLongMessage( "Neo4j Kernel properties:", Iterables.map( new Function<Map.Entry<String, String>,
                    String>()
            {
                @Override
                public String apply( Map.Entry<String, String> stringStringEntry )
                {
                    return stringStringEntry.getKey() + "=" + stringStringEntry.getValue();
                }
            }, params.entrySet() ) );
        }
    }

    @Override
    public String toString()
    {
        List<String> keys = new ArrayList<String>( params.keySet() );
        Collections.sort( keys );
        LinkedHashMap<String, String> output = new LinkedHashMap<String, String>();
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
}
