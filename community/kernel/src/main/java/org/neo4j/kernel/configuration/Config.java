/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import org.neo4j.configuration.ConfigOptions;
import org.neo4j.configuration.ConfigValue;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.config.SettingValidator;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.info.DiagnosticsPhase;
import org.neo4j.kernel.info.DiagnosticsProvider;
import org.neo4j.logging.BufferingLog;
import org.neo4j.logging.Log;
import org.neo4j.logging.Logger;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

/**
 * This class holds the overall configuration of a Neo4j database instance. Use the accessors to convert the internal
 * key-value settings to other types.
 * <p>
 * Users can assume that old settings have been migrated to their new counterparts, and that defaults have been
 * applied.
 */
public class Config implements DiagnosticsProvider, Configuration
{
    private final List<ConfigOptions> configOptions;

    private final Map<String,String> params = new ConcurrentHashMap<>();
    private final ConfigurationMigrator migrator;
    private final Optional<File> configFile;
    private final List<ConfigurationValidator> validators = new ArrayList<>();


    private ConfigValues settingsFunction;

    // Messages to this log get replayed into a real logger once logging has been instantiated.
    private Log log;

    /**
     * @return a configuration with embedded defaults
     */
    public static Config defaults()
    {
        return embeddedDefaults( Optional.empty() );
    }

    /**
     * @return a configuration with embedded defaults
     */
    public static Config embeddedDefaults( Map<String,String> additionalConfig )
    {
        return embeddedDefaults( Optional.empty(), additionalConfig );
    }

    /**
     * @return a configuration with embedded defaults
     */
    public static Config embeddedDefaults( Optional<File> configFile )
    {
        return embeddedDefaults( configFile, emptyMap() );
    }

    /**
     * @return a configuration with embedded defaults
     */
    public static Config embeddedDefaults( ConfigurationValidator... validators )
    {
        return embeddedDefaults( Optional.empty(), emptyMap(), asList( validators ) );
    }

    /**
     * @return a configuration with embedded defaults
     */
    public static Config embeddedDefaults( Optional<File> configFile, Map<String,String> additionalConfig )
    {
        return embeddedDefaults( configFile, additionalConfig, emptyList() );
    }

    /**
     * @return a configuration with embedded defaults
     */
    public static Config embeddedDefaults( Map<String,String> additionalConfig,
            Collection<ConfigurationValidator> additionalValidators )
    {
        return new Config( Optional.empty(), additionalConfig, settings -> {}, additionalValidators, Optional.empty() );
    }

    /**
     * @return a configuration with embedded defaults
     */
    public static Config embeddedDefaults( Optional<File> configFile, Map<String,String> additionalConfig,
            Collection<ConfigurationValidator> additionalValidators )
    {
        return new Config( configFile, additionalConfig, settings ->
        {
        }, additionalValidators, Optional.empty() );
    }

    public Config( Optional<File> configFile,
            Map<String,String> overriddenSettings,
            Consumer<Map<String,String>> settingsPostProcessor,
            Collection<ConfigurationValidator> additionalValidators,
            Optional<Log> log )
    {
        this.log = log.orElse( new BufferingLog() );
        this.configFile = configFile;
        List<LoadableConfig> settingsClasses = LoadableConfig.allConfigClasses();

        configOptions = settingsClasses.stream()
                .map( LoadableConfig::getConfigOptions )
                .flatMap( List::stream )
                .collect( Collectors.toList() );

        validators.addAll( additionalValidators );
        migrator = new AnnotationBasedConfigurationMigrator( settingsClasses );

        Map<String,String> settings = initSettings( configFile, settingsPostProcessor, overriddenSettings, this.log );
        replaceSettings( settings );
    }

    /**
     * Returns a copy of this config with the given modifications.
     *
     * @return a new modified config, leaves this config unchanged.
     */
    public Config with( Map<String,String> additionalConfig )
    {
        Map<String,String> newParams = getParams(); // copy is returned
        newParams.putAll( additionalConfig );
        return new Config( Optional.empty(), newParams, settings ->
        {
        }, validators, Optional.of( log ) );
    }

    // TODO: Get rid of this, to allow us to have something more
    // elaborate as internal storage (eg. something that can keep meta data with
    // properties).
    public Map<String,String> getParams()
    {
        return new HashMap<>( this.params );
    }

    /**
     * Returns a copy of this config with the given modifications except for any settings which already have a
     * specified value.
     *
     * @return a new modified config, leaves this config unchanged.
     */
    public Config withDefaults( Map<String,String> additionalDefaults )
    {
        Map<String,String> newParams = new HashMap<>( this.params ); // copy is returned
        additionalDefaults.entrySet().forEach( s -> newParams.putIfAbsent( s.getKey(), s.getValue() ) );
        return new Config( Optional.empty(), newParams, settings ->
        {
        }, validators, Optional.of( log ) );
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
    public <T> T view( Function<ConfigValues,T> projection )
    {
        return projection.apply( settingsFunction );
    }

    /**
     * Augment the existing config with new settings, overriding any conflicting settings, but keeping all old
     * non-overlapping ones.
     *
     * @param changes settings to add and override
     */
    public Config augment( Map<String,String> changes )
    {
        Map<String,String> params = getParams();
        params.putAll( changes );
        replaceSettings( params );
        return this;
    }

    /**
     * Replays possible existing messages into this log
     *
     * @param log to use
     */
    public void setLogger( Log log )
    {
        if ( this.log instanceof BufferingLog )
        {
            ((BufferingLog) this.log).replayInto( log );
        }
        this.log = log;
    }

    /**
     * Return the keys of settings which have been configured (via a file or code).
     *
     * @return setting keys
     */
    public Set<String> getConfiguredSettingKeys()
    {
        return new HashSet<>( params.keySet() );
    }


    /**
     * @param key to lookup in the config
     * @return the value or none if it doesn't exist in the config
     */
    public Optional<String> getRaw( @Nonnull String key )
    {
        return Optional.ofNullable( params.get( key ) );
    }

    /**
     * @return a configured setting
     */
    public Optional<?> getSettingValue( @Nonnull String key )
    {
        return configOptions.stream()
                .map( it -> it.asConfigValues( params ) )
                .flatMap( List::stream )
                .filter( it -> it.name().equals( key ) )
                .map( ConfigValue::value )
                .findFirst();
    }

    /**
     * @return all effective config values
     */
    public Map<String,ConfigValue> getConfigValues()
    {
        return configOptions.stream()
                .map( it -> it.asConfigValues( params ) )
                .flatMap( List::stream )
                .collect( Collectors.toMap( ConfigValue::name, it -> it ) );
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
            for ( Map.Entry<String,String> param : params.entrySet() )
            {
                logger.log( "%s=%s", param.getKey(), param.getValue() );
            }
        }
    }

    public Optional<Path> getConfigFile()
    {
        return configFile.map( File::toPath );
    }

    @Override
    public String toString()
    {
        List<String> keys = new ArrayList<>( params.keySet() );
        Collections.sort( keys );
        LinkedHashMap<String,String> output = new LinkedHashMap<>();
        for ( String key : keys )
        {
            output.put( key, params.get( key ) );
        }

        return output.toString();
    }

    private synchronized void replaceSettings( Map<String,String> newSettings )
    {
        Map<String,String> validSettings = migrator.apply( newSettings, log );
        List<SettingValidator> settingValidators = configOptions.stream()
                .map( ConfigOptions::settingGroup )
                .collect( Collectors.toList() );
        validSettings = new IndividualSettingsValidator().validate( settingValidators, validSettings, log );
        for ( ConfigurationValidator validator : validators )
        {
            validSettings = validator.validate( settingValidators, validSettings, log );
        }
        params.clear();
        params.putAll( validSettings );
        settingsFunction = new ConfigValues( params );
    }

    private static Map<String,String> initSettings( @Nonnull Optional<File> configFile,
            @Nonnull Consumer<Map<String,String>> settingsPostProcessor,
            @Nonnull Map<String,String> overriddenSettings,
            @Nonnull Log log )
    {
        Map<String,String> settings = new HashMap<>();
        configFile.ifPresent( file -> settings.putAll( loadFromFile( file, log ) ) );
        settingsPostProcessor.accept( settings );
        settings.putAll( overriddenSettings );
        return settings;
    }

    private static Map<String,String> loadFromFile( @Nonnull File file, @Nonnull Log log )
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
