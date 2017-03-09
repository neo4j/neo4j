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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import org.neo4j.configuration.ConfigOptions;
import org.neo4j.configuration.ConfigValue;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.config.SettingValidator;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.HttpConnector.Encryption;
import org.neo4j.kernel.info.DiagnosticsPhase;
import org.neo4j.kernel.info.DiagnosticsProvider;
import org.neo4j.logging.BufferingLog;
import org.neo4j.logging.Log;
import org.neo4j.logging.Logger;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.neo4j.kernel.configuration.Connector.ConnectorType.BOLT;
import static org.neo4j.kernel.configuration.Connector.ConnectorType.HTTP;
import static org.neo4j.kernel.configuration.HttpConnector.Encryption.NONE;
import static org.neo4j.kernel.configuration.HttpConnector.Encryption.TLS;
import static org.neo4j.kernel.configuration.Settings.TRUE;

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
    // Messages to this log get replayed into a real logger once logging has been instantiated.
    private Log log;

    /**
     * @return a configuration with embedded defaults
     */
    public static Config empty()
    {
        return embeddedDefaults( Optional.empty() );
    }

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
        return Config.embeddedDefaults( Optional.empty(), additionalConfig, additionalValidators );
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

    /**
     * @return a configuration with server defaults
     */
    public static Config serverDefaults()
    {
        return serverDefaults( Optional.empty(), emptyMap(), emptyList() );
    }

    /**
     * @return a configuration with server defaults
     */
    public static Config serverDefaults( Map<String,String> additionalConfig )
    {
        return serverDefaults( Optional.empty(), additionalConfig, emptyList() );
    }

    /**
     * @return a configuration with server defaults
     */
    public static Config serverDefaults( Optional<File> configFile, Map<String,String> additionalConfig,
            Collection<ConfigurationValidator> additionalValidators )
    {
        ArrayList<ConfigurationValidator> validators = new ArrayList<>();
        validators.addAll( additionalValidators );
        validators.add( new ServerConfigurationValidator() );

        HttpConnector http = new HttpConnector( "http", NONE );
        HttpConnector https = new HttpConnector( "https", TLS );
        BoltConnector bolt = new BoltConnector( "bolt" );
        return new Config( configFile,
                additionalConfig,
                settings ->
                {
                    settings.putIfAbsent( GraphDatabaseSettings.auth_enabled.name(), TRUE );
                    settings.putIfAbsent( http.enabled.name(), TRUE );
                    settings.putIfAbsent( https.enabled.name(), TRUE );
                    settings.putIfAbsent( bolt.enabled.name(), TRUE );
                },
                validators,
                Optional.empty() );

    }

    private Config( Optional<File> configFile,
            Map<String,String> overriddenSettings,
            Consumer<Map<String,String>> settingsPostProcessor,
            Collection<ConfigurationValidator> additionalValidators,
            Optional<Log> log )
    {
        this( configFile, overriddenSettings, settingsPostProcessor, additionalValidators, log,
                LoadableConfig.allConfigClasses() );
    }

    /**
     * Only package-local to support tests of this class. Other uses should use public factory methods.
     */
    Config( Optional<File> configFile,
            Map<String,String> overriddenSettings,
            Consumer<Map<String,String>> settingsPostProcessor,
            Collection<ConfigurationValidator> additionalValidators,
            Optional<Log> log,
            List<LoadableConfig> settingsClasses )
    {
        this.log = log.orElse( new BufferingLog() );
        this.configFile = configFile;

        configOptions = settingsClasses.stream()
                .map( LoadableConfig::getConfigOptions )
                .flatMap( List::stream )
                .collect( Collectors.toList() );

        validators.addAll( additionalValidators );
        migrator = new AnnotationBasedConfigurationMigrator( settingsClasses );

        Map<String,String> settings = initSettings( configFile, settingsPostProcessor, overriddenSettings, this.log );
        replaceSettings( settings, configFile.isPresent() );
    }

    /**
     * Returns a copy of this config with the given modifications.
     *
     * @return a new modified config, leaves this config unchanged.
     */
    public Config with( Map<String,String> additionalConfig )
    {
        Map<String,String> newParams = new HashMap<>( params ); // copy is returned
        newParams.putAll( additionalConfig );
        return new Config( Optional.empty(), newParams, settings ->
        {
        }, validators, Optional.of( log ) );
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
        return setting.apply( params::get );
    }

    /**
     * Augment the existing config with new settings, overriding any conflicting settings, but keeping all old
     * non-overlapping ones.
     *
     * @param changes settings to add and override
     */
    public Config augment( Map<String,String> changes )
    {
        Map<String,String> params = new HashMap<>( this.params );
        params.putAll( changes );
        replaceSettings( params, false );
        return this;
    }

    /**
     * Specify a log where errors and warnings will be reported.
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
     * @return a map of raw  configuration keys and values
     */
    public Map<String,String> getRaw()
    {
        return new HashMap<>( params );
    }

    /**
     * @return a configured setting
     */
    public Optional<?> getValue( @Nonnull String key )
    {
        return configOptions.stream()
                .map( it -> it.asConfigValues( params ) )
                .flatMap( List::stream )
                .filter( it -> it.name().equals( key ) )
                .map( ConfigValue::value )
                .findFirst()
                .orElse( Optional.empty() );
    }

    /**
     * @return all effective config values
     */
    public Map<String,ConfigValue> getConfigValues()
    {
        return configOptions.stream()
                .map( it -> it.asConfigValues( params ) )
                .flatMap( List::stream )
                .collect( Collectors.toMap( ConfigValue::name, it -> it, ( val1, val2 ) ->
                {
                    throw new RuntimeException( "Duplicate setting: " + val1.name() + ": " + val1 + " and " + val2 );
                } ) );
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

    private synchronized void replaceSettings( Map<String,String> userSettings, boolean warnOnUnknownSettings )
    {
        Map<String,String> validSettings = migrator.apply( userSettings, log );
        List<SettingValidator> settingValidators = configOptions.stream()
                .map( ConfigOptions::settingGroup )
                .collect( Collectors.toList() );
        // Validate settings
        validSettings = new IndividualSettingsValidator( warnOnUnknownSettings )
                .validate( settingValidators, validSettings, log, configFile.isPresent() );
        for ( ConfigurationValidator validator : validators )
        {
            validSettings = validator.validate( settingValidators, validSettings, log, configFile.isPresent() );
        }

        params.clear();
        params.putAll( validSettings );

        // We only warn when parsing the file so we don't warn about the same setting more than once
        configFile.ifPresent( file -> warnAboutDeprecations( userSettings ) );
    }

    private void warnAboutDeprecations( Map<String,String> userSettings )
    {
        configOptions.stream()
                .flatMap( it -> it.asConfigValues( userSettings ).stream() )
                .filter( config -> userSettings.containsKey( config.name() ) && config.deprecated() )
                .forEach( c ->
                {
                    if ( c.replacement().isPresent() )
                    {
                        log.warn( "%s is deprecated. Replaced by %s", c.name(), c.replacement().get() );
                    }
                    else
                    {
                        log.warn( "%s is deprecated.", c.name() );
                    }
                } );
    }

    @Nonnull
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

    @Nonnull
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

    /**
     * @return a list of all connector names like 'http' in 'dbms.connector.http.enabled = true'
     */
    @Nonnull
    public List<String> allConnectorIdentifiers()
    {
        return allConnectorIdentifiers( params );
    }

    /**
     * @return a list of all connector names like 'http' in 'dbms.connector.http.enabled = true'
     */
    @Nonnull
    public static List<String> allConnectorIdentifiers( @Nonnull Map<String,String> params )
    {
        Pattern pattern = Pattern.compile(
                Pattern.quote( "dbms.connector." ) + "([^\\.]+)\\.(.+)" );

        return params.keySet().stream()
                .map( pattern::matcher )
                .filter( Matcher::matches )
                .map( match -> match.group( 1 ) )
                .distinct()
                .collect( Collectors.toList() );
    }

    /**
     * @return list of all configured bolt connectors
     */
    @Nonnull
    public List<BoltConnector> boltConnectors()
    {
        return boltConnectors( params );
    }

    /**
     * @return list of all configured bolt connectors
     */
    @Nonnull
    public static List<BoltConnector> boltConnectors( @Nonnull Map<String,String> params )
    {
        return allConnectorIdentifiers( params ).stream()
                .map( BoltConnector::new )
                .filter( c ->
                        c.group.groupKey.equalsIgnoreCase( "bolt" ) || BOLT.equals( c.type.apply( params::get ) ) )
                .collect( Collectors.toList() );
    }

    /**
     * @return list of all configured bolt connectors which are enabled
     */
    @Nonnull
    public List<BoltConnector> enabledBoltConnectors()
    {
        return enabledBoltConnectors( params );
    }

    /**
     * @return list of all configured bolt connectors which are enabled
     */
    @Nonnull
    public static List<BoltConnector> enabledBoltConnectors( @Nonnull Map<String,String> params )
    {
        return boltConnectors( params ).stream()
                .filter( c -> c.enabled.apply( params::get ) )
                .collect( Collectors.toList() );
    }

    /**
     * @return list of all configured http connectors
     */
    @Nonnull
    public List<HttpConnector> httpConnectors()
    {
        return httpConnectors( params );
    }

    /**
     * @return list of all configured http connectors
     */
    @Nonnull
    public static List<HttpConnector> httpConnectors( @Nonnull Map<String,String> params )
    {
        return allConnectorIdentifiers( params ).stream()
                .map( Connector::new )
                .filter( c -> c.group.groupKey.equalsIgnoreCase( "http" ) ||
                        c.group.groupKey.equalsIgnoreCase( "https" ) ||
                        HTTP.equals( c.type.apply( params::get ) ) )
                .map( c ->
                {
                    final String name = c.group.groupKey;
                    final Encryption defaultEncryption;
                    switch ( name )
                    {
                    case "https":
                        defaultEncryption = TLS;
                        break;
                    case "http":
                    default:
                        defaultEncryption = NONE;
                        break;
                    }

                    return new HttpConnector( name,
                            HttpConnectorValidator.encryptionSetting( name, defaultEncryption ).apply( params::get ) );
                } )
                .collect( Collectors.toList() );
    }

    /**
     * @return list of all configured http connectors which are enabled
     */
    @Nonnull
    public List<HttpConnector> enabledHttpConnectors()
    {
        return enabledHttpConnectors( params );
    }

    /**
     * @return list of all configured http connectors which are enabled
     */
    @Nonnull
    public static List<HttpConnector> enabledHttpConnectors( @Nonnull Map<String,String> params )
    {
        return httpConnectors( params ).stream()
                .filter( c -> c.enabled.apply( params::get ) )
                .collect( Collectors.toList() );
    }
}
