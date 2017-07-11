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
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.neo4j.configuration.ConfigOptions;
import org.neo4j.configuration.ConfigValue;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.config.InvalidSettingException;
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

import static org.neo4j.helpers.collection.MapUtil.stringMap;
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
    public static final String DEFAULT_CONFIG_FILE_NAME = "neo4j.conf";

    private final List<ConfigOptions> configOptions;

    private final Map<String,String> params = new ConcurrentHashMap<>();
    private final ConfigurationMigrator migrator;
    private final List<ConfigurationValidator> validators = new ArrayList<>();
    private File configFile;
    // Messages to this log get replayed into a real logger once logging has been instantiated.
    private Log log = new BufferingLog();
    private ConfigValues settingsFunction;

    /**
     * Builder class for a configuration.
     * <p>
     * The configuration has three layers of values:
     * <ol>
     *   <li>Defaults settings, which is provided by validators.
     *   <li>File settings, parsed from the configuration file if one is provided.
     *   <li>Overridden settings, as provided by the user with the {@link Builder#withSettings(Map)} methods.
     * </ol>
     * They are added in the order specified, and is thus overridden by each layer.
     * <p>
     * Although the builder allows you to override the {@link LoadableConfig}'s with <code>withConfigClasses</code>,
     * this functionality is mainly for testing. If no classes are provided to the builder, they will be located through
     * service loading, and this is probably what you want in most of the cases.
     * <p>
     * Loaded {@link LoadableConfig}'s, whether provided though service loading or explicitly passed, will be scanned
     * for validators that provides migration, validation and default values. Migrators can be specified with the
     * {@link Migrator} annotation and should reside in a class implementing {@link LoadableConfig}.
     */
    public static class Builder
    {
        private Map<String,String> initialSettings = stringMap();
        private List<ConfigurationValidator> validators = new ArrayList<>();
        private File configFile;
        private List<LoadableConfig> settingsClasses;
        private boolean connectorsDisabled;

        /**
         * Augment the configuration with the passed setting.
         *
         * @param setting The setting to set.
         * @param value The value of the setting, pre parsed.
         */
        public Builder withSetting( final Setting<?> setting, final String value )
        {
            return withSetting( setting.name(), value );
        }

        /**
         * Augment the configuration with the passed setting.
         *
         * @param key The setting to set.
         * @param value The value of the setting, pre parsed.
         */
        public Builder withSetting( final String key, final String value )
        {
            initialSettings.put( key, value );
            return this;
        }

        /**
         * Augment the configuration with the passed settings.
         *
         * @param initialSettings settings to augment the configuration with.
         */
        public Builder withSettings( final Map<String,String> initialSettings )
        {
            this.initialSettings.putAll( initialSettings );
            return this;
        }

        /**
         * Set the classes that contains the {@link Setting} fields. If no classes are provided to the builder, they
         * will be located through service loading.
         *
         * @param loadableConfigs A collection fo class instances providing settings.
         */
        @Nonnull
        public Builder withConfigClasses( final Collection<? extends LoadableConfig> loadableConfigs )
        {
            if ( settingsClasses == null )
            {
                settingsClasses = new ArrayList<>();
            }
            settingsClasses.addAll( loadableConfigs );
            return this;
        }

        /**
         * Provide an additional validator. Validators are automatically localed within classes with
         * {@link LoadableConfig}, but this allows you to add others.
         *
         * @param validator an additional validator.
         */
        @Nonnull
        public Builder withValidator( final ConfigurationValidator validator )
        {
            this.validators.add( validator );
            return this;
        }

        /**
         * @see Builder#withValidator(ConfigurationValidator)
         */
        @Nonnull
        public Builder withValidators( final Collection<ConfigurationValidator> validators )
        {
            this.validators.addAll( validators );
            return this;
        }

        /**
         * Extends config with defaults for server, i.e. auth and connector settings.
         */
        @Nonnull
        public Builder withServerDefaults()
        {
            // Add server defaults
            HttpConnector http = new HttpConnector( "http", NONE );
            HttpConnector https = new HttpConnector( "https", TLS );
            BoltConnector bolt = new BoltConnector( "bolt" );
            initialSettings.putIfAbsent( GraphDatabaseSettings.auth_enabled.name(), TRUE );
            initialSettings.putIfAbsent( http.enabled.name(), TRUE );
            initialSettings.putIfAbsent( https.enabled.name(), TRUE );
            initialSettings.putIfAbsent( bolt.enabled.name(), TRUE );

            // Add server validator
            validators.add( new ServerConfigurationValidator() );

            return this;
        }

        /**
         * Provide a file for initial configuration. The settings added with the {@link Builder#withSettings(Map)}
         * methods will be applied on top of the settings in the file.
         *
         * @param configFile A configuration file to parse for initial settings.
         */
        @Nonnull
        public Builder withFile( final @Nullable File configFile )
        {
            this.configFile = configFile;
            return this;
        }

        /**
         * @see Builder#withFile(File)
         */
        @Nonnull
        public Builder withFile( final Path configFile )
        {
            return withFile( configFile.toFile() );
        }

        /**
         * @param configFile an optional configuration file. If not present, this call changes nothing.
         */
        @Nonnull
        public Builder withFile( Optional<File> configFile )
        {
            configFile.ifPresent( file -> this.configFile = file );
            return this;
        }

        /**
         * Specifies the neo4j home directory to be set for this particular config. This will modify {@link
         * GraphDatabaseSettings#neo4j_home} to the same value as provided. If this is not called, the home directory
         * will be set to a system specific default home directory.
         *
         * @param homeDir The home directory this config belongs to.
         */
        @Nonnull
        public Builder withHome( final File homeDir )
        {
            String home = Optional.ofNullable( homeDir ).map( File::getAbsolutePath ).orElse( System.getProperty( "user.dir" ) );
            initialSettings.put( GraphDatabaseSettings.neo4j_home.name(), home );
            return this;
        }

        /**
         * @see Builder#withHome(File)
         */
        @Nonnull
        public Builder withHome( final Path homeDir )
        {
            return withHome( homeDir.toFile() );
        }

        /**
         * This will force all connectors to be disabled during creation of the config. This can be useful if an
         * offline mode is wanted, e.g. in dbms tools or test environments.
         */
        @Nonnull
        public Builder withConnectorsDisabled()
        {
            connectorsDisabled = true;
            return this;
        }

        /**
         * @return The config reflecting the state of the builder.
         * @throws InvalidSettingException is thrown if an invalid setting is encountered and {@link
         * GraphDatabaseSettings#strict_config_validation} is true.
         */
        @Nonnull
        public Config build() throws InvalidSettingException
        {
            List<LoadableConfig> loadableConfigs =
                    Optional.ofNullable( settingsClasses ).orElse( LoadableConfig.allConfigClasses() );

            Config config;
            if ( configFile != null )
            {
                config =  new Config( configFile, initialSettings, validators, loadableConfigs );
            }
            else
            {
                config = new Config( initialSettings, validators, loadableConfigs );
            }

            if ( connectorsDisabled )
            {
                config.augment( config.allConnectorIdentifiers().stream().collect(
                        Collectors.toMap( id -> new Connector( id ).enabled.name(), id -> Settings.FALSE ) ) );
            }

            return config;
        }
    }

    @Nonnull
    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * Convenient method for starting building from a file.
     */
    @Nonnull
    public static Builder fromFile( @Nullable final File configFile )
    {
        return builder().withFile( configFile );
    }

    /**
     * Convenient method for starting building from a file.
     */
    @Nonnull
    public static Builder fromFile( @Nonnull final Path configFile )
    {
        return builder().withFile( configFile );
    }

    /**
     * Convenient method for starting building from initial settings.
     */
    @Nonnull
    public static Builder fromSettings( final Map<String,String> initialSettings )
    {
        return builder().withSettings( initialSettings );
    }

    /**
     * @return a configuration with default values.
     */
    @Nonnull
    public static Config defaults()
    {
        return builder().build();
    }

    /**
     * @param initialSettings a map with settings to be present in the config.
     * @return a configuration with default values augmented with the provided <code>initialSettings</code>.
     */
    @Nonnull
    public static Config defaults( @Nonnull final Map<String,String> initialSettings )
    {
        return builder().withSettings( initialSettings ).build();
    }

    /**
     * Constructs a <code>Config</code> with default values and sets the supplied <code>setting</code> to the <code>value</code>.
     * @param key The initial setting to use.
     * @param value The initial value to give the setting.
     */
    @Nonnull
    public static Config defaults( @Nonnull final String key, @Nonnull final String value )
    {
        return builder().withSetting( key, value ).build();
    }

    /**
     * Constructs a <code>Config</code> with default values and sets the supplied <code>setting</code> to the <code>value</code>.
     * @param setting The initial setting to use.
     * @param value The initial value to give the setting.
     */
    @Nonnull
    public static Config defaults( @Nonnull final Setting<?> setting, @Nonnull final String value )
    {
        return builder().withSetting( setting, value ).build();
    }

    private Config( Map<String,String> initialSettings,
            Collection<ConfigurationValidator> additionalValidators,
            List<LoadableConfig> settingsClasses )
    {
        this( settingsClasses, additionalValidators );
        this.configFile = null;

        Map<String,String> validSettings = migrateAndValidateSettings( initialSettings, false );
        replaceSettings( validSettings );
    }

    private Config( File configFile,
            Map<String,String> overriddenSettings,
            Collection<ConfigurationValidator> additionalValidators,
            List<LoadableConfig> settingsClasses )
    {
        this( settingsClasses, additionalValidators );
        this.configFile = configFile;

        // Read file and override with provided settings
        Map<String,String> settings = readConfigFile();
        settings.putAll( overriddenSettings );

        Map<String,String> validSettings = migrateAndValidateSettings( settings, true );

        warnAboutDeprecations( validSettings );

        replaceSettings( validSettings );
    }

    /**
     * Common base setup for constructor
     */
    private Config( List<LoadableConfig> settingsClasses, Collection<ConfigurationValidator> additionalValidators )
    {
        configOptions = settingsClasses.stream()
                .map( LoadableConfig::getConfigOptions )
                .flatMap( List::stream )
                .collect( Collectors.toList() );

        validators.addAll( additionalValidators );
        migrator = new AnnotationBasedConfigurationMigrator( settingsClasses );
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
     * @throws InvalidSettingException when and invalid setting is found and {@link
     * GraphDatabaseSettings#strict_config_validation} is true.
     */
    public Config augment( Map<String,String> changes ) throws InvalidSettingException
    {
        Map<String,String> params = new HashMap<>( this.params );
        params.putAll( changes );
        Map<String,String> validSettings = migrateAndValidateSettings( params, false );
        replaceSettings( validSettings );
        return this;
    }

    /**
     * Augment the existing config with new settings, overriding any conflicting settings, but keeping all old
     * non-overlapping ones.
     *
     * @param config config to add and override with
     * @return combined config
     * @throws InvalidSettingException when and invalid setting is found and {@link
     * GraphDatabaseSettings#strict_config_validation} is true.
     */
    public Config augment( Config config ) throws InvalidSettingException
    {
        return augment( config.params );
    }

    /**
     * Augment the existing config with new settings, ignoring any conflicting settings.
     *
     * @param additionalDefaults settings to add and override
     * @throws InvalidSettingException when and invalid setting is found and {@link
     * GraphDatabaseSettings#strict_config_validation} is true.
     */
    public Config augmentDefaults( Map<String,String> additionalDefaults ) throws InvalidSettingException
    {
        Map<String,String> params = new HashMap<>( this.params );
        additionalDefaults.forEach( params::putIfAbsent );
        Map<String,String> validSettings = migrateAndValidateSettings( params, false );
        replaceSettings( validSettings );
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
        return Optional.ofNullable( configFile).map( File::toPath );
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

    private void replaceSettings( Map<String,String> validSettings )
    {
        params.clear();
        params.putAll( validSettings );
        settingsFunction = new ConfigValues( params );

        // We only warn when parsing the file so we don't warn about the same setting more than once

    }

    /**
     * Migrates and validates all string values in the provided <code>settings</code> map.
     *
     * @param settings the settings to migrate and validate.
     * @param warnOnUnknownSettings if true method log messages to {@link Config#log}.
     * @return a map of migrated and valid settings.
     * @throws InvalidSettingException when and invalid setting is found and {@link
     * GraphDatabaseSettings#strict_config_validation} is true.
     */
    @Nonnull
    private Map<String,String> migrateAndValidateSettings( Map<String,String> settings, boolean warnOnUnknownSettings )
            throws InvalidSettingException
    {
        Map<String,String> validSettings = migrator.apply( settings, log );
        List<SettingValidator> settingValidators = configOptions.stream()
                .map( ConfigOptions::settingGroup )
                .collect( Collectors.toList() );

        // Validate settings
        validSettings = new IndividualSettingsValidator( warnOnUnknownSettings )
                .validate( settingValidators, validSettings, log, warnOnUnknownSettings );
        for ( ConfigurationValidator validator : validators )
        {
            validSettings = validator.validate( settingValidators, validSettings, log, warnOnUnknownSettings );
        }
        return validSettings;
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

    private Map<String,String> readConfigFile()
    {
        return loadFromFile( configFile, log );
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
        return boltConnectors( params ).collect( Collectors.toList() );
    }

    /**
     * @return stream of all configured bolt connectors
     */
    @Nonnull
    private static Stream<BoltConnector> boltConnectors( @Nonnull Map<String,String> params )
    {
        return allConnectorIdentifiers( params ).stream().map( BoltConnector::new ).filter(
                c -> c.group.groupKey.equalsIgnoreCase( "bolt" ) || BOLT.equals( c.type.apply( params::get ) ) );
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
        return boltConnectors( params )
                .filter( c -> c.enabled.apply( params::get ) )
                .collect( Collectors.toList() );
    }

    /**
     * @return list of all configured http connectors
     */
    @Nonnull
    public List<HttpConnector> httpConnectors()
    {
        return httpConnectors( params ).collect( Collectors.toList() );
    }

    /**
     * @return stream of all configured http connectors
     */
    @Nonnull
    private static Stream<HttpConnector> httpConnectors( @Nonnull Map<String,String> params )
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
                } );
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
    private static List<HttpConnector> enabledHttpConnectors( @Nonnull Map<String,String> params )
    {
        return httpConnectors( params )
                .filter( c -> c.enabled.apply( params::get ) )
                .collect( Collectors.toList() );
    }

    /**
     * Reloads the configuration from the config file used to initiate this instance.
     *
     * @param consumer a consumer of all the detected changes
     * @param dryRun if true, the changes will be reported but not applied
     */
    public void reload( ChangeConsumer consumer, boolean dryRun )
    {
        if ( configFile == null )
        {
            return;
        }

        Map<String,String> settings = readConfigFile();
        Map<String,String> validSettings = migrateAndValidateSettings( settings, true );
        performReload( validSettings, consumer, dryRun );
    }

    /**
     * Calculates the changes to the config and applies them if <code>dryRun</code> is false.
     *
     * @param newRaw to compare with.
     * @param consumer that gets all the changes.
     * @param dryRun if true the changes will not be applied.
     */
    private void performReload( Map<String,String> newRaw, ChangeConsumer consumer, boolean dryRun )
    {
        Map<String,String> oldRaw = this.getRaw();
        Map<String,String> onlyInNew = new LinkedHashMap<>( newRaw );

        for ( Map.Entry<String,String> oldEntry : oldRaw.entrySet() )
        {
            String oldKey = oldEntry.getKey();
            String oldValue = oldEntry.getValue();
            if ( newRaw.containsKey( oldKey ) )
            {
                String newValue = onlyInNew.remove( oldKey );
                if ( !oldValue.equals( newValue ) )
                {
                    // Changed
                    consumer.apply( oldKey, oldValue, newValue );
                    if ( !dryRun )
                    {
                        params.put( oldKey, newValue );
                    }
                }
            }
            else
            {
                // Deleted
                consumer.apply( oldKey, oldValue, null );
                if ( !dryRun )
                {
                    params.remove( oldKey );
                }
            }
        }
        for ( Map.Entry<String,String> newEntry : onlyInNew.entrySet() )
        {
            // Added
            String newKey = newEntry.getKey();
            String newValue = newEntry.getValue();
            consumer.apply( newKey, null, newValue );
            if ( !dryRun )
            {
                params.put( newKey, newValue );
            }
        }
    }

    public interface ChangeConsumer
    {
        /**
         * Called when a difference is found.
         *
         * @param key that this difference applies to.
         * @param oldValue of the property, {@link null} if value was created.
         * @param newValue of the property, {@link null} if value was deleted.
         */
        void apply( String key, String oldValue, String newValue );
    }
}
