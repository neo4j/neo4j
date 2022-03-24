/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.server.startup;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.collections.api.factory.Lists;

import java.io.PrintStream;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import org.neo4j.configuration.BootloaderSettings;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.SettingValueParser;
import org.neo4j.configuration.SettingValueParsers;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.util.Preconditions;

import static org.neo4j.configuration.SettingValueParsers.PATH;
import static org.neo4j.server.startup.Bootloader.ARG_EXPAND_COMMANDS;
import static org.neo4j.server.startup.Bootloader.DEFAULT_CONFIG_LOCATION;
import static org.neo4j.server.startup.Bootloader.ENV_NEO4J_CONF;
import static org.neo4j.server.startup.Bootloader.ENV_NEO4J_HOME;
import static org.neo4j.server.startup.Bootloader.PROP_BASEDIR;

abstract class BootloaderContext
{
    final PrintStream out;
    final PrintStream err;
    final Class<?> entrypoint;
    private final Function<String,String> envLookup;
    private final Function<String,String> propLookup;
    private final Runtime.Version version;
    final Collection<BootloaderExtension> extensions;

    //init
    private boolean initiated;
    boolean verbose;
    boolean expandCommands;
    List<String> additionalArgs;

    //inferred
    private Path home;
    private Path conf;
    private Configuration config;
    private boolean fullConfig;
    private BootloaderOsAbstraction os;
    private ProcessManager processManager;

    protected BootloaderContext( Class<?> entrypoint )
    {
        this( entrypoint, List.of() );
    }

    protected BootloaderContext( Class<?> entrypoint, Collection<BootloaderExtension> extensions )
    {
        this( System.out, System.err, System::getenv, System::getProperty, entrypoint, Runtime.version(), extensions );
    }

    protected BootloaderContext( PrintStream out, PrintStream err, Function<String,String> envLookup, Function<String,String> propLookup,
                                 Class<?> entrypoint, Runtime.Version version, Collection<BootloaderExtension> extensions )
    {
        this.out = out;
        this.err = err;
        this.envLookup = envLookup;
        this.propLookup = propLookup;
        this.entrypoint = entrypoint;
        this.version = version;
        this.extensions = extensions;
    }

    String getEnv( String key )
    {
        return getEnv( key, "", SettingValueParsers.STRING );
    }

    <T> T getEnv( String key, T defaultValue, SettingValueParser<T> parser )
    {
        return getValue( key, defaultValue, parser, envLookup );
    }

    String getProp( String key )
    {
        return getProp( key, "", SettingValueParsers.STRING );
    }

    <T> T getProp( String key, T defaultValue, SettingValueParser<T> parser )
    {
        return getValue( key, defaultValue, parser, propLookup );
    }

    private <T> T getValue( String key, T defaultValue, SettingValueParser<T> parser, Function<String,String> lookup )
    {
        assertInitiated();
        String value = lookup.apply( key );
        try
        {
            return StringUtils.isNotEmpty( value ) ? parser.parse( value ) : defaultValue;
        }
        catch ( IllegalArgumentException e )
        {
            throw new BootFailureException( "Failed to parse value for " + key + ". " + e.getMessage(), 1, e );
        }
    }

    void init( boolean expandCommands, boolean verbose, String... additionalArgs )
    {
        Preconditions.checkArgument( !initiated, "Context already initiated" );
        initiated = true;

        this.expandCommands = expandCommands;
        this.verbose = verbose;
        this.additionalArgs = Lists.mutable.with( additionalArgs );
        if ( expandCommands )
        {
            this.additionalArgs.add( ARG_EXPAND_COMMANDS );
        }
    }

    Path home()
    {
        if ( home == null )
        {
            assertInitiated();
            Path defaultHome = getProp( PROP_BASEDIR, Path.of( "" ).toAbsolutePath().getParent(), PATH ); //Basedir is provided by the app-assembler
            home = getEnv( ENV_NEO4J_HOME, defaultHome, PATH ).toAbsolutePath(); //But a NEO4J_HOME has higher prio
        }
        return home;
    }

    Path confDir()
    {
        if ( conf == null )
        {
            assertInitiated();
            conf = getEnv( ENV_NEO4J_CONF, home().resolve( DEFAULT_CONFIG_LOCATION ), PATH );
        }
        return conf;
    }

    void validateConfig()
    {
        config( true );
    }

    Configuration config()
    {
        return config( false );
    }

    private Configuration config( boolean full )
    {
        if ( config == null || !fullConfig && full )
        {
            assertInitiated();
            this.config = buildConfig( full );
            this.fullConfig = full;
        }
        return config;
    }

    private Configuration buildConfig( boolean full )
    {
        Path confFile = confDir().resolve( Config.DEFAULT_CONFIG_FILE_NAME );
        try
        {
            Predicate<String> filter = full ? settingsDeclaredInNeo4j()::contains : settingsUsedByBootloader()::contains;

            Configuration config = Config.newBuilder()
                    .commandExpansion( expandCommands )
                    .setDefaults( overriddenDefaultsValues() )
                    .set( GraphDatabaseSettings.neo4j_home, home() )
                    .fromFile( confFile, false, filter )
                    .build();

            return new Configuration()
            {
                @Override
                public <T> T get( Setting<T> setting )
                {
                    if ( filter.test( setting.name() ) )
                    {
                        return config.get( setting );
                    }
                    //This is to prevent silent error and should only be encountered while developing. Just add the setting to the filter!
                    throw new IllegalArgumentException( "Not allowed to read this setting " + setting.name() + ". It has been filtered out" );
                }
            };
        }
        catch ( RuntimeException e )
        {
            throw new BootFailureException( "Failed to read config " + confFile + ": " + e.getMessage(), e );
        }
    }

    private Set<String> settingsUsedByBootloader()
    {
        //These settings are the that might be used by the bootloader minor commands (stop/status etc..)
        //Additional settings are used on the start/console path, but they use the full config anyway so not added here.
        return Set.of(
                GraphDatabaseSettings.neo4j_home.name(),
                GraphDatabaseSettings.logs_directory.name(),
                GraphDatabaseSettings.plugin_dir.name(),
                GraphDatabaseSettings.store_user_log_path.name(),
                GraphDatabaseSettings.strict_config_validation.name(),
                GraphDatabaseInternalSettings.config_command_evaluation_timeout.name(),
                BootloaderSettings.run_directory.name(),
                BootloaderSettings.additional_jvm.name(),
                BootloaderSettings.lib_directory.name(),
                BootloaderSettings.windows_service_name.name(),
                BootloaderSettings.windows_tools_directory.name(),
                BootloaderSettings.pid_file.name()
        );
    }

    private static Set<String> settingsDeclaredInNeo4j()
    {
        // We filter out any settings not declared in Neo4j jars since we can't do strict config validation otherwise
        // E.g settings declared in plugins since the plugin directory is not on the class path for the bootloader
        return Config.defaults().getDeclaredSettings().keySet();
    }

    protected abstract Map<Setting<?>,Object> overriddenDefaultsValues();

    BootloaderOsAbstraction os()
    {
        if ( os == null )
        {
            assertInitiated();
            os = BootloaderOsAbstraction.getOsAbstraction( this );
        }
        return os;
    }

    ProcessManager processManager()
    {
        if ( processManager == null )
        {
            assertInitiated();
            processManager = new ProcessManager( this );
        }
        return processManager;
    }

    private void assertInitiated()
    {
        Preconditions.checkArgument( initiated, "Context not initiated" );
    }

    Runtime.Version version()
    {
        return version;
    }
}
