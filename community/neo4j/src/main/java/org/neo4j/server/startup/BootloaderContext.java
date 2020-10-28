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
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.SettingValueParser;
import org.neo4j.configuration.SettingValueParsers;
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

    //init
    private boolean initiated;
    boolean verbose;
    boolean expandCommands;
    List<String> additionalArgs;

    //inferred
    private Path home;
    private Path conf;
    private Config config;
    private BootloaderOsAbstraction os;
    private ProcessManager processManager;

    protected BootloaderContext( Class<?> entrypoint )
    {
        this( System.out, System.err, System::getenv, System::getProperty, entrypoint );
    }

    protected BootloaderContext( PrintStream out, PrintStream err, Function<String,String> envLookup, Function<String,String> propLookup, Class<?> entrypoint )
    {
        this.out = out;
        this.err = err;
        this.envLookup = envLookup;
        this.propLookup = propLookup;
        this.entrypoint = entrypoint;
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

    Config config()
    {
        if ( config == null )
        {
            assertInitiated();
            Path confFile = confDir().resolve( Config.DEFAULT_CONFIG_FILE_NAME );
            try
            {
                this.config = Config.newBuilder()
                        .commandExpansion( expandCommands )
                        .setDefaults( overriddenDefaultsValues() )
                        .set( GraphDatabaseSettings.neo4j_home, home() )
                        .fromFileNoThrow( confFile )
                        .build();
            }
            catch ( RuntimeException e )
            {
                throw new BootFailureException( "Failed to read config " + confFile + ": " + e.getMessage(), e );
            }
        }
        return config;
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
}
