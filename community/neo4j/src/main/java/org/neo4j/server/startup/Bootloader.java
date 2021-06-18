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

import org.apache.commons.exec.util.StringUtils;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.configuration.BootloaderSettings;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpsConnector;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.time.Stopwatch;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.configuration.SettingValueParsers.INT;
import static org.neo4j.server.startup.BootloaderOsAbstraction.UNKNOWN_PID;

class Bootloader
{
    static final int EXIT_CODE_OK = 0;
    static final int EXIT_CODE_RUNNING = 1;
    static final int EXIT_CODE_NOT_RUNNING = 3;

    static final String ENV_NEO4J_HOME = "NEO4J_HOME";
    static final String ENV_NEO4J_CONF = "NEO4J_CONF";
    static final String ENV_NEO4J_SHUTDOWN_TIMEOUT = "NEO4J_SHUTDOWN_TIMEOUT";
    static final String ENV_NEO4J_START_WAIT = "NEO4J_START_WAIT";
    static final String ENV_HEAP_SIZE = "HEAP_SIZE";
    static final String ENV_JAVA_HOME = "JAVA_HOME";
    static final String ENV_JAVA_CMD = "JAVA_CMD";
    static final String PROP_JAVA_CP = "java.class.path";
    static final String PROP_JAVA_VERSION = "java.version";
    static final String PROP_VM_NAME = "java.vm.name";
    static final String PROP_VM_VENDOR = "java.vm.vendor";
    static final String PROP_BASEDIR = "basedir";

    static final String ARG_EXPAND_COMMANDS = "--expand-commands";

    static final Path DEFAULT_CONFIG_LOCATION = Path.of( Config.DEFAULT_CONFIG_DIR_NAME );
    static final int DEFAULT_NEO4J_SHUTDOWN_TIMEOUT = 120;

    private final BootloaderContext ctx;

    Bootloader( BootloaderContext ctx )
    {
        this.ctx = ctx;
    }

    int start()
    {
        BootloaderOsAbstraction os = ctx.os();
        ctx.validateConfig();
        Long pid = os.getPidIfRunning();
        if ( pid != null )
        {
            ctx.out.printf( "Neo4j is already running%s.%n", pidIfKnown( pid ) );
            return EXIT_CODE_RUNNING;
        }
        printDirectories();
        ctx.out.println( "Starting Neo4j." );
        try
        {
            pid = os.start();
        }
        catch ( BootFailureException e )
        {
            ctx.out.println( "Unable to start. See user log for details." );
            throw e;
        }

        String serverLocation;
        Configuration config = ctx.config();
        if ( config.get( HttpsConnector.enabled ) )
        {
            serverLocation = "It is available at https://" + config.get( HttpsConnector.listen_address );
        }
        else if ( config.get( HttpConnector.enabled ) )
        {
            serverLocation = "It is available at http://" + config.get( HttpConnector.listen_address );
        }
        else
        {
            serverLocation = "Both http & https are disabled.";
        }
        ctx.out.printf( "Started neo4j%s. %s%n", pidIfKnown( pid ), serverLocation );
        ctx.out.println( "There may be a short delay until the server is ready." );
        return EXIT_CODE_OK;
    }

    private void printDirectories()
    {
        Configuration config = ctx.config();

        ctx.out.println( "Directories in use:" );
        ctx.out.println( "home:         " + ctx.home().toAbsolutePath() );
        ctx.out.println( "config:       " + ctx.confDir().toAbsolutePath() );
        ctx.out.println( "logs:         " + config.get( GraphDatabaseSettings.logs_directory ).toAbsolutePath() );
        ctx.out.println( "plugins:      " + config.get( GraphDatabaseSettings.plugin_dir ).toAbsolutePath() );
        ctx.out.println( "import:       " + config.get( GraphDatabaseSettings.load_csv_file_url_root ).toAbsolutePath() );
        ctx.out.println( "data:         " + config.get( GraphDatabaseSettings.data_directory ).toAbsolutePath() );
        ctx.out.println( "certificates: " + ctx.home().resolve( "certificates" ).toAbsolutePath() ); //this is no longer an individual setting
        ctx.out.println( "licenses:     " + config.get( GraphDatabaseSettings.licenses_directory ).toAbsolutePath() );
        ctx.out.println( "run:          " + config.get( BootloaderSettings.run_directory ).toAbsolutePath() );
    }

    int console( boolean dryRun )
    {
        BootloaderOsAbstraction os = ctx.os();
        ctx.validateConfig();
        if ( dryRun )
        {
            List<String> args = os.buildStandardStartArguments();
            String cmd = args.stream().map( StringUtils::quoteArgument ).collect( Collectors.joining( " " ) );
            ctx.out.println( cmd );
            return EXIT_CODE_OK;
        }

        Long pid = os.getPidIfRunning();
        if ( pid != null )
        {
            ctx.out.printf( "Neo4j is already running%s.%n", pidIfKnown( pid ) );
            return EXIT_CODE_RUNNING;
        }
        printDirectories();
        ctx.out.println( "Starting Neo4j." );
        os.console();
        return EXIT_CODE_OK;
    }

    private static String pidIfKnown( long pid )
    {
        return pid != UNKNOWN_PID ? " (pid:" + pid + ")" : "";
    }

    int stop()
    {
        BootloaderOsAbstraction os = ctx.os();
        Long pid = os.getPidIfRunning();
        if ( pid == null )
        {
            ctx.out.println( "Neo4j is not running." );
            return EXIT_CODE_OK;
        }
        ctx.out.print( "Stopping Neo4j." );
        int timeout = ctx.getEnv( ENV_NEO4J_SHUTDOWN_TIMEOUT, DEFAULT_NEO4J_SHUTDOWN_TIMEOUT, INT );
        Stopwatch stopwatch = Stopwatch.start();
        os.stop( pid );
        int printCount = 0;
        do
        {
            if ( os.getPidIfRunning() == null )
            {
                ctx.out.println( " stopped." );
                return EXIT_CODE_OK;
            }

            if ( stopwatch.hasTimedOut( printCount, SECONDS ) )
            {
                printCount++;
                ctx.out.print( "." );
            }
            try
            {
                Thread.sleep( 50 );
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                break;
            }
        }
        while ( !stopwatch.hasTimedOut( timeout, SECONDS ) );

        ctx.out.println( " failed to stop." );
        ctx.out.printf( "Neo4j%s took more than %d seconds to stop.%n", pidIfKnown( pid ), stopwatch.elapsed( SECONDS ) );
        ctx.out.printf( "Please see %s for details.%n", ctx.config().get( GraphDatabaseSettings.store_user_log_path ) );
        return EXIT_CODE_RUNNING;
    }

    int restart()
    {
        int stopCode = stop();
        if ( stopCode != EXIT_CODE_OK )
        {
            return stopCode;
        }
        return start();
    }

    int status()
    {
        Long pid = ctx.os().getPidIfRunning();
        if ( pid == null )
        {
            ctx.out.println( "Neo4j is not running." );
            return EXIT_CODE_NOT_RUNNING;
        }
        ctx.out.printf( "Neo4j is running%s%n", pid != UNKNOWN_PID ? " at pid " + pid : "" );
        return EXIT_CODE_OK;
    }

    int installService()
    {
        ctx.validateConfig();
        if ( ctx.os().serviceInstalled() )
        {
            ctx.out.println( "Neo4j service is already installed" );
            return EXIT_CODE_RUNNING;
        }
        ctx.os().installService();
        ctx.out.println( "Neo4j service installed." );
        return EXIT_CODE_OK;
    }

    int uninstallService()
    {
        if ( !ctx.os().serviceInstalled() )
        {
            ctx.out.println( "Neo4j service is not installed" );
            return EXIT_CODE_OK;
        }
        ctx.os().uninstallService();
        ctx.out.println( "Neo4j service uninstalled." );
        return EXIT_CODE_OK;
    }

    int updateService()
    {
        ctx.validateConfig();
        if ( !ctx.os().serviceInstalled() )
        {
            ctx.out.println( "Neo4j service is not installed" );
            return EXIT_CODE_RUNNING;
        }
        ctx.os().updateService();
        ctx.out.println( "Neo4j service updated." );
        return EXIT_CODE_OK;
    }

    int admin()
    {
        ctx.out.printf( "Selecting JVM - Version:%s, Name:%s, Vendor:%s%n",
                ctx.getProp( PROP_JAVA_VERSION ), ctx.getProp( PROP_VM_NAME ), ctx.getProp( PROP_VM_VENDOR ) );
        try
        {
            ctx.validateConfig();
            ctx.os().admin();
            return EXIT_CODE_OK;
        }
        catch ( BootProcessFailureException e )
        {
            return e.getExitCode(); //NOTE! This is not the generic BootFailureException, it indicates a process non-zero exit, not bootloader failure.
        }
    }
}
