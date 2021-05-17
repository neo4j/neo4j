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
import org.apache.commons.lang3.SystemUtils;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.MutableList;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.neo4j.configuration.BootloaderSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileUtils;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.neo4j.configuration.BootloaderSettings.initial_heap_size;
import static org.neo4j.configuration.BootloaderSettings.max_heap_size;
import static org.neo4j.server.startup.Bootloader.ENV_HEAP_SIZE;
import static org.neo4j.server.startup.Bootloader.PROP_JAVA_CP;
import static org.neo4j.server.startup.Bootloader.PROP_JAVA_VERSION;
import static org.neo4j.server.startup.Bootloader.PROP_VM_NAME;
import static org.neo4j.server.startup.ProcessManager.behaviour;

abstract class BootloaderOsAbstraction
{
    static final long UNKNOWN_PID = Long.MAX_VALUE;

    protected final BootloaderContext ctx;

    protected BootloaderOsAbstraction( BootloaderContext ctx )
    {
        this.ctx = ctx;
    }

    abstract Long getPidIfRunning();

    abstract long start() throws BootFailureException;

    abstract void stop( long pid ) throws BootFailureException;

    long console() throws BootFailureException
    {
        return ctx.processManager().run( buildStandardStartArguments(), consoleBehaviour() );
    }

    protected ProcessManager.Behaviour consoleBehaviour()
    {
        return behaviour().blocking().inheritIO().withShutdownHook();
    }

    long admin() throws BootFailureException
    {
        MutableList<String> arguments = buildBaseArguments();
        if ( ctx.getEnv( ENV_HEAP_SIZE ).isBlank() )
        {
            // Server config is used as one source of heap settings for admin commands.
            // That leads to ridiculous memory demands especially by simple admin commands
            // like getting store info.
            arguments = arguments.reject( argument -> argument.startsWith( "-Xms" ) );
        }
        return ctx.processManager().run( arguments.withAll( ctx.additionalArgs ), behaviour().blocking().inheritIO().homeAndConfAsEnv() );
    }

    abstract void installService() throws BootFailureException;

    abstract void uninstallService() throws BootFailureException;

    abstract void updateService() throws BootFailureException;

    abstract boolean serviceInstalled();

    protected List<String> buildStandardStartArguments()
    {
        return buildBaseArguments()
                .with( "--home-dir=" + ctx.home() )
                .with( "--config-dir=" + ctx.confDir() )
                .withAll( ctx.additionalArgs );
    }

    private MutableList<String> buildBaseArguments()
    {
        return Lists.mutable
                .with( getJavaCmd() )
                .with( "-cp" ).with( getClassPath() )
                .withAll( getJvmOpts() )
                .with( ctx.entrypoint.getName() );
    }

    static BootloaderOsAbstraction getOsAbstraction( BootloaderContext context )
    {
        return SystemUtils.IS_OS_WINDOWS ? new WindowsBootloaderOs( context ) :
               SystemUtils.IS_OS_MAC_OSX ? new MacBootloaderOs( context ) : new UnixBootloaderOs( context );
    }

    protected static String executeCommand( String[] command )
    {
        Process process = null;
        try
        {
            process = new ProcessBuilder( command ).start();
            if ( !process.waitFor( 30, TimeUnit.SECONDS ) )
            {
                throw new IllegalStateException( format( "Timed out executing command `%s`", String.join(" ", command ) ) );
            }

            String output = StringUtils.trimToEmpty( new String( process.getInputStream().readAllBytes() ) );

            int exitCode = process.exitValue();
            if ( exitCode != 0 )
            {
                String errOutput = new String( process.getErrorStream().readAllBytes() );
                throw new IllegalStateException( format( "Command `%s` failed with exit code %s.%n%s%n%s",
                        String.join(" ", command ), exitCode, output, errOutput ) );
            }
            return output;
        }
        catch ( IOException | InterruptedException e )
        {
            throw new IllegalStateException( e );
        }
        finally
        {
            if ( process != null && process.isAlive() )
            {
                process.destroyForcibly();
            }
        }
    }

    protected String getJavaCmd()
    {
        Path java = getJava();
        checkJavaVersion();
        return java.toString();
    }

    private void printBadRuntime()
    {
        ctx.err.println( "WARNING! You are using an unsupported Java runtime." );
        ctx.err.println( "* Please use Oracle(R) Java(TM) 11, OpenJDK(TM) 11 to run Neo4j." );
        ctx.err.println( "* Please see https://neo4j.com/docs/ for Neo4j installation instructions." );
    }

    private static Path getJava()
    {
        Optional<String> currentCommand = ProcessHandle.current().info().command();
        return Path.of( currentCommand.orElseThrow( () -> new IllegalStateException( "Wasn't able to figure out java binary" ) ) );
    }

    private void checkJavaVersion()
    {
        //Is it okay to check this on the boostrapper VM or should it be done on the "real"?
        String version = ctx.getProp( PROP_JAVA_VERSION );
        if ( !version.startsWith( "11" ) )
        {
            //too new java
            printBadRuntime();
        }
        else
        {
            //correct version
            String runtime = ctx.getProp( PROP_VM_NAME );
            if ( !runtime.matches( "(Java HotSpot\\(TM\\)|OpenJDK) (64-Bit Server|Server|Client) VM" ) )
            {
                printBadRuntime();
            }
        }
    }

    private static String bytesToSuitableJvmString( long bytes )
    {
        // the JVM accepts k,m,g but we go with k to avoid conversion loss
        return Math.max( bytes / ByteUnit.kibiBytes( 1 ), 1 ) + "k";
    }

    protected List<String> getJvmOpts()
    {
        MutableList<String> opts = Lists.mutable.empty();
        String xmsValue;
        String xmxValue;
        String envHeapSize = ctx.getEnv( ENV_HEAP_SIZE );
        Configuration config = ctx.config();
        if ( isNotEmpty( envHeapSize ) )
        {
            // HEAP_SIZE env. variable has highest prio
            xmsValue = envHeapSize;
            xmxValue = envHeapSize;
        }
        else
        {
            Long xmsConfigValue = config.get( initial_heap_size );
            Long xmxConfigValue = config.get( max_heap_size );
            xmsValue = xmsConfigValue != null ? bytesToSuitableJvmString( xmsConfigValue ) : null;
            xmxValue = xmxConfigValue != null ? bytesToSuitableJvmString( xmxConfigValue ) : null;
        }
        if ( xmsValue != null )
        {
            opts.with( "-Xms" + xmsValue );
        }
        if ( xmxValue != null )
        {
            opts.with( "-Xmx" + xmxValue );
        }

        String jvmAdditionals = config.get( BootloaderSettings.additional_jvm );
        if ( isNotEmpty( jvmAdditionals ) )
        {
            opts.withAll( List.of( jvmAdditionals.split( System.lineSeparator() ) ) );
        }

        if ( config.get( BootloaderSettings.gc_logging_enabled ) )
        {
            opts.with( String.format( "%s:file=%s::filecount=%s,filesize=%s",
                    config.get( BootloaderSettings.gc_logging_options ),
                    config.get( GraphDatabaseSettings.logs_directory ).resolve( "gc.log" ),
                    config.get( BootloaderSettings.gc_logging_rotation_keep_number ),
                    bytesToSuitableJvmString( config.get( BootloaderSettings.gc_logging_rotation_size ) )
            ) );
        }
        opts.with( "-Dfile.encoding=UTF-8" );
        return opts;
    }

    protected String getClassPath()
    {
        String libCp = classPathFromDir( ctx.config().get( BootloaderSettings.lib_directory ) );

        List<String> paths = Lists.mutable.with(
                classPathFromDir( ctx.config().get( GraphDatabaseSettings.plugin_dir ) ),
                classPathFromDir( ctx.confDir() ),
                StringUtils.isNotBlank( libCp ) ? libCp : ctx.getProp( PROP_JAVA_CP )
        );
        return paths.stream().filter( StringUtils::isNotBlank ).collect( Collectors.joining( File.pathSeparator ) );
    }

    private static String classPathFromDir( Path dir )
    {
        try
        {
            if ( Files.isDirectory( dir ) && !FileUtils.isDirectoryEmpty( dir ) )
            {
                return dir.toAbsolutePath() + File.separator + "*";
            }
        }
        catch ( IOException e )
        { //Ignore. Default to this JVMs classpath
        }
        return null;
    }
}
