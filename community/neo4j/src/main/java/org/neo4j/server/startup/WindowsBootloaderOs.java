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
import org.eclipse.collections.api.list.MutableList;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.configuration.BootloaderSettings;
import org.neo4j.time.Stopwatch;
import org.neo4j.util.Preconditions;

import static java.lang.String.format;
import static java.util.Arrays.stream;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.commons.lang3.StringUtils.join;
import static org.neo4j.configuration.BootloaderSettings.windows_tools_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.logs_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.store_user_log_path;
import static org.neo4j.server.startup.Bootloader.EXIT_CODE_NOT_RUNNING;
import static org.neo4j.server.startup.ProcessManager.behaviour;

class WindowsBootloaderOs extends BootloaderOsAbstraction
{
    static final String PRUNSRV_AMD_64_EXE = "prunsrv-amd64.exe";
    static final String PRUNSRV_I_386_EXE = "prunsrv-i386.exe";
    private static final int WINDOWS_PATH_MAX_LENGTH = 250;

    WindowsBootloaderOs( BootloaderContext ctx )
    {
        super( ctx );
    }

    @Override
    long start() throws BootFailureException
    {
        if ( !serviceInstalled() )
        {
            throw new BootFailureException( "Neo4j service is not installed", EXIT_CODE_NOT_RUNNING );
        }
        issueServiceCommand( "ES", behaviour().blocking() );
        return UNKNOWN_PID;
    }

    @Override
    void stop( long pid ) throws BootFailureException
    {
        if ( serviceInstalled() )
        {
            issueServiceCommand( "SS", behaviour() );
        }
    }

    @Override
    void installService() throws BootFailureException
    {
        runServiceCommand( "IS" );
    }

    private void runServiceCommand( String baseCommand )
    {
        MutableList<String> argList = baseServiceCommandArgList( baseCommand );
        Path home = ctx.home();
        Path logs = ctx.config().get( logs_directory );
        Path jvmDll = Path.of( getJavaCmd() ).getParent().resolve( Path.of( "server", "jvm.dll" ) );
        Preconditions.checkState( Files.exists( jvmDll ), "Couldn't find the jvm DLL file %s", jvmDll );
        List<String> jvmOpts = getJvmOpts();
        argList
                .with( arg( "--StartMode", "jvm" ) )
                .with( arg( "--StartMethod", "start" ) )
                .with( arg( "--ServiceUser", "LocalSystem" ) )
                .with( arg( "--StartPath", home.toString() ) )
                .with( multiArg( "--StartParams", "--config-dir=" + ctx.confDir(), "--home-dir=" + home ) )
                .with( arg( "--StopMode", "jvm" ) )
                .with( arg( "--StopMethod", "stop" ) )
                .with( arg( "--StopPath", home.toString() ) )
                .with( arg( "--Description", "Neo4j Graph Database - " + home ) )
                .with( arg( "--DisplayName", "Neo4j Graph Database - " + serviceName() ) )
                .with( arg( "--Jvm", jvmDll.toString() ) )
                .with( arg( "--LogPath", logs.toString() ) )
                .with( arg( "--StdOutput", logs.resolve( ctx.config().get( store_user_log_path ) ).toString() ) )
                .with( arg( "--StdError", logs.resolve( "service-error.log" ).toString() ) )
                .with( arg( "--LogPrefix", "neo4j-service" ) )
                .with( arg( "--Classpath", getClassPath() ) )
                .with( multiArg( "--JvmOptions", jvmOpts.toArray( new String[0] ) ) )
                .with( arg( "--Startup", "auto" ) )
                .with( arg( "--StopClass", ctx.entrypoint.getName() ) )
                .with( arg( "--StartClass", ctx.entrypoint.getName() ) );
        for ( String additionalArg : ctx.additionalArgs )
        {
            argList = argList.with( arg( "++StartParams", additionalArg ) );
        }
        // Apparently the Xms/Xmx options are passed in a special form here too
        argList = includeMemoryOption( jvmOpts, argList, "-Xms", "--JvmMs", "Start" );
        argList = includeMemoryOption( jvmOpts, argList, "-Xmx", "--JvmMx", "Max" );
        runProcess( argList, behaviour().inheritIO().blocking() );
    }

    private static String multiArg( String key, String... values )
    {
        // Procrun expects us to split each option with `;` if these characters are used inside the actual option values
        // that will cause problems in parsing. To overcome the problem, we need to escape those characters by placing
        // them inside single quotes.
        List<String> argsEscaped = stream( values )
                .peek( WindowsBootloaderOs::throwIfContainsSingleQuotes )
                .map( opt -> opt.replace( ";", "';'" ) )
                .map( opt -> opt.replace( "#", "'#'" ) )
                .collect( Collectors.toList() );
        return arg( key, join( argsEscaped, ';' ) );
    }

    private static void throwIfContainsSingleQuotes( String s )
    {
        // A limitation/bug in prunsrv not parsing ' characters correctly. It is better to throw exception than fail silently like before
        if ( s.contains( "'" ) )
        {
            var firstIndex = s.indexOf( "'" );
            var context = s.substring( Math.max( firstIndex - 25, 0 ), Math.min( s.length(), firstIndex + 25 ) );
            throw new BootFailureException(
                    format( "We are unable to support values that contain single quote marks ('). Single quotes found in value: %s", context ) );
        }
    }

    private String serviceName()
    {
        return ctx.config().get( BootloaderSettings.windows_service_name );
    }

    @Override
    void uninstallService() throws BootFailureException
    {
        issueServiceCommand( "DS", behaviour().blocking() );
        Stopwatch stopwatch = Stopwatch.start();
        while ( serviceInstalled() && !stopwatch.hasTimedOut( Bootloader.DEFAULT_NEO4J_SHUTDOWN_TIMEOUT, TimeUnit.SECONDS ) )
        {
            try
            {
                Thread.sleep( 300 );
            }
            catch ( InterruptedException ie )
            {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    @Override
    void updateService() throws BootFailureException
    {
        runServiceCommand( "US" );
    }

    @Override
    Long getPidIfRunning()
    {
        String status = getStatus();
        boolean stopped = StringUtils.isEmpty( status ) || status.startsWith( "Stopped" );
        return stopped ? null : UNKNOWN_PID;
    }

    @Override
    boolean serviceInstalled()
    {
        return StringUtils.isNotEmpty( getStatus() );
    }

    private String getStatus()
    {
        try
        {
            // These are the possible states Get-Service can reply with:
            // - Stopped
            // - StartPending
            // - StopPending
            // - Running
            // - ContinuePending
            // - PausePending
            // - Paused
            //
            // It seems plausible to interpret anything other than "Stopped" as running, at least for how the Neo4j boot loader is interacting with it
            return stream( resultFromPowerShellCommand( "Get-Service", serviceName(), "|", "Format-Table", "-AutoSize" ) )
                    .filter( s -> s.contains( serviceName() ) )
                    .findFirst()
                    .orElse( "" );
        }
        catch ( BootFailureException e )
        {
            return ""; //Service did not exist
        }
    }

    private String[] resultFromPowerShellCommand( String... command )
    {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try ( PrintStream out = new PrintStream( buffer ) )
        {
            ctx.processManager().run( asPowershellScript( List.of( command ) ), behaviour().blocking().outputConsumer( out ).errorConsumer( out ) );
            return buffer.toString().split( format( "%n" ) );
        }
    }

    private void issueServiceCommand( String serviceCommand, ProcessManager.Behaviour behaviour )
    {
        runProcess( baseServiceCommandArgList( serviceCommand ), behaviour );
    }

    private void runProcess( List<String> command, ProcessManager.Behaviour behaviour )
    {
        List<String> entireCommand = asExternalCommand( command );
        ctx.processManager().run( entireCommand, behaviour );
        if ( entireCommand.stream().anyMatch( cmd -> cmd.equals( powershellCmd() ) ) &&
                command.stream().anyMatch( cmd -> cmd.endsWith( PRUNSRV_I_386_EXE ) || cmd.endsWith( PRUNSRV_AMD_64_EXE ) ) )
        {
            // This is special condition where we run a command with our prunsrv windows-service util and we have to run it with powershell,
            // probably because we're running a command which exceeds 2000 characters which is the limit of cmd.exe.
            // Since it seems to be really hard to make powershell wait for completion of commands that it runs (we've certainly tried)
            // then we have to try and wait the completion manually here. The general idea is to see if there's any prunsrv process running
            // and we're simply waiting until there is none. Now this is somewhat risky because if there's any other process with the exact same name
            // we'll wait here for the max time. Although know that the PS1 scripts that this was ported from doesn't even have the option to run
            // these prunsrv commands in powershell.exe, it always ran them in cmd.exe. The main cause of a command line being too long for cmd.exe
            // is that the classpath is too long since other things are somewhat fixed and doesn't exceed this limit on any sane environment.
            // And the main reason the classpath is too long is that we're currently running in a test environment, because in a real-world
            // packaging environment the classpath is a couple of wildcard directories.
            Stopwatch stopwatch = Stopwatch.start();
            do
            {
                try
                {
                    resultFromPowerShellCommand( "Get-Process", PRUNSRV_AMD_64_EXE + "," + PRUNSRV_I_386_EXE );
                    // If this command completes normally there's at least one running process containing that name
                    try
                    {
                        Thread.sleep( 100 );
                    }
                    catch ( InterruptedException ie )
                    {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
                catch ( BootFailureException e )
                {
                    // If this command returns exit code != 0 it typically means that there's no processes of this name running
                    break;
                }
            }
            while ( !stopwatch.hasTimedOut( Bootloader.DEFAULT_NEO4J_SHUTDOWN_TIMEOUT, TimeUnit.SECONDS ) );
        }
    }

    private MutableList<String> baseServiceCommandArgList( String serviceCommand )
    {
        return Lists.mutable
                .with( format( "& %s", escapeQuote( findPrunCommand().toString() ) ) )
                .with( format( "//%s//%s", serviceCommand, serviceName() ) );
    }

    private static List<String> asPowershellScript( List<String> command )
    {
        return asExternalCommand( List.of( String.join( " ", command ) ) );
    }

    private static List<String> asExternalCommand( List<String> command )
    {
        // We use powershell rather than cmd.exe because cmd.exe doesn't support large argument lists and will wait.
        // Powershell tolerates much longer argument lists and will not wait
        Stream<String> argsAsOne = command.size() < 2 ? Stream.empty() : Stream
                .of( command.stream().skip( 1 ).map( WindowsBootloaderOs::escapeQuote ).collect( Collectors.joining( " " ) ) );
        return Stream.concat( Stream.of( powershellCmd(), "-OutputFormat", "Text", "-ExecutionPolicy", "Bypass", "-Command", command.get( 0 ) ), argsAsOne )
                     .collect( Collectors.toList() );
    }

    private static String powershellCmd()
    {
        return "powershell.exe";
    }

    private Path findPrunCommand()
    {
        // This is apparently a standard way of finding this out on Windows
        boolean is64bit = isNotEmpty( ctx.getEnv( "ProgramFiles(x86)" ) );
        // These two files are part of the Neo4j packaging
        String prunSrvName = is64bit ? PRUNSRV_AMD_64_EXE : PRUNSRV_I_386_EXE;
        Path tools = ctx.config().get( windows_tools_directory );
        Path path = tools.resolve( prunSrvName );
        Preconditions.checkState( Files.exists( path ), "Couldn't find prunsrv file for interacting with the windows service subsystem %s", path );

        int length = path.toString().length();
        if ( length >= WINDOWS_PATH_MAX_LENGTH )
        {
            ctx.err.printf( "WARNING: Path length over %s characters detected. The service may not work correctly because of limitations in" +
                    " the Windows operating system when dealing with long file paths. Path:%s (length:%s)%n", WINDOWS_PATH_MAX_LENGTH, path, length );
        }
        return path;
    }

    private MutableList<String> includeMemoryOption( List<String> jvmOpts, MutableList<String> argList, String option, String serviceOption,
            String description )
    {
        String memory = findOptionValue( jvmOpts, option );
        if ( memory != null )
        {
            argList = argList.with( arg( serviceOption, memory ) );
            ctx.out.println( "Use JVM " + description + " Memory of " + memory );
        }
        return argList;
    }

    private static String findOptionValue( List<String> opts, String option )
    {
        for ( String opt : opts )
        {
            if ( opt.startsWith( option ) )
            {
                return opt.substring( option.length() );
            }
        }
        return null;
    }

    private static String arg( String key, String value )
    {
        return value == null ? key : format( "%s=%s", key, value );
    }

    private static String escapeQuote( String str )
    {
        // Using single quotes stops powershell from trying to evaluate the contents of the string
        // replace pre-existing single quotes with double single quotes - this is the correct escape mechanism for powershell
        return format( "'%s'", str.replaceAll( "'", "''" ) );
    }
}
