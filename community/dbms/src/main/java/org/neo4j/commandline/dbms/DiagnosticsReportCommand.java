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
package org.neo4j.commandline.dbms;

import org.jutils.jprocesses.JProcesses;
import org.jutils.jprocesses.model.ProcessInfo;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.admin.Usage;
import org.neo4j.commandline.arguments.Arguments;
import org.neo4j.commandline.arguments.MandatoryNamedArg;
import org.neo4j.commandline.arguments.OptionalNamedArg;
import org.neo4j.commandline.arguments.PositionalArgument;
import org.neo4j.commandline.arguments.common.OptionalCanonicalPath;
import org.neo4j.dbms.diagnostics.jmx.JmxDump;
import org.neo4j.dbms.diagnostics.jmx.LocalVirtualMachine;
import org.neo4j.diagnostics.DiagnosticsOfflineReportProvider;
import org.neo4j.diagnostics.DiagnosticsReportSource;
import org.neo4j.diagnostics.DiagnosticsReportSources;
import org.neo4j.diagnostics.DiagnosticsReporter;
import org.neo4j.diagnostics.DiagnosticsReporterProgressCallback;
import org.neo4j.diagnostics.InteractiveProgress;
import org.neo4j.diagnostics.NonInteractiveProgress;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.Service;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;

import static org.apache.commons.text.StringEscapeUtils.escapeCsv;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.database_path;

public class DiagnosticsReportCommand implements AdminCommand
{
    private static final OptionalNamedArg destinationArgument =
            new OptionalCanonicalPath( "to", System.getProperty( "java.io.tmpdir" ), "reports" + File.separator,
                    "Destination directory for reports" );
    private static final Arguments arguments = new Arguments()
            .withArgument( new OptionalListArgument() )
            .withArgument( destinationArgument )
            .withArgument( new OptionalVerboseArgument() )
            .withPositionalArgument( new ClassifierFiltersArgument() );

    private final Path homeDir;
    private final Path configDir;
    static final String[] DEFAULT_CLASSIFIERS = new String[]{"logs", "config", "plugins", "tree", "metrics", "threads", "env", "sysprop", "ps"};
    private boolean verbose;
    private final PrintStream err;
    private final PrintStream out;
    private final FileSystemAbstraction fs;

    DiagnosticsReportCommand( Path homeDir, Path configDir, OutsideWorld outsideWorld )
    {
        this.homeDir = homeDir;
        this.configDir = configDir;
        this.fs = outsideWorld.fileSystem();
        this.out = outsideWorld.outStream();
        this.err = outsideWorld.errorStream();
    }

    public static Arguments allArguments()
    {
        return arguments;
    }

    @Override
    public void execute( String[] stringArgs ) throws IncorrectUsage, CommandFailed
    {
        Args args = Args.withFlags( "list", "to", "verbose" ).parse( stringArgs );
        verbose = args.has( "verbose" );

        // Make sure we can access the configuration file
        File configFile = configDir.resolve( Config.DEFAULT_CONFIG_FILE_NAME ).toFile();
        if ( !fs.fileExists( configFile ) )
        {
            throw new CommandFailed( "Unable to find config file, tried: " + configFile.getAbsolutePath() );
        }
        Config config = Config.fromFile( configFile ).withHome( homeDir ).withConnectorsDisabled().build();

        DiagnosticsReporter reporter = createAndRegisterSources( config, configFile );
        Set<String> availableClassifiers = reporter.getAvailableClassifiers();

        // Passing '--list' should print list and end execution
        if ( args.has( "list" ) )
        {
            out.println( "All available classifiers:" );
            for ( String classifier : availableClassifiers )
            {
                out.printf( "  %-10s %s\n", classifier, describeClassifier( classifier ) );
            }
            return;
        }

        // Make sure 'all' is the only classifier if specified
        Set<String> orphans = new HashSet<>( args.orphans() );
        if ( orphans.contains( "all" ) )
        {
            if ( orphans.size() != 1 )
            {
                orphans.remove( "all" );
                throw new IncorrectUsage( "If you specify 'all' this has to be the only classifier. Found ['" +
                        orphans.stream().collect( Collectors.joining( "','" ) ) + "'] as well." );
            }
        }
        else
        {
            // Add default classifiers that are available
            if ( orphans.isEmpty() )
            {
                for ( String classifier : DEFAULT_CLASSIFIERS )
                {
                    if ( availableClassifiers.contains( classifier ) )
                    {
                        orphans.add( classifier );
                    }
                }
            }

            // Validate classifiers
            for ( String classifier : orphans )
            {
                if ( !availableClassifiers.contains( classifier ) )
                {
                    throw new IncorrectUsage( "Unknown classifier: " + classifier );
                }
            }
        }

        DiagnosticsReporterProgressCallback progress;
        if ( System.console() != null )
        {
            progress = new InteractiveProgress( out, verbose );
        }
        else
        {
            progress = new NonInteractiveProgress( out, verbose );
        }

        // Start dumping
        Path destinationDir = new File( destinationArgument.parse( args ) ).toPath();
        try
        {
            SimpleDateFormat dumpFormat = new SimpleDateFormat( "yyyy-MM-dd_HHmmss" );
            Path reportFile = destinationDir.resolve( dumpFormat.format( new Date() ) + ".zip" );
            out.println( "Writing report to " + reportFile.toAbsolutePath().toString() );
            reporter.dump( orphans, reportFile, progress );
        }
        catch ( IOException e )
        {
            throw new CommandFailed( "Creating archive failed", e );
        }
    }

    private DiagnosticsReporter createAndRegisterSources( Config config, File configFile )
    {
        DiagnosticsReporter reporter = new DiagnosticsReporter();
        File storeDirectory = config.get( database_path );

        // Find all offline providers and register them
        for ( DiagnosticsOfflineReportProvider provider : Service.load( DiagnosticsOfflineReportProvider.class ) )
        {
            provider.init( fs, config, storeDirectory );
            reporter.registerOfflineProvider( provider );
        }

        // Register sources provided by this tool
        reporter.registerSource( "config", DiagnosticsReportSources.newDiagnosticsFile( "neo4j.conf", fs, configFile ) );
        reporter.registerSource( "ps", runningProcesses() );

        // Online connection
        Optional<JmxDump> jmxDump = connectToNeo4jInstance();
        if ( jmxDump.isPresent() )
        {
            JmxDump jmx = jmxDump.get();
            reporter.registerSource( "threads", jmx.threadDump() );
            reporter.registerSource( "heap", jmx.heapDump() );
            reporter.registerSource( "sysprop", jmx.systemProperties() );
            reporter.registerSource( "env", jmx.environmentVariables() );
            reporter.registerSource( "activetxs", jmx.listTransactions() );
        }
        return reporter;
    }

    private Optional<JmxDump> connectToNeo4jInstance()
    {
        out.println( "Trying to find running instance of neo4j" );

        Optional<Long> pid = getPid();
        if ( pid.isPresent() )
        {
            try
            {
                LocalVirtualMachine vm = LocalVirtualMachine.from( pid.get() );
                out.println( "Attached to running process with process id " + pid.get() );
                try
                {
                    JmxDump jmxDump = JmxDump.connectTo( vm.getJmxAddress() );
                    jmxDump.attachSystemProperties( vm.getSystemProperties() );
                    out.println( "Connected to JMX endpoint" );
                    return Optional.of( jmxDump );
                }
                catch ( IOException e )
                {
                    printError( "Unable to communicate with JMX endpoint. Reason: " + e.getMessage(), e );
                }
            }
            catch ( IOException e )
            {
                printError( "Unable to connect to process. Reason: " + e.getMessage(), e );
            }
        }
        else
        {
            out.println( "No running instance of neo4j was found. Online reports will be omitted." );
        }

        return Optional.empty();
    }

    private Optional<Long> getPid()
    {
        Path pidFile = homeDir.resolve( "run/neo4j.pid" );
        if ( fs.fileExists( pidFile.toFile() ) )
        {
            try ( BufferedReader reader = new BufferedReader( fs.openAsReader( pidFile.toFile(), Charset.defaultCharset() ) ) )
            {
                String pidFileContent = reader.readLine();
                try
                {
                    return Optional.of( Long.parseLong( pidFileContent ) );
                }

                catch ( NumberFormatException e )
                {
                    printError( pidFile.toString() + " does not contain a valid id. Found: " + pidFileContent );
                }
            }
            catch ( IOException e )
            {
                printError( "Error reading the .pid file. Reason: " + e.getMessage(), e );
            }
        }
        return Optional.empty();
    }

    private void printError( String message )
    {
        printError( message, null );
    }

    private void printError( String message, Throwable e )
    {
        err.println( message );
        if ( verbose && e != null )
        {
            e.printStackTrace( err );
        }
    }

    static String describeClassifier( String classifier )
    {
        switch ( classifier )
        {
        case "logs":
            return "include log files";
        case "config":
            return "include configuration file";
        case "plugins":
            return "include a view of the plugin directory";
        case "tree":
            return "include a view of the tree structure of the data directory";
        case "tx":
            return "include transaction logs";
        case "metrics":
            return "include metrics";
        case "threads":
            return "include a thread dump of the running instance";
        case "heap":
            return "include a heap dump";
        case "env":
            return "include a list of all environment variables";
        case "sysprop":
            return "include a list of java system properties";
        case "raft":
            return "include the raft log";
        case "ccstate":
            return "include the current cluster state";
        case "activetxs":
            return "include the output of dbms.listTransactions()";
        case "ps":
            return "include a list of running processes";
        default:
        }
        throw new IllegalArgumentException( "Unknown classifier: " + classifier );
    }

    private static DiagnosticsReportSource runningProcesses()
    {
        return DiagnosticsReportSources.newDiagnosticsString( "ps.csv", () ->
        {
            List<ProcessInfo> processesList = JProcesses.getProcessList();

            StringBuilder sb = new StringBuilder();
            sb.append( escapeCsv( "Process PID" ) ).append( ',' )
                    .append( escapeCsv( "Process Name" ) ).append( ',' )
                    .append( escapeCsv( "Process Time" ) ).append( ',' )
                    .append( escapeCsv( "User" ) ).append( ',' )
                    .append( escapeCsv( "Virtual Memory" ) ).append( ',' )
                    .append( escapeCsv( "Physical Memory" ) ).append( ',' )
                    .append( escapeCsv( "CPU usage" ) ).append( ',' )
                    .append( escapeCsv( "Start Time" ) ).append( ',' )
                    .append( escapeCsv( "Priority" ) ).append( ',' )
                    .append( escapeCsv( "Full command" ) ).append( '\n' );

            for ( final ProcessInfo processInfo : processesList )
            {
                sb.append( processInfo.getPid() ).append( ',' )
                        .append( escapeCsv( processInfo.getName() ) ).append( ',' )
                        .append( processInfo.getTime() ).append( ',' )
                        .append( escapeCsv( processInfo.getUser() ) ).append( ',' )
                        .append( processInfo.getVirtualMemory() ).append( ',' )
                        .append( processInfo.getPhysicalMemory() ).append( ',' )
                        .append( processInfo.getCpuUsage() ).append( ',' )
                        .append( processInfo.getStartTime() ).append( ',' )
                        .append( processInfo.getStartTime() ).append( ',' )
                        .append( escapeCsv( processInfo.getCommand() ) ).append( '\n' );
            }
            return sb.toString();
        } );
    }

    /**
     * Helper class to format output of {@link Usage}. Parsing is done manually in this command module.
     */
    public static class OptionalListArgument extends MandatoryNamedArg
    {
        OptionalListArgument()
        {
            super( "list", "", "List all available classifiers" );
        }

        @Override
        public String optionsListing()
        {
            return "--list";
        }

        @Override
        public String usage()
        {
            return "[--list]";
        }
    }

    /**
     * Helper class to format output of {@link Usage}. Parsing is done manually in this command module.
     */
    public static class OptionalVerboseArgument extends MandatoryNamedArg
    {
        OptionalVerboseArgument()
        {
            super( "verbose", "", "More verbose error messages" );
        }

        @Override
        public String optionsListing()
        {
            return "--verbose";
        }

        @Override
        public String usage()
        {
            return "[--verbose]";
        }
    }

    /**
     * Helper class to format output of {@link Usage}. Parsing is done manually in this command module.
     */
    public static class ClassifierFiltersArgument implements PositionalArgument
    {
        @Override
        public int position()
        {
            return 1;
        }

        @Override
        public String usage()
        {
            return "[all] [<classifier1> <classifier2> ...]";
        }

        @Override
        public String parse( Args parsedArgs )
        {
            throw new UnsupportedOperationException( "no parser exists" );
        }
    }
}
