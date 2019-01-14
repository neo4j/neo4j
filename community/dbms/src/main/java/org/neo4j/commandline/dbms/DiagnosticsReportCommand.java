/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.commandline.dbms;

import org.jutils.jprocesses.JProcesses;
import org.jutils.jprocesses.model.ProcessInfo;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

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
import org.neo4j.dbms.diagnostics.jmx.JMXDumper;
import org.neo4j.dbms.diagnostics.jmx.JmxDump;
import org.neo4j.diagnostics.DiagnosticsReportSource;
import org.neo4j.diagnostics.DiagnosticsReportSources;
import org.neo4j.diagnostics.DiagnosticsReporter;
import org.neo4j.diagnostics.DiagnosticsReporterProgress;
import org.neo4j.diagnostics.InteractiveProgress;
import org.neo4j.diagnostics.NonInteractiveProgress;
import org.neo4j.helpers.Args;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;

import static org.apache.commons.text.StringEscapeUtils.escapeCsv;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.database_path;

public class DiagnosticsReportCommand implements AdminCommand
{
    private static final OptionalNamedArg destinationArgument =
            new OptionalCanonicalPath( "to", System.getProperty( "java.io.tmpdir" ), "reports" + File.separator,
                    "Destination directory for reports" );
    public static final String PID_KEY = "pid";
    private static final long NO_PID = 0;
    private static final Arguments arguments = new Arguments()
            .withArgument( new OptionalListArgument() )
            .withArgument( destinationArgument )
            .withArgument( new OptionalVerboseArgument() )
            .withArgument( new OptionalForceArgument() )
            .withArgument( new OptionalNamedArg( PID_KEY, "1234", "", "Specify process id of running neo4j instance" ) )
            .withPositionalArgument( new ClassifierFiltersArgument() );

    private final Path homeDir;
    private final Path configDir;
    static final String[] DEFAULT_CLASSIFIERS = new String[]{"logs", "config", "plugins", "tree", "metrics", "threads", "env", "sysprop", "ps"};

    private JMXDumper jmxDumper;
    private boolean verbose;
    private final PrintStream out;
    private final FileSystemAbstraction fs;
    private final PrintStream err;
    private static final DateTimeFormatter filenameDateTimeFormatter =
            new DateTimeFormatterBuilder().appendPattern( "yyyy-MM-dd_HHmmss" ).toFormatter();
    private long pid;

    DiagnosticsReportCommand( Path homeDir, Path configDir, OutsideWorld outsideWorld )
    {
        this.homeDir = homeDir;
        this.configDir = configDir;
        this.fs = outsideWorld.fileSystem();
        this.out = outsideWorld.outStream();
        err = outsideWorld.errorStream();
    }

    public static Arguments allArguments()
    {
        return arguments;
    }

    @Override
    public void execute( String[] stringArgs ) throws IncorrectUsage, CommandFailed
    {
        Args args = Args.withFlags( "list", "to", "verbose", "force", PID_KEY ).parse( stringArgs );
        verbose = args.has( "verbose" );
        jmxDumper = new JMXDumper( homeDir, fs, out, err, verbose );
        pid = parsePid( args );
        boolean force = args.has( "force" );

        DiagnosticsReporter reporter = createAndRegisterSources();

        Optional<Set<String>> classifiers = parseAndValidateArguments( args, reporter );
        if ( !classifiers.isPresent() )
        {
            return;
        }

        DiagnosticsReporterProgress progress = buildProgress();

        // Start dumping
        Path destinationDir = new File( destinationArgument.parse( args ) ).toPath();
        try
        {
            Path reportFile = destinationDir.resolve( getDefaultFilename() );
            out.println( "Writing report to " + reportFile.toAbsolutePath().toString() );
            reporter.dump( classifiers.get(), reportFile, progress, force );
        }
        catch ( IOException e )
        {
            throw new CommandFailed( "Creating archive failed", e );
        }
    }

    private static long parsePid( Args args ) throws CommandFailed
    {
        if ( args.has( PID_KEY ) )
        {
            try
            {
                return Long.parseLong( args.get( PID_KEY, "" ) );
            }
            catch ( NumberFormatException e )
            {
                throw new CommandFailed( "Unable to parse --" + PID_KEY, e );
            }
        }
        return NO_PID;
    }

    private String getDefaultFilename() throws UnknownHostException
    {
        String hostName = InetAddress.getLocalHost().getHostName();
        String safeFilename = hostName.replaceAll( "[^a-zA-Z0-9._]+", "_" );
        return safeFilename + "-" + LocalDateTime.now().format( filenameDateTimeFormatter ) + ".zip";
    }

    private DiagnosticsReporterProgress buildProgress()
    {
        DiagnosticsReporterProgress progress;
        if ( System.console() != null )
        {
            progress = new InteractiveProgress( out, verbose );
        }
        else
        {
            progress = new NonInteractiveProgress( out, verbose );
        }
        return progress;
    }

    private Optional<Set<String>> parseAndValidateArguments( Args args, DiagnosticsReporter reporter ) throws IncorrectUsage
    {
        Set<String> availableClassifiers = reporter.getAvailableClassifiers();

        // Passing '--list' should print list and end execution
        if ( args.has( "list" ) )
        {
            listClassifiers( availableClassifiers );
            return Optional.empty();
        }

        // Make sure 'all' is the only classifier if specified
        Set<String> classifiers = new TreeSet<>( args.orphans() );
        if ( classifiers.contains( "all" ) )
        {
            if ( classifiers.size() != 1 )
            {
                classifiers.remove( "all" );
                throw new IncorrectUsage(
                        "If you specify 'all' this has to be the only classifier. Found ['" + String.join( "','", classifiers ) + "'] as well." );
            }
        }
        else
        {
            // Add default classifiers that are available
            if ( classifiers.isEmpty() )
            {
                addDefaultClassifiers( availableClassifiers, classifiers );
            }

            validateClassifiers( availableClassifiers, classifiers );
        }
        return Optional.of( classifiers );
    }

    private void validateClassifiers( Set<String> availableClassifiers, Set<String> orphans ) throws IncorrectUsage
    {
        for ( String classifier : orphans )
        {
            if ( !availableClassifiers.contains( classifier ) )
            {
                throw new IncorrectUsage( "Unknown classifier: " + classifier );
            }
        }
    }

    private void addDefaultClassifiers( Set<String> availableClassifiers, Set<String> orphans )
    {
        for ( String classifier : DEFAULT_CLASSIFIERS )
        {
            if ( availableClassifiers.contains( classifier ) )
            {
                orphans.add( classifier );
            }
        }
    }

    private void listClassifiers( Set<String> availableClassifiers )
    {
        out.println( "All available classifiers:" );
        for ( String classifier : availableClassifiers )
        {
            out.printf( "  %-10s %s%n", classifier, describeClassifier( classifier ) );
        }
    }

    private DiagnosticsReporter createAndRegisterSources() throws CommandFailed
    {
        DiagnosticsReporter reporter = new DiagnosticsReporter();
        File configFile = configDir.resolve( Config.DEFAULT_CONFIG_FILE_NAME ).toFile();
        Config config = getConfig( configFile );

        File storeDirectory = config.get( database_path );

        reporter.registerAllOfflineProviders( config, storeDirectory, this.fs );

        // Register sources provided by this tool
        reporter.registerSource( "config",
                DiagnosticsReportSources.newDiagnosticsFile( "neo4j.conf", fs, configFile ) );

        reporter.registerSource( "ps", runningProcesses() );

        // Online connection
        registerJMXSources( reporter );
        return reporter;
    }

    private void registerJMXSources( DiagnosticsReporter reporter )
    {
        Optional<JmxDump> jmxDump;
        if ( pid == NO_PID )
        {
            jmxDump = jmxDumper.getJMXDump();
        }
        else
        {
            jmxDump = jmxDumper.getJMXDump( pid );
        }
        jmxDump.ifPresent( jmx ->
        {
            reporter.registerSource( "threads", jmx.threadDump() );
            reporter.registerSource( "heap", jmx.heapDump() );
            reporter.registerSource( "sysprop", jmx.systemProperties() );
            reporter.registerSource( "env", jmx.environmentVariables() );
            reporter.registerSource( "activetxs", jmx.listTransactions() );
        } );
    }

    private Config getConfig( File configFile ) throws CommandFailed
    {
        if ( !fs.fileExists( configFile ) )
        {
            throw new CommandFailed( "Unable to find config file, tried: " + configFile.getAbsolutePath() );
        }
        return Config.fromFile( configFile ).withHome( homeDir ).withConnectorsDisabled().build();
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
    public static class OptionalForceArgument extends MandatoryNamedArg
    {
        OptionalForceArgument()
        {
            super( "force", "", "Ignore disk full warning" );
        }

        @Override
        public String optionsListing()
        {
            return "--force";
        }

        @Override
        public String usage()
        {
            return "[--force]";
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
