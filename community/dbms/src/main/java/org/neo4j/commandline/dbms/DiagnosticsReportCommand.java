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
package org.neo4j.commandline.dbms;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
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
import org.neo4j.dbms.report.jmx.JmxDump;
import org.neo4j.dbms.report.jmx.LocalVirtualMachine;
import org.neo4j.diagnostics.DiagnosticsOfflineReportProvider;
import org.neo4j.diagnostics.DiagnosticsReportSources;
import org.neo4j.diagnostics.DiagnosticsReporter;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.Service;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.database_path;

public class DiagnosticsReportCommand implements AdminCommand
{
    private static OptionalNamedArg destinationArgument =
            new OptionalCanonicalPath( "to", "/tmp/", "reports/", "Destination directory for reports" );
    private static final Arguments arguments =
            new Arguments().withArgument( new OptionalListArgument() ).withArgument( destinationArgument )
                    .withPositionalArgument( new ClassifierFiltersArgument() );
    private final Path homeDir;
    private final Path configDir;
    private final OutsideWorld outsideWorld;
    static final String[] DEFAULT_CLASSIFIERS = new String[]{"logs", "configs"};

    DiagnosticsReportCommand( Path homeDir, Path configDir, OutsideWorld outsideWorld )
    {
        this.homeDir = homeDir;
        this.configDir = configDir;
        this.outsideWorld = outsideWorld;
    }

    public static Arguments allArguments()
    {
        return arguments;
    }

    @Override
    public void execute( String[] stringArgs ) throws IncorrectUsage, CommandFailed
    {
        Args args = Args.withFlags( "list", "to" ).parse( stringArgs );
        FileSystemAbstraction fs = outsideWorld.fileSystem();
        PrintStream out = outsideWorld.outStream();

        // Make sure we can access the configuration file
        File configFile = configDir.resolve( Config.DEFAULT_CONFIG_FILE_NAME ).toFile();
        if ( !fs.fileExists( configFile ) )
        {
            throw new CommandFailed( "Unable to find config file, tried: " + configFile.getAbsolutePath() );
        }
        Config config = Config.fromFile( configFile ).withHome( homeDir ).withConnectorsDisabled().build();

        File storeDirectory = config.get( database_path );

        DiagnosticsReporter reporter = new DiagnosticsReporter( out );

        // Find all offline providers and register them
        for ( DiagnosticsOfflineReportProvider provider : Service.load( DiagnosticsOfflineReportProvider.class ) )
        {
            provider.init( fs, config, storeDirectory );
            reporter.registerOfflineProvider( provider );
        }

        // Register sources provided by this tool
        reporter.registerSource( "config",
                DiagnosticsReportSources.newDiagnosticsFile( "neo4j.conf", fs, configFile ) );

        // Online connection
        JmxDump jmxDump = connectToNeo4jInstance( fs, out );
        if ( jmxDump != null )
        {
            reporter.registerSource( "threads", jmxDump.threadDump() );
            reporter.registerSource( "heap", jmxDump.heapDump() );
            reporter.registerSource( "sysprop", jmxDump.systemProperties() );
            //reporter.registerSource( "env", null );
        }

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
        List<String> orphans = args.orphans();
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
            // Add default classifiers
            if ( orphans.size() == 0 )
            {
                orphans.addAll( Arrays.asList( DEFAULT_CLASSIFIERS ) );
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

        // TODO: we should really guesstimate the size of the dump, in part not to fill up the disk and die in the
        // TODO: middle, and part warn the user if it's larger than a given threshold

        // Start dumping
        Path destinationDir = new File( destinationArgument.parse( args ) ).toPath();
        try
        {
            SimpleDateFormat dumpFormat = new SimpleDateFormat( "yyyy-MM-dd_HHmmss" );
            Path reportFile = destinationDir.resolve( dumpFormat.format( new Date() ) + ".zip" );
            out.println( "Writing report to " + reportFile.toAbsolutePath().toString() );
            reporter.dump( new HashSet<>( orphans ), reportFile );
        }
        catch ( IOException e )
        {
            throw new CommandFailed( "Creating archive failed", e );
        }
    }

    private JmxDump connectToNeo4jInstance( FileSystemAbstraction fs, PrintStream out )
    {
        out.println( "Trying to find running instance of neo4j" );
        Path pidFile = homeDir.resolve( "run/neo4j.pid" );
        if ( fs.fileExists( pidFile.toFile() ) )
        {
            try ( BufferedReader reader = new BufferedReader( fs.openAsReader( pidFile.toFile(), Charset.defaultCharset() ) ) )
            {
                String pidFileContent = reader.readLine();
                try
                {
                    long pid = Long.parseLong( pidFileContent );

                    try
                    {
                        LocalVirtualMachine vm = LocalVirtualMachine.from( pid );
                        out.println( "Attached to running process with process id " + pid );
                        try
                        {
                            JmxDump jmxDump = JmxDump.connectTo( vm.getJmxAddress() );
                            jmxDump.attachSystemProperties( vm.getSystemProperties() );
                            out.println( "Connected to JMX endpoint" );
                            return jmxDump;
                        }
                        catch ( IOException e )
                        {
                            out.println( "Unable to communicate with JMX endpoint. Reason: " + e.getMessage() );
                        }
                    }
                    catch ( IOException e )
                    {
                        out.println( "Unable to connect to process. Reason: " + e.getMessage() );
                    }
                }
                catch ( NumberFormatException e )
                {
                    out.println( pidFile.toString() + " does not contain a valid id. Found: " + pidFileContent );
                }
            }
            catch ( IOException e )
            {
                out.println( "Error reading the .pid file. Reason: " + e.getMessage() );
            }
        }
        else
        {
            out.println( "No running instance of neo4j was found. Online reports will be omitted." );
        }

        return null;
    }

    private static String describeClassifier( String classifier )
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
        case "queries":
            return "include the output of dbms.listQueries()";
        default:
        }
        throw new IllegalArgumentException( "Unknown classifier:" + classifier );
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
