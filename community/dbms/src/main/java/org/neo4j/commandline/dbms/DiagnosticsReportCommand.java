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
package org.neo4j.commandline.dbms;

import static java.lang.String.join;
import static org.apache.commons.text.StringEscapeUtils.escapeCsv;
import static picocli.CommandLine.Command;
import static picocli.CommandLine.Help.Visibility.NEVER;
import static picocli.CommandLine.Option;
import static picocli.CommandLine.Parameters;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import org.eclipse.collections.impl.factory.Sets;
import org.jutils.jprocesses.JProcesses;
import org.jutils.jprocesses.model.ProcessInfo;
import org.neo4j.cli.AbstractCommand;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.Converters;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.ConfigUtils;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.DatabaseNamePattern;
import org.neo4j.dbms.diagnostics.jmx.JMXDumper;
import org.neo4j.dbms.diagnostics.jmx.JmxDump;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.diagnostics.DiagnosticsReportSource;
import org.neo4j.kernel.diagnostics.DiagnosticsReportSources;
import org.neo4j.kernel.diagnostics.DiagnosticsReporter;
import org.neo4j.kernel.diagnostics.DiagnosticsReporterProgress;
import org.neo4j.kernel.diagnostics.InteractiveProgress;
import org.neo4j.kernel.diagnostics.NonInteractiveProgress;

@Command(
        name = "report",
        header = "Produces a zip/tar of the most common information needed for remote assessments.",
        description = "Will collect information about the system and package everything in an archive. If you specify 'all', " +
                "everything will be included. You can also fine tune the selection by passing classifiers to the tool, " +
                "e.g 'logs tx threads'."
)
public class DiagnosticsReportCommand extends AbstractCommand
{
    static final String[] DEFAULT_CLASSIFIERS = {"logs", "config", "plugins", "tree", "metrics", "threads", "sysprop", "ps", "version"};
    private static final DateTimeFormatter filenameDateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HHmmss" );
    private static final long NO_PID = 0;

    @Option(
            names = "--database",
            paramLabel = "<database>",
            defaultValue = "*",
            description = "Name of the database to report for. Can contain * and ? for globbing. "
                    + "Note that * and ? have special meaning in some shells "
                    + "and might need to be escaped or used with quotes.",
            converter = Converters.DatabaseNamePatternConverter.class )
    private DatabaseNamePattern database;

    @Option( names = "--list", arity = "0", description = "List all available classifiers" )
    private boolean list;

    @Option( names = "--force", arity = "0", description = "Ignore disk full warning" )
    private boolean force;

    @Option( names = "--to", paramLabel = "<path>", description = "Destination directory for reports. Defaults to a system tmp directory." )
    private Path reportDir;

    @Option( names = "--pid", description = "Specify process id of running neo4j instance", showDefaultValue = NEVER )
    private long pid;

    @Parameters( arity = "0..*", paramLabel = "<classifier>" )
    private Set<String> classifiers = new TreeSet<>( List.of( DEFAULT_CLASSIFIERS ) );

    private JMXDumper jmxDumper;

    public DiagnosticsReportCommand( ExecutionContext ctx )
    {
        super( ctx );
    }

    @Override
    public void execute()
    {
        Config config = getConfig( configFile() );

        jmxDumper = new JMXDumper( config, ctx.fs(), ctx.out(), ctx.err(), verbose );
        Set<String> dbNames = getDbNames(config, ctx.fs(), database);
        DiagnosticsReporter reporter = createAndRegisterSources(config, dbNames);

        if ( list )
        {
            listClassifiers( reporter.getAvailableClassifiers() );
            return;
        }

        validateClassifiers( reporter );

        DiagnosticsReporterProgress progress = buildProgress();

        // Start dumping
        try
        {
            if ( reportDir == null )
            {
                 reportDir = Path.of( System.getProperty( "java.io.tmpdir" ) ).resolve( "reports" ).toAbsolutePath();
            }
            Path reportFile = reportDir.resolve( getDefaultFilename() );
            ctx.out().println( "Writing report to " + reportFile.toAbsolutePath() );
            reporter.dump( classifiers, reportFile, progress, force );
        }
        catch ( IOException e )
        {
            throw new CommandFailedException( "Creating archive failed", e );
        }
    }

    private static String getDefaultFilename() throws UnknownHostException
    {
        String hostName = InetAddress.getLocalHost().getHostName();
        String safeFilename = hostName.replaceAll( "[^a-zA-Z0-9._]+", "_" );
        return safeFilename + "-" + LocalDateTime.now().format( filenameDateTimeFormatter ) + ".zip";
    }

    private DiagnosticsReporterProgress buildProgress()
    {
        return System.console() == null ? new NonInteractiveProgress( ctx.out(), verbose ) : new InteractiveProgress( ctx.out(), verbose );
    }

    private void validateClassifiers( DiagnosticsReporter reporter )
    {
        Set<String> availableClassifiers = reporter.getAvailableClassifiers();
        if ( classifiers.contains( "all" ) )
        {
            if ( classifiers.size() != 1 )
            {
                classifiers.remove( "all" );
                throw new CommandFailedException( "If you specify 'all' this has to be the only classifier. Found ['" +
                        join( "','", classifiers ) + "'] as well." );
            }
        }
        else
        {
            if ( classifiers.equals( Set.of( DEFAULT_CLASSIFIERS ) ) )
            {
                classifiers = new HashSet<>( classifiers );
                classifiers.retainAll( availableClassifiers );
            }
            validateOrphanClassifiers( availableClassifiers, classifiers );
        }
    }

    private static void validateOrphanClassifiers( Set<String> availableClassifiers, Set<String> orphans )
    {
        for ( String classifier : orphans )
        {
            if ( !availableClassifiers.contains( classifier ) )
            {
                throw new CommandFailedException( "Unknown classifier: " + classifier );
            }
        }
    }

    private void listClassifiers( Set<String> availableClassifiers )
    {
        ctx.out().println( "All available classifiers:" );
        for ( String classifier : availableClassifiers )
        {
            ctx.out().printf( "  %-10s %s%n", classifier, describeClassifier( classifier ) );
        }
    }

    private DiagnosticsReporter createAndRegisterSources( Config config, Set<String> databaseNames )
    {
        DiagnosticsReporter reporter = new DiagnosticsReporter();

        reporter.registerAllOfflineProviders( config, ctx.fs(), databaseNames );

        // Register sources provided by this tool
        reporter.registerSource( "config",
                DiagnosticsReportSources.newDiagnosticsFile( "neo4j.conf", ctx.fs(), configFile() ) );

        reporter.registerSource( "ps", runningProcesses() );

        // Online connection
        registerJMXSources( reporter );
        return reporter;
    }

    private Path configFile()
    {
        return ctx.confDir().resolve( Config.DEFAULT_CONFIG_FILE_NAME );
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
        } );
    }

    private Config getConfig( Path configFile )
    {
        if ( !ctx.fs().fileExists( configFile ) )
        {
            throw new CommandFailedException( "Unable to find config file, tried: " + configFile.toAbsolutePath() );
        }
        Config cfg = Config.newBuilder()
                           .fromFileNoThrow( configFile )
                           .set( GraphDatabaseSettings.neo4j_home, ctx.homeDir() )
                           .commandExpansion( allowCommandExpansion )
                           .build();
        ConfigUtils.disableAllConnectors( cfg );
        return cfg;
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
        case "sysprop":
            return "include a list of java system properties";
        case "raft":
            return "include the raft log";
        case "ccstate":
            return "include the current cluster state";
        case "ps":
            return "include a list of running processes";
        case "version":
            return "include version of neo4j";
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
                        .append( processInfo.getPriority() ).append( ',' )
                        .append( escapeCsv( processInfo.getCommand() ) ).append( '\n' );
            }
            return sb.toString();
        } );
    }

    private Set<String> getDbNames( Config config, FileSystemAbstraction fs, DatabaseNamePattern database )
    {
        if ( !database.containsPattern() )
        {
            return Set.of( database.getDatabaseName() );
        }
        else
        {
            Set<String> dbNames = Sets.mutable.empty();
            Path databasesDir = Neo4jLayout.of( config ).databasesDirectory();
            try
            {
                for ( Path path : fs.listFiles( databasesDir ) )
                {
                    if ( fs.isDirectory( path ) )
                    {
                        String name = path.getFileName().toString();
                        if ( database.matches( name ) )
                        {
                            dbNames.add( name );
                        }
                    }
                }
            }
            catch ( IOException e )
            {
                throw new CommandFailedException( "Failed to list databases", e );
            }
            if ( dbNames.isEmpty() )
            {
                throw new CommandFailedException(
                        "Pattern '" + database.getDatabaseName() + "' did not match any database" );
            }
            return dbNames;
        }
    }
}
