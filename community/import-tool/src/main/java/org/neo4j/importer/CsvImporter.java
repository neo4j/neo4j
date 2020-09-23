/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.importer;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.neo4j.commandline.Util;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.csv.reader.IllegalMultilineFieldException;
import org.neo4j.internal.batchimport.BatchImporter;
import org.neo4j.internal.batchimport.BatchImporterFactory;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.cache.idmapping.string.DuplicateInputIdException;
import org.neo4j.internal.batchimport.input.BadCollector;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.IdType;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.batchimport.input.InputException;
import org.neo4j.internal.batchimport.input.MissingRelationshipDataException;
import org.neo4j.internal.batchimport.input.csv.CsvInput;
import org.neo4j.internal.batchimport.input.csv.DataFactory;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.internal.batchimport.staging.ExecutionMonitors;
import org.neo4j.internal.batchimport.staging.SpectrumExecutionMonitor;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileSystemUtils;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.os.OsBeanUtil;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogInitializer;
import org.neo4j.kernel.internal.Version;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.exception.ExceptionUtils.indexOfThrowable;
import static org.neo4j.configuration.GraphDatabaseSettings.store_internal_log_path;
import static org.neo4j.internal.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.internal.batchimport.input.Collectors.badCollector;
import static org.neo4j.internal.batchimport.input.Collectors.collect;
import static org.neo4j.internal.batchimport.input.Collectors.silentBadCollector;
import static org.neo4j.internal.batchimport.input.InputEntityDecorators.NO_DECORATOR;
import static org.neo4j.internal.batchimport.input.InputEntityDecorators.additiveLabels;
import static org.neo4j.internal.batchimport.input.InputEntityDecorators.defaultRelationshipType;
import static org.neo4j.internal.batchimport.input.csv.DataFactories.data;
import static org.neo4j.internal.batchimport.input.csv.DataFactories.defaultFormatNodeFileHeader;
import static org.neo4j.internal.batchimport.input.csv.DataFactories.defaultFormatRelationshipFileHeader;
import static org.neo4j.internal.helpers.Exceptions.throwIfUnchecked;
import static org.neo4j.io.ByteUnit.bytesToString;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;

class CsvImporter implements Importer
{
    static final String DEFAULT_REPORT_FILE_NAME = "import.report";

    private final DatabaseLayout databaseLayout;
    private final Config databaseConfig;
    private final org.neo4j.csv.reader.Configuration csvConfig;
    private final org.neo4j.internal.batchimport.Configuration importConfig;
    private final Path reportFile;
    private final IdType idType;
    private final Charset inputEncoding;
    private final boolean ignoreExtraColumns;
    private final boolean skipBadRelationships;
    private final boolean skipDuplicateNodes;
    private final boolean skipBadEntriesLogging;
    private final long badTolerance;
    private final boolean normalizeTypes;
    private final boolean verbose;
    private final Map<Set<String>, List<Path[]>> nodeFiles;
    private final Map<String, List<Path[]>> relationshipFiles;
    private final FileSystemAbstraction fileSystem;
    private final PrintStream stdOut;
    private final PrintStream stdErr;
    private final PageCacheTracer pageCacheTracer;
    private final MemoryTracker memoryTracker;

    private CsvImporter( Builder b )
    {
        this.databaseLayout = requireNonNull( b.databaseLayout );
        this.databaseConfig = requireNonNull( b.databaseConfig );
        this.csvConfig = requireNonNull( b.csvConfig );
        this.importConfig = requireNonNull( b.importConfig );
        this.reportFile = requireNonNull( b.reportFile );
        this.idType = requireNonNull( b.idType );
        this.inputEncoding = requireNonNull( b.inputEncoding );
        this.ignoreExtraColumns = b.ignoreExtraColumns;
        this.skipBadRelationships = b.skipBadRelationships;
        this.skipDuplicateNodes = b.skipDuplicateNodes;
        this.skipBadEntriesLogging = b.skipBadEntriesLogging;
        this.badTolerance = b.badTolerance;
        this.normalizeTypes = b.normalizeTypes;
        this.verbose = b.verbose;
        this.nodeFiles = requireNonNull( b.nodeFiles );
        this.relationshipFiles = requireNonNull( b.relationshipFiles );
        this.fileSystem = requireNonNull( b.fileSystem );
        this.pageCacheTracer = requireNonNull( b.pageCacheTracer );
        this.memoryTracker = requireNonNull( b.memoryTracker );
        this.stdOut = requireNonNull( b.stdOut );
        this.stdErr = requireNonNull( b.stdErr );
    }

    @Override
    public void doImport() throws IOException
    {
        try ( OutputStream badOutput = fileSystem.openAsOutputStream( reportFile, false );
                Collector badCollector = getBadCollector( skipBadEntriesLogging, badOutput ) )
        {
            // Extract the default time zone from the database configuration
            ZoneId dbTimeZone = databaseConfig.get( GraphDatabaseSettings.db_temporal_timezone );
            Supplier<ZoneId> defaultTimeZone = () -> dbTimeZone;

            final var nodeData = nodeData();
            final var relationshipsData = relationshipData();

            CsvInput input = new CsvInput( nodeData, defaultFormatNodeFileHeader( defaultTimeZone, normalizeTypes ),
                relationshipsData, defaultFormatRelationshipFileHeader( defaultTimeZone, normalizeTypes ), idType,
                csvConfig, new CsvInput.PrintingMonitor( stdOut ), memoryTracker );

            doImport( input, badCollector );
        }
    }

    private void doImport( Input input, Collector badCollector )
    {
        boolean success = false;

        Path internalLogFile = databaseConfig.get( store_internal_log_path );
        try ( JobScheduler jobScheduler = createInitialisedScheduler();
              OutputStream outputStream = FileSystemUtils.createOrOpenAsOutputStream( fileSystem, internalLogFile, true );
              Log4jLogProvider logProvider = Util.configuredLogProvider( databaseConfig, outputStream ) )
        {
            ExecutionMonitor executionMonitor = verbose ? new SpectrumExecutionMonitor( 2, TimeUnit.SECONDS, stdOut,
                    SpectrumExecutionMonitor.DEFAULT_WIDTH ) : ExecutionMonitors.defaultVisible();

            BatchImporter importer = BatchImporterFactory.withHighestPriority().instantiate(
                    databaseLayout,
                    fileSystem,
                    null, // no external page cache
                    pageCacheTracer,
                    importConfig,
                    new SimpleLogService( NullLogProvider.getInstance(), logProvider ),
                    executionMonitor,
                    EMPTY,
                    databaseConfig,
                    RecordFormatSelector.selectForConfig( databaseConfig, logProvider ),
                    new PrintingImportLogicMonitor( stdOut, stdErr ),
                    jobScheduler,
                    badCollector,
                    TransactionLogInitializer.getLogFilesInitializer(),
                    memoryTracker );

            printOverview( databaseLayout.databaseDirectory(), nodeFiles, relationshipFiles, importConfig, stdOut );

            importer.doImport( input );

            success = true;
        }
        catch ( Exception e )
        {
            throw andPrintError( "Import error", e, verbose, stdErr );
        }
        finally
        {
            long numberOfBadEntries = badCollector.badEntries();

            if ( reportFile != null )
            {
                if ( numberOfBadEntries > 0 )
                {
                    stdOut.println( "There were bad entries which were skipped and logged into " + reportFile.toAbsolutePath() );
                }
            }

            if ( !success )
            {
                stdErr.println( "WARNING Import failed. The store files in " + databaseLayout.databaseDirectory().toAbsolutePath() +
                        " are left as they are, although they are likely in an unusable state. " +
                        "Starting a database on these store files will likely fail or observe inconsistent records so " +
                        "start at your own risk or delete the store manually" );
            }
        }
    }

    /**
     * Method name looks strange, but look at how it's used and you'll see why it's named like that.
     *
     * @param stackTrace whether or not to also print the stack trace of the error.
     */
    private static RuntimeException andPrintError( String typeOfError, Exception e, boolean stackTrace,
            PrintStream err )
    {
        // List of common errors that can be explained to the user
        if ( DuplicateInputIdException.class.equals( e.getClass() ) )
        {
            printErrorMessage( "Duplicate input ids that would otherwise clash can be put into separate id space.", e, stackTrace, err );
        }
        else if ( MissingRelationshipDataException.class.equals( e.getClass() ) )
        {
            printErrorMessage( "Relationship missing mandatory field", e, stackTrace, err );
        }
        // This type of exception is wrapped since our input code throws InputException consistently,
        // and so IllegalMultilineFieldException comes from the csv component, which has no access to InputException
        // therefore it's wrapped.
        else if ( indexOfThrowable( e, IllegalMultilineFieldException.class ) != -1 )
        {
            printErrorMessage( "Detected field which spanned multiple lines for an import where " +
                    "--multiline-fields=false. If you know that your input data " +
                    "include fields containing new-line characters then import with this option set to " +
                    "true.", e, stackTrace, err );
        }
        else if ( indexOfThrowable( e, InputException.class ) != -1 )
        {
            printErrorMessage( "Error in input data", e, stackTrace, err );
        }
        // Fallback to printing generic error and stack trace
        else
        {
            printErrorMessage( typeOfError + ": " + e.getMessage(), e, true, err );
        }
        err.println();

        // Mute the stack trace that the default exception handler would have liked to print.
        // Calling System.exit( 1 ) or similar would be convenient on one hand since we can set
        // a specific exit code. On the other hand It's very inconvenient to have any System.exit
        // call in code that is tested.
        Thread.currentThread().setUncaughtExceptionHandler( ( t, e1 ) ->
        {
            /* Shhhh */
        } );
        throwIfUnchecked( e );
        return new RuntimeException( e ); // throw in order to have process exit with !0
    }

    private static void printErrorMessage( String string, Exception e, boolean stackTrace, PrintStream err )
    {
        err.println( string );
        err.println( "Caused by:" + e.getMessage() );
        if ( stackTrace )
        {
            e.printStackTrace( err );
        }
    }

    private static void printOverview( Path storeDir, Map<Set<String>, List<Path[]>> nodesFiles, Map<String, List<Path[]>> relationshipsFiles,
        Configuration configuration, PrintStream out )
    {
        out.println( "Neo4j version: " + Version.getNeo4jVersion() );
        out.println( "Importing the contents of these files into " + storeDir + ":" );
        printInputFiles( "Nodes", nodesFiles, out );
        printInputFiles( "Relationships", relationshipsFiles, out );
        out.println();
        out.println( "Available resources:" );
        printIndented( "Total machine memory: " + bytesToString( OsBeanUtil.getTotalPhysicalMemory() ), out );
        printIndented( "Free machine memory: " + bytesToString( OsBeanUtil.getFreePhysicalMemory() ), out );
        printIndented( "Max heap memory : " + bytesToString( Runtime.getRuntime().maxMemory() ), out );
        printIndented( "Processors: " + configuration.maxNumberOfProcessors(), out );
        printIndented( "Configured max memory: " + bytesToString( configuration.maxMemoryUsage() ), out );
        printIndented( "High-IO: " + configuration.highIO(), out );
        out.println();
    }

    private static void printInputFiles( String name, Map<?, List<Path[]>> inputFiles, PrintStream out )
    {
        if ( inputFiles.isEmpty() )
        {
            return;
        }

        out.println( name + ":" );

        inputFiles.forEach( ( k, files ) ->
        {
            if ( !isEmptyKey( k ) )
            {
                printIndented( k + ":", out );
            }

            for ( Path[] arr : files )
            {
                for ( final Path file : arr )
                {
                    printIndented( file, out );
                }
            }
            out.println();
        } );
    }

    private static boolean isEmptyKey( Object k )
    {
        if ( k instanceof String )
        {
            return ((String) k).isEmpty();
        }
        else if ( k instanceof Set )
        {
            return ((Set) k).isEmpty();
        }
        return false;
    }

    private static void printIndented( Object value, PrintStream out )
    {
        out.println( "  " + value );
    }

    private Iterable<DataFactory> relationshipData()
    {
        final var result = new ArrayList<DataFactory>();
        relationshipFiles.forEach( ( defaultTypeName, fileSets ) ->
        {
            final var decorator = defaultRelationshipType( defaultTypeName );
            for ( Path[] files : fileSets )
            {
                final var data = data( decorator, inputEncoding, files );
                result.add( data );
            }
        } );
        return result;
    }

    private Iterable<DataFactory> nodeData()
    {
        final var result = new ArrayList<DataFactory>();
        nodeFiles.forEach( ( labels, fileSets ) ->
        {
            final var decorator = labels.isEmpty() ? NO_DECORATOR : additiveLabels( labels.toArray( new String[0] ) );
            for ( Path[] files : fileSets )
            {
                final var data = data( decorator, inputEncoding, files );
                result.add( data );
            }
        } );
        return result;
    }

    private Collector getBadCollector( boolean skipBadEntriesLogging, OutputStream badOutput )
    {
        return skipBadEntriesLogging ? silentBadCollector( badTolerance ) :
               badCollector( badOutput, isIgnoringSomething() ? BadCollector.UNLIMITED_TOLERANCE : 0,
                       collect( skipBadRelationships, skipDuplicateNodes, ignoreExtraColumns ) );
    }

    private boolean isIgnoringSomething()
    {
        return skipBadRelationships || skipDuplicateNodes || ignoreExtraColumns;
    }

    static Builder builder()
    {
        return new Builder();
    }

    static class Builder
    {
        private DatabaseLayout databaseLayout;
        private Config databaseConfig;
        private org.neo4j.csv.reader.Configuration csvConfig = org.neo4j.csv.reader.Configuration.COMMAS;
        private Configuration importConfig = Configuration.DEFAULT;
        private Path reportFile;
        private IdType idType = IdType.STRING;
        private Charset inputEncoding = StandardCharsets.UTF_8;
        private boolean ignoreExtraColumns;
        private boolean skipBadRelationships;
        private boolean skipDuplicateNodes;
        private boolean skipBadEntriesLogging;
        private long badTolerance;
        private boolean normalizeTypes;
        private boolean verbose;
        private final Map<Set<String>, List<Path[]>> nodeFiles = new HashMap<>();
        private final Map<String, List<Path[]>> relationshipFiles = new HashMap<>();
        private FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
        private PageCacheTracer pageCacheTracer = PageCacheTracer.NULL;
        private MemoryTracker memoryTracker = EmptyMemoryTracker.INSTANCE;
        private PrintStream stdOut = System.out;
        private PrintStream stdErr = System.err;

        Builder withDatabaseLayout( DatabaseLayout databaseLayout )
        {
            this.databaseLayout = databaseLayout;
            return this;
        }

        Builder withDatabaseConfig( Config databaseConfig )
        {
            this.databaseConfig = databaseConfig;
            return this;
        }

        Builder withCsvConfig( org.neo4j.csv.reader.Configuration csvConfig )
        {
            this.csvConfig = csvConfig;
            return this;
        }

        Builder withImportConfig( Configuration importConfig )
        {
            this.importConfig = importConfig;
            return this;
        }

        Builder withReportFile( Path reportFile )
        {
            this.reportFile = reportFile;
            return this;
        }

        Builder withIdType( IdType idType )
        {
            this.idType = idType;
            return this;
        }

        Builder withInputEncoding( Charset inputEncoding )
        {
            this.inputEncoding = inputEncoding;
            return this;
        }

        Builder withIgnoreExtraColumns( boolean ignoreExtraColumns )
        {
            this.ignoreExtraColumns = ignoreExtraColumns;
            return this;
        }

        Builder withSkipBadRelationships( boolean skipBadRelationships )
        {
            this.skipBadRelationships = skipBadRelationships;
            return this;
        }

        Builder withSkipDuplicateNodes( boolean skipDuplicateNodes )
        {
            this.skipDuplicateNodes = skipDuplicateNodes;
            return this;
        }

        Builder withSkipBadEntriesLogging( boolean skipBadEntriesLogging )
        {
            this.skipBadEntriesLogging = skipBadEntriesLogging;
            return this;
        }

        Builder withBadTolerance( long badTolerance )
        {
            this.badTolerance = badTolerance;
            return this;
        }

        Builder withNormalizeTypes( boolean normalizeTypes )
        {
            this.normalizeTypes = normalizeTypes;
            return this;
        }

        Builder withVerbose( boolean verbose )
        {
            this.verbose = verbose;
            return this;
        }

        Builder addNodeFiles( Set<String> labels, Path[] files )
        {
            final var list = nodeFiles.computeIfAbsent( labels, unused -> new ArrayList<>() );
            list.add( files );
            return this;
        }

        Builder addRelationshipFiles( String defaultRelType, Path[] files )
        {
            final var list = relationshipFiles.computeIfAbsent( defaultRelType, unused -> new ArrayList<>() );
            list.add( files );
            return this;
        }

        Builder withFileSystem( FileSystemAbstraction fileSystem )
        {
            this.fileSystem = fileSystem;
            return this;
        }

        Builder withPageCacheTracer( PageCacheTracer pageCacheTracer )
        {
            this.pageCacheTracer = pageCacheTracer;
            return this;
        }

        Builder withMemoryTracker( MemoryTracker memoryTracker )
        {
            this.memoryTracker = memoryTracker;
            return this;
        }

        Builder withStdOut( PrintStream stdOut )
        {
            this.stdOut = stdOut;
            return this;
        }

        Builder withStdErr( PrintStream stdErr )
        {
            this.stdErr = stdErr;
            return this;
        }

        CsvImporter build()
        {
            return new CsvImporter( this );
        }
    }
}
