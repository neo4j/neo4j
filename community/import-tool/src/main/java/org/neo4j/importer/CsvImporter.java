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
package org.neo4j.importer;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.batchinsert.internal.TransactionLogsInitializer;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.common.Validator;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.Settings;
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
import org.neo4j.internal.batchimport.input.csv.Decorator;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.internal.batchimport.staging.ExecutionMonitors;
import org.neo4j.internal.batchimport.staging.SpectrumExecutionMonitor;
import org.neo4j.internal.helpers.Args;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.helpers.collection.IterableWrapper;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.os.OsBeanUtil;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.util.Converters;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.kernel.internal.Version;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.StoreLogService;
import org.neo4j.scheduler.JobScheduler;

import static java.lang.Math.toIntExact;
import static java.nio.charset.Charset.defaultCharset;
import static org.neo4j.configuration.GraphDatabaseSettings.store_internal_log_path;
import static org.neo4j.configuration.Settings.parseLongWithUnit;
import static org.neo4j.csv.reader.Configuration.COMMAS;
import static org.neo4j.importer.ImportCommand.OPT_ARRAY_DELIMITER;
import static org.neo4j.importer.ImportCommand.OPT_BAD_TOLERANCE;
import static org.neo4j.importer.ImportCommand.OPT_CACHE_ON_HEAP;
import static org.neo4j.importer.ImportCommand.OPT_DELIMITER;
import static org.neo4j.importer.ImportCommand.OPT_DETAILED_PROGRESS;
import static org.neo4j.importer.ImportCommand.OPT_HIGH_IO;
import static org.neo4j.importer.ImportCommand.OPT_ID_TYPE;
import static org.neo4j.importer.ImportCommand.OPT_IGNORE_EMPTY_STRINGS;
import static org.neo4j.importer.ImportCommand.OPT_IGNORE_EXTRA_COLUMNS;
import static org.neo4j.importer.ImportCommand.OPT_INPUT_ENCODING;
import static org.neo4j.importer.ImportCommand.OPT_LEGACY_STYLE_QUOTING;
import static org.neo4j.importer.ImportCommand.OPT_MAX_MEMORY;
import static org.neo4j.importer.ImportCommand.OPT_MULTILINE_FIELDS;
import static org.neo4j.importer.ImportCommand.OPT_NODES;
import static org.neo4j.importer.ImportCommand.OPT_NORMALIZE_TYPES;
import static org.neo4j.importer.ImportCommand.OPT_PROCESSORS;
import static org.neo4j.importer.ImportCommand.OPT_QUOTE;
import static org.neo4j.importer.ImportCommand.OPT_READ_BUFFER_SIZE;
import static org.neo4j.importer.ImportCommand.OPT_RELATIONSHIPS;
import static org.neo4j.importer.ImportCommand.OPT_REPORT_FILE;
import static org.neo4j.importer.ImportCommand.OPT_SKIP_BAD_ENTRIES_LOGGING;
import static org.neo4j.importer.ImportCommand.OPT_SKIP_BAD_RELATIONSHIPS;
import static org.neo4j.importer.ImportCommand.OPT_SKIP_DUPLICATE_NODES;
import static org.neo4j.importer.ImportCommand.OPT_TRIM_STRINGS;
import static org.neo4j.internal.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.internal.batchimport.Configuration.calculateMaxMemoryFromPercent;
import static org.neo4j.internal.batchimport.Configuration.canDetectFreeMemory;
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
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createScheduler;
import static org.neo4j.kernel.impl.util.Converters.withDefault;

class CsvImporter implements Importer
{
    /**
     * Delimiter used between files in an input group.
     */
    static final String MULTI_FILE_DELIMITER = ",";

    private static final Function<String, Character> CHARACTER_CONVERTER = new CharacterConverter();

    private final Collection<Args.Option<File[]>> nodesFiles;
    private final Collection<Args.Option<File[]>> relationshipsFiles;
    private final IdType idType;
    private final Charset inputEncoding;
    private final Config databaseConfig;
    private final Args args;
    private final OutsideWorld outsideWorld;
    private final DatabaseLayout databaseLayout;
    private final String reportFileName;
    private final boolean ignoreExtraColumns;
    private final boolean highIO;
    private final Boolean skipBadRelationships;
    private final boolean skipDuplicateNodes;
    private final boolean skipBadEntriesLogging;
    private final boolean detailedProgress;
    private final boolean cacheOnHeap;
    private final long badTolerance;
    private final Number maxMemory;
    private final Number numOfProcessors;
    private final boolean normalizeTypes;

    CsvImporter( Args args, Config databaseConfig, OutsideWorld outsideWorld, DatabaseLayout databaseLayout ) throws IncorrectUsage
    {
        this.args = args;
        this.outsideWorld = outsideWorld;
        this.databaseLayout = databaseLayout;
        nodesFiles = extractInputFiles( args, OPT_NODES, outsideWorld.errorStream() );
        relationshipsFiles = extractInputFiles( args, OPT_RELATIONSHIPS, outsideWorld.errorStream() );
        reportFileName = args.get( OPT_REPORT_FILE, ImportCommand.DEFAULT_REPORT_FILE_NAME );
        ignoreExtraColumns = args.getBoolean( OPT_IGNORE_EXTRA_COLUMNS );
        numOfProcessors = args.getNumber( OPT_PROCESSORS, Configuration.DEFAULT.maxNumberOfProcessors() );
        badTolerance = args.getNumber( OPT_BAD_TOLERANCE, 1000 ).longValue();
        maxMemory = parseMaxMemory( args.get( OPT_MAX_MEMORY, Configuration.DEFAULT_MAX_MEMORY_PERCENT + "%", null ) );
        skipBadRelationships = args.getBoolean( OPT_SKIP_BAD_RELATIONSHIPS );
        skipDuplicateNodes = args.getBoolean( OPT_SKIP_DUPLICATE_NODES );
        skipBadEntriesLogging = args.getBoolean( OPT_SKIP_BAD_ENTRIES_LOGGING );
        highIO = args.getBoolean( OPT_HIGH_IO, Configuration.DEFAULT.highIO() );
        detailedProgress = args.getBoolean( OPT_DETAILED_PROGRESS );
        cacheOnHeap = args.getBoolean( OPT_CACHE_ON_HEAP );

        try
        {
            validateInputFiles( nodesFiles, relationshipsFiles );
        }
        catch ( IllegalArgumentException e )
        {
            throw new IncorrectUsage( e.getMessage() );
        }

        idType = args.interpretOption( OPT_ID_TYPE, withDefault( IdType.STRING ),
                from -> IdType.valueOf( from.toUpperCase() ) );
        inputEncoding = Charset.forName( args.get( OPT_INPUT_ENCODING, defaultCharset().name() ) );
        normalizeTypes = args.getBoolean( OPT_NORMALIZE_TYPES, true );
        this.databaseConfig = databaseConfig;
    }

    @Override
    public void doImport() throws IOException
    {
        FileSystemAbstraction fs = outsideWorld.fileSystem();
        File reportFile = new File( reportFileName );

        OutputStream badOutput = new BufferedOutputStream( fs.openAsOutputStream( reportFile, false ) );
        try ( Collector badCollector = getBadCollector( skipBadEntriesLogging, badOutput ) )
        {
            Configuration configuration = importConfiguration(
                    numOfProcessors, maxMemory, databaseLayout, cacheOnHeap, highIO );

            // Extract the default time zone from the database configuration
            ZoneId dbTimeZone = databaseConfig.get( GraphDatabaseSettings.db_temporal_timezone );
            Supplier<ZoneId> defaultTimeZone = () -> dbTimeZone;

            CsvInput input = new CsvInput( nodeData( inputEncoding, nodesFiles ), defaultFormatNodeFileHeader( defaultTimeZone, normalizeTypes ),
                    relationshipData( inputEncoding, relationshipsFiles ), defaultFormatRelationshipFileHeader( defaultTimeZone, normalizeTypes ), idType,
                    csvConfiguration( args, false ),
                    new CsvInput.PrintingMonitor( outsideWorld.outStream() ) );

            doImport( outsideWorld.errorStream(), outsideWorld.errorStream(), outsideWorld.inStream(), databaseLayout, reportFile, fs,
                    nodesFiles, relationshipsFiles, false, input, this.databaseConfig, badCollector, configuration, detailedProgress );
        }
    }

    public static void doImport( PrintStream out, PrintStream err, InputStream in, DatabaseLayout databaseLayout, File badFile,
            FileSystemAbstraction fs, Collection<Args.Option<File[]>> nodesFiles,
            Collection<Args.Option<File[]>> relationshipsFiles, boolean enableStacktrace, Input input,
            Config dbConfig, Collector badCollector,
            org.neo4j.internal.batchimport.Configuration configuration, boolean detailedProgress ) throws IOException
    {
        boolean success;
        LifeSupport life = new LifeSupport();

        File internalLogFile = dbConfig.get( store_internal_log_path );
        LogService logService = life.add( StoreLogService.withInternalLog( internalLogFile ).build( fs ) );
        final JobScheduler jobScheduler = life.add( createScheduler() );

        life.start();
        ExecutionMonitor executionMonitor = detailedProgress
                                            ? new SpectrumExecutionMonitor( 2, TimeUnit.SECONDS, out, SpectrumExecutionMonitor.DEFAULT_WIDTH )
                                            : ExecutionMonitors.defaultVisible( in, jobScheduler );
        BatchImporter importer = BatchImporterFactory.withHighestPriority().instantiate( databaseLayout,
                fs,
                null, // no external page cache
                configuration,
                logService, executionMonitor,
                EMPTY,
                dbConfig,
                RecordFormatSelector.selectForConfig( dbConfig, logService.getInternalLogProvider() ),
                new PrintingImportLogicMonitor( out, err ), jobScheduler, badCollector, TransactionLogsInitializer.INSTANCE );
        printOverview( databaseLayout.databaseDirectory(), nodesFiles, relationshipsFiles, configuration, out );
        success = false;
        try
        {
            importer.doImport( input );
            success = true;
        }
        catch ( Exception e )
        {
            throw andPrintError( "Import error", e, enableStacktrace, err );
        }
        finally
        {
            long numberOfBadEntries = badCollector.badEntries();

            if ( badFile != null )
            {
                if ( numberOfBadEntries > 0 )
                {
                    System.out.println( "There were bad entries which were skipped and logged into " +
                            badFile.getAbsolutePath() );
                }
            }

            life.shutdown();

            if ( !success )
            {
                err.println( "WARNING Import failed. The store files in " + databaseLayout.databaseDirectory().getAbsolutePath() +
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
     * @param err
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
        else if ( Exceptions.contains( e, IllegalMultilineFieldException.class ) )
        {
            printErrorMessage( "Detected field which spanned multiple lines for an import where " +
                    OPT_MULTILINE_FIELDS + "=false. If you know that your input data " +
                    "include fields containing new-line characters then import with this option set to " +
                    "true.", e, stackTrace, err );
        }
        else if ( Exceptions.contains( e, InputException.class ) )
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

    private static void printOverview( File storeDir, Collection<Args.Option<File[]>> nodesFiles,
            Collection<Args.Option<File[]>> relationshipsFiles,
            org.neo4j.internal.batchimport.Configuration configuration, PrintStream out )
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

    private static void printInputFiles( String name, Collection<Args.Option<File[]>> files, PrintStream out )
    {
        if ( files.isEmpty() )
        {
            return;
        }

        out.println( name + ":" );
        int i = 0;
        for ( Args.Option<File[]> group : files )
        {
            if ( i++ > 0 )
            {
                out.println();
            }
            if ( group.metadata() != null )
            {
                printIndented( ":" + group.metadata(), out );
            }
            for ( File file : group.value() )
            {
                printIndented( file, out );
            }
        }
    }

    private static void printIndented( Object value, PrintStream out )
    {
        out.println( "  " + value );
    }

    private static Iterable<DataFactory>
    relationshipData( final Charset encoding, Collection<Args.Option<File[]>> relationshipsFiles )
    {
        return new IterableWrapper<>( relationshipsFiles )
        {
            @Override
            protected DataFactory underlyingObjectToObject( Args.Option<File[]> group )
            {
                return data( defaultRelationshipType( group.metadata() ), encoding, group.value() );
            }
        };
    }

    private static Iterable<DataFactory> nodeData( final Charset encoding,
            Collection<Args.Option<File[]>> nodesFiles )
    {
        return new IterableWrapper<>( nodesFiles )
        {
            @Override
            protected DataFactory underlyingObjectToObject( Args.Option<File[]> input )
            {
                Decorator decorator = input.metadata() != null
                                      ? additiveLabels( input.metadata().split( ":" ) )
                                      : NO_DECORATOR;
                return data( decorator, encoding, input.value() );
            }
        };
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

    private static org.neo4j.csv.reader.Configuration csvConfiguration( Args args, final boolean defaultSettingsSuitableForTests )
    {
        final var config = COMMAS.toBuilder();

        Optional.ofNullable( args.interpretOption( OPT_DELIMITER, Converters.optional(), CHARACTER_CONVERTER ) )
                .ifPresent( config::withDelimiter );

        Optional.ofNullable( args.interpretOption( OPT_ARRAY_DELIMITER, Converters.optional(), CHARACTER_CONVERTER ) )
                .ifPresent( config::withArrayDelimiter );

        Optional.ofNullable( args.interpretOption( OPT_QUOTE, Converters.optional(), CHARACTER_CONVERTER ) )
                .ifPresent( config::withQuotationCharacter );

        Optional.ofNullable( args.getBoolean( OPT_MULTILINE_FIELDS, null ) )
                .ifPresent( config::withMultilineFields );

        Optional.ofNullable( args.getBoolean( OPT_IGNORE_EMPTY_STRINGS, null ) )
                .ifPresent( config::withEmptyQuotedStringsAsNull );

        Optional.ofNullable( args.getBoolean( OPT_TRIM_STRINGS, null ) )
                .ifPresent( config::withTrimStrings );

        Optional.ofNullable( args.getBoolean( OPT_LEGACY_STYLE_QUOTING, null ) )
                .ifPresent( config::withLegacyStyleQuoting );

        Optional.ofNullable( args.get( OPT_READ_BUFFER_SIZE, null ) )
                .map( s -> toIntExact( parseLongWithUnit( s ) ) )
                .ifPresentOrElse( config::withBufferSize, () ->
                {
                    if ( defaultSettingsSuitableForTests )
                    {
                        config.withBufferSize( 10_000 );
                    }
                } );

        return config.build();
    }

    private static org.neo4j.internal.batchimport.Configuration importConfiguration(
            Number processors, Number maxMemory, DatabaseLayout databaseLayout,
            boolean allowCacheOnHeap, Boolean defaultHighIO )
    {
        return new org.neo4j.internal.batchimport.Configuration()
        {
            @Override
            public int maxNumberOfProcessors()
            {
                return processors != null ? processors.intValue() : DEFAULT.maxNumberOfProcessors();
            }

            @Override
            public long maxMemoryUsage()
            {
                return maxMemory != null ? maxMemory.longValue() : DEFAULT.maxMemoryUsage();
            }

            @Override
            public boolean highIO()
            {
                return defaultHighIO != null ? defaultHighIO : FileUtils.highIODevice( databaseLayout.databaseDirectory().toPath(), false );
            }

            @Override
            public boolean allowCacheAllocationOnHeap()
            {
                return allowCacheOnHeap;
            }
        };
    }

    private static Long parseMaxMemory( String maxMemoryString )
    {
        if ( maxMemoryString != null )
        {
            maxMemoryString = maxMemoryString.trim();
            if ( maxMemoryString.endsWith( "%" ) )
            {
                int percent = Integer.parseInt( maxMemoryString.substring( 0, maxMemoryString.length() - 1 ) );
                long result = calculateMaxMemoryFromPercent( percent );
                if ( !canDetectFreeMemory() )
                {
                    System.err.println( "WARNING: amount of free memory couldn't be detected so defaults to " +
                            bytesToString( result ) + ". For optimal performance instead explicitly specify amount of " +
                            "memory that importer is allowed to use using " + OPT_MAX_MEMORY );
                }
                return result;
            }
            return Settings.parseLongWithUnit( maxMemoryString );
        }
        return null;
    }

    private static Collection<Args.Option<File[]>> extractInputFiles( Args args, String key, PrintStream err )
    {
        return args
                .interpretOptionsWithMetadata( key, Converters.optional(),
                        Converters.toFiles( MULTI_FILE_DELIMITER, Converters.regexFiles( true ) ),
                        filesExist( err ),
                        Validators.atLeast( "--" + key, 1 ) );
    }

    private static Validator<File[]> filesExist( PrintStream err )
    {
        return files ->
        {
            for ( File file : files )
            {
                if ( file.getName().startsWith( ":" ) )
                {
                    err.println( "It looks like you're trying to specify default label or relationship type (" +
                            file.getName() + "). Please put such directly on the key, f.ex. " +
                            OPT_NODES + ":MyLabel" );
                }
                Validators.REGEX_FILE_EXISTS.validate( file );
            }
        };
    }

    private static void validateInputFiles( Collection<Args.Option<File[]>> nodesFiles,
            Collection<Args.Option<File[]>> relationshipsFiles )
    {
        if ( nodesFiles.isEmpty() )
        {
            if ( relationshipsFiles.isEmpty() )
            {
                throw new IllegalArgumentException( "No input specified, nothing to import" );
            }
            throw new IllegalArgumentException( "No node input specified, cannot import relationships without nodes" );
        }
    }
}
