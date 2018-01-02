/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.tools.dump;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.TimeZone;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.neo4j.helpers.Args;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.command.Command.PropertyCommand;
import org.neo4j.kernel.impl.transaction.command.Command.RelationshipCommand;
import org.neo4j.kernel.impl.transaction.command.Command.RelationshipGroupCommand;
import org.neo4j.kernel.impl.transaction.command.Command.SchemaRuleCommand;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.CheckPoint;
import org.neo4j.kernel.impl.transaction.log.entry.InvalidLogEntryHandler;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.tools.dump.TransactionLogAnalyzer.Monitor;
import org.neo4j.tools.dump.inconsistency.ReportInconsistencies;

import static java.util.TimeZone.getTimeZone;
import static org.neo4j.helpers.Format.DEFAULT_TIME_ZONE;

/**
 * Tool to represent logical logs in readable format for further analysis.
 */
public class DumpLogicalLog
{
    private static final String TO_FILE = "tofile";
    private static final String TX_FILTER = "txfilter";
    private static final String CC_FILTER = "ccfilter";
    private static final String LENIENT = "lenient";

    private final FileSystemAbstraction fileSystem;

    public DumpLogicalLog( FileSystemAbstraction fileSystem )
    {
        this.fileSystem = fileSystem;
    }

    public void dump( String filenameOrDirectory, PrintStream out,
            Predicate<LogEntry[]> filter, Function<LogEntry,String> serializer,
            InvalidLogEntryHandler invalidLogEntryHandler ) throws IOException
    {
        TransactionLogAnalyzer.analyze( fileSystem, new File( filenameOrDirectory ), invalidLogEntryHandler, new Monitor()
        {
            @Override
            public void logFile( File file, long logVersion ) throws IOException
            {
                LogHeader logHeader = LogHeaderReader.readLogHeader( fileSystem, file );
                out.println( "=== " + file.getAbsolutePath() + "[" + logHeader + "] ===" );
            }

            @Override
            public void transaction( LogEntry[] transactionEntries )
            {
                if ( filter == null || filter.test( transactionEntries ) )
                {
                    for ( LogEntry entry : transactionEntries )
                    {
                        out.println( serializer.apply( entry ) );
                    }
                }
            }

            @Override
            public void checkpoint( CheckPoint checkpoint, LogPosition checkpointEntryPosition )
            {
                if ( filter == null || filter.test( new LogEntry[] {checkpoint} ) )
                {
                    out.println( serializer.apply( checkpoint ) );
                }
            }
        } );
    }

    private static class TransactionRegexCriteria implements Predicate<LogEntry[]>
    {
        private final Pattern pattern;
        private final TimeZone timeZone;

        TransactionRegexCriteria( String regex, TimeZone timeZone )
        {
            this.pattern = Pattern.compile( regex );
            this.timeZone = timeZone;
        }

        @Override
        public boolean test( LogEntry[] transaction )
        {
            for ( LogEntry entry : transaction )
            {
                if ( pattern.matcher( entry.toString( timeZone ) ).find() )
                {
                    return true;
                }
            }
            return false;
        }
    }

    public static class ConsistencyCheckOutputCriteria implements Predicate<LogEntry[]>, Function<LogEntry,String>
    {
        private final TimeZone timeZone;
        private final ReportInconsistencies inconsistencies;

        public ConsistencyCheckOutputCriteria( String ccFile, TimeZone timeZone ) throws IOException
        {
            this.timeZone = timeZone;
            inconsistencies = new ReportInconsistencies();
            new InconsistencyReportReader( inconsistencies ).read( new File( ccFile ) );
        }

        @Override
        public boolean test( LogEntry[] transaction )
        {
            for ( LogEntry logEntry : transaction )
            {
                if ( matches( logEntry ) )
                {
                    return true;
                }
            }
            return false;
        }

        private boolean matches( LogEntry logEntry )
        {
            if ( logEntry instanceof LogEntryCommand )
            {
                if ( matches( ((LogEntryCommand)logEntry).getCommand() ) )
                {
                    return true;
                }
            }
            return false;
        }

        private boolean matches( StorageCommand command )
        {
            if ( command instanceof NodeCommand )
            {
                return inconsistencies.containsNodeId( ((NodeCommand) command).getKey() );
            }
            if ( command instanceof RelationshipCommand )
            {
                return inconsistencies.containsRelationshipId( ((RelationshipCommand) command).getKey() );
            }
            if ( command instanceof PropertyCommand )
            {
                return inconsistencies.containsPropertyId( ((PropertyCommand) command).getKey() );
            }
            if ( command instanceof RelationshipGroupCommand )
            {
                return inconsistencies.containsRelationshipGroupId( ((RelationshipGroupCommand) command).getKey() );
            }
            if ( command instanceof SchemaRuleCommand )
            {
                return inconsistencies.containsSchemaIndexId( ((SchemaRuleCommand) command).getKey() );
            }
            return false;
        }

        @Override
        public String apply( LogEntry logEntry )
        {
            String result = logEntry.toString( timeZone );
            if ( matches( logEntry ) )
            {
                result += "  <----";
            }
            return result;
        }
    }

    /**
     * Usage: [--txfilter "regex"] [--ccfilter cc-report-file] [--tofile] [--lenient] storeDirOrFile1 storeDirOrFile2 ...
     *
     * --txfilter
     * Will match regex against each {@link LogEntry} and if there is a match,
     * include transaction containing the LogEntry in the dump.
     * regex matching is done with {@link Pattern}
     *
     * --ccfilter
     * Will look at an inconsistency report file from consistency checker and
     * include transactions that are relevant to them
     *
     * --tofile
     * Redirects output to dump-logical-log.txt in the store directory
     *
     * --lenient
     * Will attempt to read log entries even if some look broken along the way
     */
    public static void main( String[] args ) throws IOException
    {
        Args arguments = Args.withFlags( TO_FILE, LENIENT ).parse( args );
        TimeZone timeZone = parseTimeZoneConfig( arguments );
        Predicate<LogEntry[]> filter = parseFilter( arguments, timeZone );
        Function<LogEntry,String> serializer = parseSerializer( filter, timeZone );
        Function<PrintStream,InvalidLogEntryHandler> invalidLogEntryHandler = parseInvalidLogEntryHandler( arguments );
        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
              Printer printer = getPrinter( arguments ) )
        {
            for ( String fileAsString : arguments.orphans() )
            {
                PrintStream out = printer.getFor( fileAsString );
                new DumpLogicalLog( fileSystem ).dump( fileAsString, out, filter, serializer,
                        invalidLogEntryHandler.apply( out ) );
            }
        }
    }

    private static Function<PrintStream,InvalidLogEntryHandler> parseInvalidLogEntryHandler( Args arguments )
    {
        if ( arguments.getBoolean( LENIENT ) )
        {
            return LenientInvalidLogEntryHandler::new;
        }
        return out -> InvalidLogEntryHandler.STRICT;
    }

    @SuppressWarnings( "unchecked" )
    private static Function<LogEntry,String> parseSerializer( Predicate<LogEntry[]> filter, TimeZone timeZone )
    {
        if ( filter instanceof Function )
        {
            return (Function<LogEntry,String>) filter;
        }
        return logEntry -> logEntry.toString( timeZone );
    }

    private static Predicate<LogEntry[]> parseFilter( Args arguments, TimeZone timeZone ) throws IOException
    {
        String regex = arguments.get( TX_FILTER );
        if ( regex != null )
        {
            return new TransactionRegexCriteria( regex, timeZone );
        }
        String cc = arguments.get( CC_FILTER );
        if ( cc != null )
        {
            return new ConsistencyCheckOutputCriteria( cc, timeZone );
        }
        return null;
    }

    public static Printer getPrinter( Args args )
    {
        boolean toFile = args.getBoolean( TO_FILE, false, true );
        return toFile ? new FilePrinter() : SYSTEM_OUT_PRINTER;
    }

    public interface Printer extends AutoCloseable
    {
        PrintStream getFor( String file ) throws FileNotFoundException;

        @Override
        void close();
    }

    private static final Printer SYSTEM_OUT_PRINTER = new Printer()
    {
        @Override
        public PrintStream getFor( String file )
        {
            return System.out;
        }

        @Override
        public void close()
        {   // Don't close System.out
        }
    };

    private static class FilePrinter implements Printer
    {
        private File directory;
        private PrintStream out;

        @Override
        public PrintStream getFor( String file ) throws FileNotFoundException
        {
            File absoluteFile = new File( file ).getAbsoluteFile();
            File dir = absoluteFile.isDirectory() ? absoluteFile : absoluteFile.getParentFile();
            if ( !dir.equals( directory ) )
            {
                close();
                File dumpFile = new File( dir, "dump-logical-log.txt" );
                System.out.println( "Redirecting the output to " + dumpFile.getPath() );
                out = new PrintStream( dumpFile );
                directory = dir;
            }
            return out;
        }

        @Override
        public void close()
        {
            if ( out != null )
            {
                out.close();
            }
        }
    }

    public static TimeZone parseTimeZoneConfig( Args arguments )
    {
        return getTimeZone( arguments.get( "timezone", DEFAULT_TIME_ZONE.getID() ) );
    }
}
