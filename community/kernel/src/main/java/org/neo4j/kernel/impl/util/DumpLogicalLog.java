/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.util;

import static java.util.TimeZone.getTimeZone;
import static org.neo4j.helpers.Format.DEFAULT_TIME_ZONE;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Comparator;
import java.util.TimeZone;
import java.util.TreeSet;
import javax.transaction.xa.Xid;

import org.neo4j.helpers.Args;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.xa.LogDeserializer;
import org.neo4j.kernel.impl.nioneo.xa.XaCommandReaderFactory;
import org.neo4j.kernel.impl.nioneo.store.StoreChannel;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.VersionAwareLogEntryReader;

public class DumpLogicalLog
{
    private final FileSystemAbstraction fileSystem;

    public DumpLogicalLog( FileSystemAbstraction fileSystem )
    {
        this.fileSystem = fileSystem;
    }

    public int dump( String filenameOrDirectory, PrintStream out, TimeZone timeZone ) throws IOException
    {
        int logsFound = 0;
        for ( String fileName : filenamesOf( filenameOrDirectory, getLogPrefix() ) )
        {
            logsFound++;
            out.println( "=== " + fileName + " ===" );
            StoreChannel fileChannel = fileSystem.open( new File( fileName ), "r" );
            ByteBuffer buffer = ByteBuffer.allocateDirect( 9 + Xid.MAXGTRIDSIZE
                    + Xid.MAXBQUALSIZE * 10 );
            long logVersion, prevLastCommittedTx, logFormat;
            try
            {
                long[] header = VersionAwareLogEntryReader.readLogHeader( buffer, fileChannel, false );
                logVersion = header[0];
                prevLastCommittedTx = header[1];
                logFormat = header[2];
            }
            catch ( IOException ex )
            {
                out.println( "Unable to read timestamp information, no records in logical log." );
                out.println( ex.getMessage() );
                fileChannel.close();
                throw ex;
            }
            out.println( "Logical log format:" + logFormat + " version:" + logVersion + " with prev committed tx[" +
                prevLastCommittedTx + "]" );

            LogDeserializer deserializer =
                    new LogDeserializer( buffer, instantiateCommandReaderFactory() );
            PrintingConsumer consumer = new PrintingConsumer( out, timeZone );

            try( Cursor<LogEntry, IOException> cursor = deserializer.cursor( fileChannel ) )
            {
                while( cursor.next( consumer ) );
            }
        }
        return logsFound;
    }

    protected static boolean isAGraphDatabaseDirectory( String fileName )
    {
        File file = new File( fileName );
        return file.isDirectory() && new File( file, NeoStore.DEFAULT_NAME ).exists();
    }

    protected XaCommandReaderFactory instantiateCommandReaderFactory()
    {
        return XaCommandReaderFactory.DEFAULT;
    }

    protected String getLogPrefix()
    {
        return "nioneo_logical.log";
    }

    public static void main( String args[] ) throws IOException
    {
        Args arguments = new Args( args );
        TimeZone timeZone = parseTimeZoneConfig( arguments );
        try ( Printer printer = getPrinter( arguments ) )
        {
            for ( String fileAsString : arguments.orphans() )
            {
                new DumpLogicalLog( new DefaultFileSystemAbstraction() ).dump(
                        fileAsString, printer.getFor( fileAsString ), timeZone );
            }
        }
    }

    public static Printer getPrinter( Args args )
    {
        boolean toFile = args.getBoolean( "tofile", false, true ).booleanValue();
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
                safeClose();
                File dumpFile = new File( dir, "dump-logical-log.txt" );
                System.out.println( "Redirecting the output to " + dumpFile.getPath() );
                out = new PrintStream( dumpFile );
                directory = dir;
            }
            return out;
        }

        private void safeClose()
        {
            if ( out != null )
            {
                out.close();
            }
        }

        @Override
        public void close()
        {
            safeClose();
        }
    }

    public static TimeZone parseTimeZoneConfig( Args arguments )
    {
        return getTimeZone( arguments.get( "timezone", DEFAULT_TIME_ZONE.getID() ) );
    }

    protected String[] filenamesOf( String filenameOrDirectory, final String prefix )
    {

        File file = new File( filenameOrDirectory );
        if ( fileSystem.isDirectory(file) )
        {
            File[] files = fileSystem.listFiles( file , new FilenameFilter()
            {
                @Override
                public boolean accept( File dir, String name )
                {
                    return name.contains( prefix ) && !name.contains( "active" );
                }
            } );
            Collection<String> result = new TreeSet<String>( sequentialComparator() );
            for ( int i = 0; i < files.length; i++ )
            {
                result.add( files[i].getPath() );
            }
            return result.toArray( new String[result.size()] );
        }
        else
        {
            return new String[] { filenameOrDirectory };
        }
    }

    private static Comparator<? super String> sequentialComparator()

    {
        return new Comparator<String>()
        {
            @Override
            public int compare( String o1, String o2 )
            {
                return versionOf( o1 ).compareTo( versionOf( o2 ) );
            }

            private Integer versionOf( String string )
            {
                String toFind = ".v";
                int index = string.indexOf( toFind );
                if ( index == -1 )
                {
                    return Integer.MAX_VALUE;
                }
                return Integer.valueOf( string.substring( index + toFind.length() ) );
            }
        };
    }

    private class PrintingConsumer implements Consumer<LogEntry, IOException>
    {

        private final PrintStream out;
        private final TimeZone timeZone;

        private PrintingConsumer( PrintStream out, TimeZone timeZone )
        {
            this.out = out;
            this.timeZone = timeZone;
        }

        @Override
        public boolean accept( LogEntry entry ) throws IOException
        {
            out.println( entry.toString( timeZone ) );
            return true;
        }
    }
}
