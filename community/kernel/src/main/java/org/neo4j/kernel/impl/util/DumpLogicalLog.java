/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.Collection;
import java.util.Comparator;
import java.util.TimeZone;
import java.util.TreeSet;

import javax.transaction.xa.Xid;

import org.neo4j.helpers.Args;
import org.neo4j.helpers.Format;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.xa.Command;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.LogIoUtils;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommandFactory;

public class DumpLogicalLog
{
    private final FileSystemAbstraction fileSystem;

    public DumpLogicalLog( FileSystemAbstraction fileSystem )
    {
        this.fileSystem = fileSystem;
    }
    
    public int dump( String filenameOrDirectory,
            TimeZone timeZone ) throws IOException
    {
        int logsFound = 0;
        for ( String fileName : filenamesOf( filenameOrDirectory, getLogPrefix() ) )
        {
            logsFound++;
            System.out.println( "=== " + fileName + " ===" );
            FileChannel fileChannel = fileSystem.open( new File( fileName ), "r" );
            ByteBuffer buffer = ByteBuffer.allocateDirect( 9 + Xid.MAXGTRIDSIZE
                    + Xid.MAXBQUALSIZE * 10 );
            long logVersion, prevLastCommittedTx;
            try
            {
                long[] header = LogIoUtils.readLogHeader( buffer, fileChannel, true );
                logVersion = header[0];
                prevLastCommittedTx = header[1];
            }
            catch ( IOException ex )
            {
                System.out.println( "Unable to read timestamp information, "
                    + "no records in logical log." );
                System.out.println( ex.getMessage() );
                fileChannel.close();
                throw ex;
            }
            System.out.println( "Logical log version: " + logVersion + " with prev committed tx[" +
                prevLastCommittedTx + "]" );
            long logEntriesFound = 0;
            XaCommandFactory cf = instantiateCommandFactory();
            while ( readAndPrintEntry( fileChannel, buffer, cf, timeZone ) )
            {
                logEntriesFound++;
            }
            fileChannel.close();
        }
        return logsFound;
    }

    protected static boolean isAGraphDatabaseDirectory( String fileName )
    {
        File file = new File( fileName );
        return file.isDirectory() && new File( file, NeoStore.DEFAULT_NAME ).exists();
    }

    protected boolean readAndPrintEntry( FileChannel fileChannel, ByteBuffer buffer, XaCommandFactory cf, TimeZone timeZone )
            throws IOException
    {
        LogEntry entry = LogIoUtils.readEntry( buffer, fileChannel, cf );
        if ( entry != null )
        {
            System.out.println( entry.toString( timeZone ) );
            return true;
        }
        return false;
    }

    protected XaCommandFactory instantiateCommandFactory()
    {
        return new CommandFactory();
    }

    protected String getLogPrefix()
    {
        return "nioneo_logical.log";
    }

    public static void main( String args[] ) throws IOException
    {
        Pair<Iterable<String>, TimeZone> config = parseConfig( args );
        for ( String file : config.first() )
            new DumpLogicalLog( new DefaultFileSystemAbstraction() ).dump( file, config.other() );
    }

    public static Pair<Iterable<String>, TimeZone> parseConfig( String[] args )
    {
        Args arguments = new Args( args );
        TimeZone timeZone = Format.DEFAULT_TIME_ZONE;
        String timeZoneString = arguments.get( "timezone", null );
        if ( timeZoneString != null ) timeZone = TimeZone.getTimeZone( timeZoneString );
        return Pair.<Iterable<String>,TimeZone>of( arguments.orphans(), timeZone );
    }

    protected static String[] filenamesOf( String filenameOrDirectory, final String prefix )
    {
        File file = new File( filenameOrDirectory );
        if ( file.isDirectory() )
        {
            File[] files = file.listFiles( new FilenameFilter()
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
                if ( index == -1 ) return Integer.MAX_VALUE;
                return Integer.valueOf( string.substring( index + toFind.length() ) );
            }
        };
    }

    public static class CommandFactory extends XaCommandFactory
    {
        @Override
        public XaCommand readCommand( ReadableByteChannel byteChannel,
                ByteBuffer buffer ) throws IOException
        {
            return Command.readCommand( null, null, byteChannel, buffer );
        }
    }
}
