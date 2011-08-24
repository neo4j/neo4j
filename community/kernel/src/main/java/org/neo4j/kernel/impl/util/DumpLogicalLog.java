/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.Collection;
import java.util.Comparator;
import java.util.TreeSet;

import javax.transaction.xa.Xid;

import org.neo4j.kernel.impl.nioneo.xa.Command;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.LogIoUtils;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommandFactory;

public class DumpLogicalLog
{
    private static final String PREFIX = "_logical.log.v";

    public static void main( String args[] ) throws IOException
    {
        for ( String arg : args )
        {
            for ( String fileName : filenamesOf( arg ) )
            {
                System.out.println( "=== " + fileName + " ===" );
                FileChannel fileChannel = new RandomAccessFile( fileName, "r" ).getChannel();
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
                    return;
                }
                System.out.println( "Logical log version: " + logVersion + " with prev committed tx[" +
                    prevLastCommittedTx + "]" );
                long logEntriesFound = 0;
                XaCommandFactory cf = new CommandFactory();
                while ( readEntry( fileChannel, buffer, cf ) )
                {
                    logEntriesFound++;
                }
                fileChannel.close();
            }
        }
    }

    private static String[] filenamesOf( String string )
    {
        File file = new File( string );
        if ( file.isDirectory() )
        {
            File[] files = file.listFiles( new FilenameFilter()
            {
                public boolean accept( File dir, String name )
                {
                    return name.contains( PREFIX );
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
            return new String[] { string };
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
                String toFind = PREFIX;
                int index = string.indexOf( toFind );
                if ( index == -1 ) throw new RuntimeException( string );
                return Integer.valueOf( string.substring( index + toFind.length() ) );
            }
        };
    }

    private static boolean readEntry( FileChannel channel, ByteBuffer buf,
            XaCommandFactory cf ) throws IOException
    {
        LogEntry entry = LogIoUtils.readEntry( buf, channel, cf );
        if ( entry != null )
        {
            System.out.println( entry.toString() );
            return true;
        }
        return false;
    }

    private static class CommandFactory extends XaCommandFactory
    {
        @Override
        public XaCommand readCommand( ReadableByteChannel byteChannel,
                ByteBuffer buffer ) throws IOException
        {
            return Command.readCommand( null, byteChannel, buffer );
        }
    }
}
